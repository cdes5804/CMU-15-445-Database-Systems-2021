//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager->NewPage(&this->directory_page_id_));
  dir_page->SetPageId(this->directory_page_id_);
  dir_page->IncrGlobalDepth();
  page_id_t page_id;
  buffer_pool_manager->NewPage(&page_id);
  dir_page->SetBucketPageId(0, page_id);
  dir_page->SetLocalDepth(0, 1);
  buffer_pool_manager->UnpinPage(page_id, false, nullptr);
  buffer_pool_manager->NewPage(&page_id);
  dir_page->SetBucketPageId(1, page_id);
  dir_page->SetLocalDepth(1, 1);
  buffer_pool_manager->UnpinPage(page_id, false, nullptr);
  buffer_pool_manager->UnpinPage(directory_page_id_, true, nullptr);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  auto mask = dir_page->GetGlobalDepthMask();
  return mask & Hash(key);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  auto mask = dir_page->GetGlobalDepthMask();
  auto directory_index = mask & Hash(key);
  return dir_page->GetBucketPageId(directory_index);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id));
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  auto directory_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, directory_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);

  auto found = bucket_page->GetValue(key, comparator_, result);

  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);

  return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto dir_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);

  std::vector<ValueType> values;
  bool found_key = bucket_page->GetValue(key, comparator_, &values);
  if (found_key) {
    for (auto &v : values) {
      if (v == value) {
        return false;
      }
    }
  }

  bool has_split = false;
  while (bucket_page->IsFull()) {
    has_split = true;
    auto bucket_index = KeyToDirectoryIndex(key, dir_page);
    Split(dir_page, bucket_index);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
    bucket_page_id = KeyToPageId(key, dir_page);
    bucket_page = FetchBucketPage(bucket_page_id);
  }
  bool result = bucket_page->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, has_split, nullptr);
  return result;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Split(HashTableDirectoryPage *dir_page, const uint32_t &bucket_index) {
  const auto global_depth = dir_page->GetGlobalDepth();
  const auto local_depth = dir_page->GetLocalDepth(bucket_index);

  if (local_depth == global_depth) {
    uint32_t num_entries = dir_page->Size();
    dir_page->IncrGlobalDepth();
    for (uint32_t i = 0; i < num_entries; i++) {
      auto split_index = i + num_entries;
      dir_page->SetBucketPageId(split_index, dir_page->GetBucketPageId(i));
      dir_page->SetLocalDepth(split_index, dir_page->GetLocalDepth(i));
    }
  }

  // dir_page->IncrLocalDepth(bucket_index);
  auto bucket_page_id = dir_page->GetBucketPageId(bucket_index);
  auto bucket_page = FetchBucketPage(bucket_page_id);

  page_id_t split_page_id;
  const auto split_page = reinterpret_cast<Page *>(buffer_pool_manager_->NewPage(&split_page_id));
  split_page->WLatch();

  auto split_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(split_page);
  uint32_t high_bit = (1U << local_depth) & bucket_index;
  auto num_entries = dir_page->Size();
  for (uint32_t i = 0; i < num_entries; i++) {
    if (dir_page->GetBucketPageId(i) == bucket_page_id) {
      dir_page->IncrLocalDepth(i);
      if (((1U << local_depth) & i) != high_bit) {
        dir_page->SetBucketPageId(i, split_page_id);
      }
    }
  }
  const auto mask = dir_page->GetLocalDepthMask(bucket_index);
  const auto bucket_mask = bucket_index & mask;

  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (bucket_page->IsReadable(i)) {
      auto key = bucket_page->KeyAt(i);
      auto hash = Hash(key);
      if ((hash & mask) != bucket_mask) {
        auto value = bucket_page->ValueAt(i);
        bucket_page->RemoveAt(i);
        split_bucket_page->Insert(key, value, comparator_);
      } else {
      }
    }
  }
  split_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
  buffer_pool_manager_->UnpinPage(split_page_id, true, nullptr);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto dir_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);

  if (!bucket_page->Remove(key, value, comparator_)) {
    buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    return false;
  }

  bool has_merged = false;
  auto bucket_page_index = KeyToDirectoryIndex(key, dir_page);

  while (bucket_page->IsEmpty() && Merge(dir_page, bucket_page_index)) {
    has_merged = true;
    buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
    bucket_page_index = KeyToDirectoryIndex(key, dir_page);
    bucket_page_id = KeyToPageId(key, dir_page);
    bucket_page = FetchBucketPage(bucket_page_id);
  }

  buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, has_merged, nullptr);
  return true;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Merge(HashTableDirectoryPage *dir_page, const uint32_t &bucket_index) {
  if (dir_page->GetGlobalDepth() == 0) {
    return false;
  }
  auto local_depth = dir_page->GetLocalDepth(bucket_index);
  auto split_image_index = dir_page->GetSplitImageIndex(bucket_index);
  auto split_image_depth = dir_page->GetLocalDepth(split_image_index);
  if (local_depth <= 1 || local_depth != split_image_depth) {
    return false;
  }

  auto global_depth = dir_page->GetGlobalDepth();
  auto num_entries = 1U << (global_depth - local_depth);
  auto local_depth_mask = dir_page->GetLocalDepthMask(bucket_index);
  auto suffix = local_depth_mask & bucket_index;
  auto split_page_id = dir_page->GetBucketPageId(split_image_index);

  for (uint32_t i = 0; i < num_entries; i++) {
    auto index = (i << local_depth) + suffix;
    dir_page->SetBucketPageId(index, split_page_id);
  }

  local_depth_mask >>= 1;
  local_depth--;
  suffix = local_depth_mask & split_image_index;
  num_entries = 1U << (global_depth - local_depth);
  for (uint32_t i = 0; i < num_entries; i++) {
    auto index = (i << local_depth) + suffix;
    dir_page->DecrLocalDepth(index);
  }

  auto bucket_page_id = dir_page->GetBucketPageId(bucket_index);
  buffer_pool_manager_->FlushPage(bucket_page_id, nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  buffer_pool_manager_->DeletePage(bucket_page_id, nullptr);

  if (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }
  return true;
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
