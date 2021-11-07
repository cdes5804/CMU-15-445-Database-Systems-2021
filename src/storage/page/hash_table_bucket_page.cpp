//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  uint32_t index = BinarySearch(key, cmp);
  for (uint32_t i = index; i < BUCKET_ARRAY_SIZE && IsOccupied(i); i++) {
    if (IsReadable(i)) {
      if (cmp(KeyAt(i), key) == 0) {
        result->emplace_back(ValueAt(i));
      } else {
        break;
      }
    }
  }
  return !result->empty();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  if (IsFull()) {
    return false;
  }
  const uint32_t index = BinarySearch(key, cmp);
  bool found_duplicate = false;
  bool through_different_key = false;
  uint32_t seek_right_index = index;
  while (seek_right_index < BUCKET_ARRAY_SIZE && IsReadable(seek_right_index)) {
    if (cmp(key, array_[seek_right_index].first) != 0) {
      through_different_key = true;
    } else if (array_[seek_right_index].second == value) {
      found_duplicate = true;
      break;
    }
    seek_right_index++;
  }
  if (found_duplicate) {
    return false;
  }
  if (seek_right_index < BUCKET_ARRAY_SIZE && !through_different_key) {
    InsertAt(seek_right_index, key, value);
    return true;
  }

  int32_t seek_left_index = index - 1;
  through_different_key = false;
  while (seek_left_index >= 0 && IsReadable(seek_left_index)) {
    if (cmp(key, array_[seek_left_index].first) != 0) {
      through_different_key = true;
    }
    seek_left_index--;
  }
  if (seek_left_index >= 0 && !through_different_key) {
    InsertAt(seek_left_index, key, value);
    return true;
  }
  uint32_t available_index = 0;
  if (seek_left_index >= 0) {
    available_index = ShiftBucketUntilKeyCanInsert(seek_left_index, 1, key, cmp);
  } else {
    available_index = ShiftBucketUntilKeyCanInsert(seek_right_index, -1, key, cmp);
  }
  InsertAt(available_index, key, value);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  uint32_t index = BinarySearch(key, cmp);
  for (uint32_t i = index; i < BUCKET_ARRAY_SIZE && IsOccupied(i); i++) {
    if (IsReadable(i)) {
      if (cmp(array_[i].first, key) == 0) {
        if (array_[i].second == value) {
          RemoveAt(i);
          return true;
        }
      } else {
        break;
      }
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  const auto [char_pos, bit_pos] = std::div(bucket_idx, CELL_SIZE);
  readable_[char_pos] &= ~(1U << bit_pos);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  const auto [char_pos, bit_pos] = std::div(bucket_idx, CELL_SIZE);
  return (occupied_[char_pos] & (1U << bit_pos)) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  const auto [char_pos, bit_pos] = std::div(bucket_idx, CELL_SIZE);
  occupied_[char_pos] |= (1U << bit_pos);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  const auto [char_pos, bit_pos] = std::div(bucket_idx, CELL_SIZE);
  return (readable_[char_pos] & (1U << bit_pos)) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  const auto [char_pos, bit_pos] = std::div(bucket_idx, CELL_SIZE);
  readable_[char_pos] |= (1U << bit_pos);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() const {
  return NumReadable() == BUCKET_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() const {
  uint32_t count = 0;
  for (auto r : readable_) {
    while (r) {
      r &= (r - 1);
      count++;
    }
  }
  return count;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() const {
  return NumReadable() == 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() const {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NonOccupiedIndex() const {
  int32_t result = BUCKET_ARRAY_SIZE;
  int32_t left = 0;
  int32_t right = BUCKET_ARRAY_SIZE - 1;
  while (left <= right) {
    int32_t mid = (left + right) / 2;
    if (IsOccupied(mid)) {
      left = mid + 1;
    } else {
      result = mid;
      right = mid - 1;
    }
  }
  return result;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
int32_t HASH_TABLE_BUCKET_TYPE::BinarySearch(KeyType key, KeyComparator cmp) const {
  int32_t left = 0;
  int32_t right = NonOccupiedIndex() - 1;
  int32_t result = right + 1;
  while (left <= right) {
    int32_t mid = (left + right) / 2;
    auto cmp_result = cmp(array_[mid].first, key);
    if (cmp_result < 0) {
      left = mid + 1;
    } else {
      result = mid;
      right = mid - 1;
    }
  }
  return result;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::ShiftBucketUntilKeyCanInsert(uint32_t available_index, int8_t shift_direction,
                                                              KeyType key, KeyComparator cmp) {
  SetOccupied(available_index);
  SetReadable(available_index);

  while (true) {
    uint32_t next_index = available_index + shift_direction;
    if (next_index >= BUCKET_ARRAY_SIZE || cmp(array_[next_index].first, key) * shift_direction >= 0) {
      break;
    }
    array_[available_index].first = array_[next_index].first;
    array_[available_index].second = array_[next_index].second;
    available_index += shift_direction;
  }
  return available_index;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::InsertAt(uint32_t index, KeyType key, ValueType value) {
  SetOccupied(index);
  SetReadable(index);
  array_[index].first = key;
  array_[index].second = value;
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
