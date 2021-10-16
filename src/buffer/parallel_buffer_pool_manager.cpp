//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  managers_ = new BufferPoolManagerInstance *[num_instances];
  for (size_t i = 0; i < num_instances; i++) {
    managers_[i] = new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager);
  }
  num_instances_ = num_instances;
  start_index_ = 0;
  pool_size_ = pool_size;
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (size_t i = 0; i < num_instances_; i++) {
    delete managers_[i];
  }
  delete[] managers_;
}

size_t ParallelBufferPoolManager::GetPoolSize() { return num_instances_ * pool_size_; }

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return managers_[page_id % num_instances_];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  auto manager = GetBufferPoolManager(page_id);
  return manager->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  auto manager = GetBufferPoolManager(page_id);
  return manager->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  auto manager = GetBufferPoolManager(page_id);
  return manager->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  std::scoped_lock lock(mu_);
  Page *page = nullptr;
  size_t tried = 0;
  while (page == nullptr && tried != num_instances_) {
    page = managers_[start_index_]->NewPage(page_id);
    start_index_ = (start_index_ + 1) % num_instances_;
    tried++;
  }
  return page;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  auto manager = GetBufferPoolManager(page_id);
  return manager->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  for (size_t i = 0; i < num_instances_; i++) {
    managers_[i]->FlushAllPages();
  }
  // flush all pages from all BufferPoolManagerInstances
}

}  // namespace bustub
