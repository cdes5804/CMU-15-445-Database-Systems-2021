//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  iters_.resize(num_pages);
  iter_valid_.resize(num_pages, false);
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::shared_lock rlock(r_mu_);
  if (unpinned_pages_.empty()) {
    return false;
  }
  std::scoped_lock wlock(w_mu_);
  auto iter = unpinned_pages_.begin();
  *frame_id = *iter;
  iter_valid_[*iter] = false;
  unpinned_pages_.erase(iter);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= iter_valid_.size()) {
    return;
  }
  std::shared_lock rlock(r_mu_);
  if (iter_valid_[frame_id]) {
    std::scoped_lock wlock(w_mu_);
    unpinned_pages_.erase(iters_[frame_id]);
    iter_valid_[frame_id] = false;
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= iter_valid_.size()) {
    return;
  }
  std::shared_lock rlock(r_mu_);
  if (!iter_valid_[frame_id]) {
    std::scoped_lock wlock(w_mu_);
    unpinned_pages_.push_back(frame_id);
    iters_[frame_id] = std::prev(unpinned_pages_.end());
    iter_valid_[frame_id] = true;
  }
}

size_t LRUReplacer::Size() {
  std::shared_lock lock(r_mu_);
  return unpinned_pages_.size();
}

}  // namespace bustub
