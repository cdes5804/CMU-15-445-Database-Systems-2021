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
  timer_ = 0;
  timestamp_.resize(num_pages);
  std::fill(timestamp_.begin(), timestamp_.end(), NOT_IN_REPLACER);
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock lock(mu_);
  if (unpinned_pages_.empty()) {
    return false;
  }
  auto iter = unpinned_pages_.begin();
  *frame_id = iter->second;
  timestamp_[*frame_id] = NOT_IN_REPLACER;
  unpinned_pages_.erase(iter);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock lock(mu_);
  uint64_t time = timestamp_[frame_id];
  if (time != NOT_IN_REPLACER) {
    auto iter = unpinned_pages_.find({time, frame_id});
    unpinned_pages_.erase(iter);
    timestamp_[frame_id] = NOT_IN_REPLACER;
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock lock(mu_);
  uint64_t time = timestamp_[frame_id];
  if (time == NOT_IN_REPLACER) {
    timer_++;
    timestamp_[frame_id] = timer_;
    unpinned_pages_.insert({timer_, frame_id});
  }
}

size_t LRUReplacer::Size() {
  std::scoped_lock lock(mu_);
  return unpinned_pages_.size();
}

}  // namespace bustub
