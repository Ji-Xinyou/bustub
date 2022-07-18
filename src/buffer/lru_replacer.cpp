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

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  for (size_t i = 0; i < num_pages; ++i) {
    frameid_to_iter_.emplace_back(std::list<frame_id_t>::iterator{});
  }
}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lg(mu_);
  if (list_.empty()) {
    *frame_id = INVALID_PAGE_ID;
    return false;
  }
  *frame_id = list_.back();
  list_.pop_back();
  frameid_to_iter_[*frame_id] = std::list<frame_id_t>::iterator{};
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lg(mu_);
  if (!Find(frame_id)) {
    return;
  }
  list_.erase(frameid_to_iter_[frame_id]);
  frameid_to_iter_[frame_id] = std::list<frame_id_t>::iterator{};
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lg(mu_);
  if (Find(frame_id)) {
    return;
  }
  list_.emplace_front(frame_id);  // newer frame is at the front
  frameid_to_iter_[frame_id] = list_.begin();
}

inline bool LRUReplacer::Find(frame_id_t frame_id) {
  return (frameid_to_iter_[frame_id] != std::list<frame_id_t>::iterator{});
}

auto LRUReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lg(mu_);
  return list_.size();
}

}  // namespace bustub
