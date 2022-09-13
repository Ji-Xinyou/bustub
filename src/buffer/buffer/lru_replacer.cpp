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

LRUReplacer::LRUReplacer(size_t num_pages) : cap_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock<std::mutex> lk{lk_};

  if (len_ == 0U) {
    frame_id = nullptr;  // nullptr if not found
    return false;
  }
  *frame_id = rlist_.back();
  rlist_.pop_back();
  idmap_.erase(*frame_id);
  len_--;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lk{lk_};

  auto it = idmap_.find(frame_id);
  if (it != idmap_.end()) {
    rlist_.erase(it->second);
    idmap_.erase(it);
    len_--;
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lk{lk_};

  if (idmap_.find(frame_id) != idmap_.end()) {
    return;
  }

  if (len_ == cap_) {  // full lru list, evict one
    frame_id_t id = rlist_.back();
    rlist_.pop_back();
    idmap_.erase(id);
    len_--;
  }

  rlist_.push_front(frame_id);
  idmap_.emplace(frame_id, rlist_.begin());
  len_++;
}

size_t LRUReplacer::Size() {
  std::scoped_lock<std::mutex> lk{lk_};

  size_t sz = len_;
  return sz;
}

}  // namespace bustub
