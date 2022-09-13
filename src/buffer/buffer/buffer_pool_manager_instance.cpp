//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::scoped_lock<std::mutex> lk{latch_};

  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  frame_id_t fid;
  if (!GetFrameid(page_id, &fid)) {
    return false;
  }

  Page *p = &pages_[fid];
  disk_manager_->WritePage(p->GetPageId(), p->GetData());
  p->is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::scoped_lock<std::mutex> lk{latch_};

  for (size_t i = 0; i < pool_size_; i++) {
    Page *p = &pages_[i];
    if (p->IsDirty()) {
      FlushPgImp(p->GetPageId());
    }
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::scoped_lock<std::mutex> lk{latch_};

  frame_id_t fid;
  if (!FindFreePg(&fid)) {
    return nullptr;
  }

  *page_id = AllocatePage();
  PreemptPage(&pages_[fid], *page_id, fid);
  replacer_->Pin(fid);

  return &pages_[fid];
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  std::scoped_lock<std::mutex> lk{latch_};

  {  // found?
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {  // found
      frame_id_t fid = it->second;
      Page *p = &pages_[fid];  // page in bp
      p->pin_count_++;
      replacer_->Pin(fid);
      return p;
    }
  }

  {  // not found
    frame_id_t fid;
    if (!FindFreePg(&fid)) {
      return nullptr;  // no free pg
    }

    Page *vp = &pages_[fid];
    PreemptPage(vp, page_id, fid);

    disk_manager_->ReadPage(page_id, vp->data_);
    replacer_->Pin(fid);

    return vp;
  }
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  std::scoped_lock<std::mutex> lk{latch_};

  frame_id_t fid;
  if (!GetFrameid(page_id, &fid)) {
    return true;  // does not exist
  }

  Page *p = &pages_[fid];
  if (p->GetPinCount() != 0) {
    return false;  // someone is using
  }

  assert(!p->GetPinCount());

  PreemptPage(p, INVALID_PAGE_ID, fid);
  p->pin_count_ = 0;
  replacer_->Unpin(fid);
  free_list_.emplace_back(fid);

  DeallocatePage(page_id);

  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::scoped_lock<std::mutex> lk{latch_};

  frame_id_t fid;
  if (!GetFrameid(page_id, &fid)) {
    return false;
  }

  Page *p = &pages_[fid];
  if (p->pin_count_ <= 0) {
    return false;
  }

  p->pin_count_--;
  if (p->pin_count_ == 0) {
    replacer_->Unpin(fid);
  }

  if (is_dirty) {
    p->is_dirty_ = true;
  }

  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

bool BufferPoolManagerInstance::FindFreePg(frame_id_t *frame_id) {
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }

  return replacer_->Victim(frame_id);
}

// hold the lock when entering this method, remember to modify the replacer
void BufferPoolManagerInstance::PreemptPage(Page *p, page_id_t npid, frame_id_t nfid) {
  if (p->IsDirty()) {
    disk_manager_->WritePage(p->GetPageId(), p->GetData());
  }

  page_table_.erase(p->GetPageId());

  p->is_dirty_ = false;
  p->page_id_ = npid;
  p->ResetMemory();
  p->pin_count_ = 1;

  if (npid != INVALID_PAGE_ID) {
    page_table_.emplace(npid, nfid);
  }
}

bool BufferPoolManagerInstance::GetFrameid(page_id_t page_id, frame_id_t *frame_id) {
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {  // not found
    frame_id = nullptr;
    return false;
  }

  *frame_id = it->second;
  return true;
}

}  // namespace bustub
