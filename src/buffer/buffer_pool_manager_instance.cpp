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

#include "common/logger.h"
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

//  LOG_DEBUG("new buffer pool with size:%zu.. ", pool_size);
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> lock_guard(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }

  Page* page = &pages_[iter->second];
  if (page->IsDirty()) {
    page->WLatch();
//    LOG_DEBUG("write page:{%d}", page_id);
    disk_manager_->WritePage(page_id, page->GetData());
    page->is_dirty_ = false;
    page->WUnlatch();
  }
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard guard(latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].IsDirty()) {
      pages_[i].WLatch();
//      LOG_DEBUG("write page:{%d}", pages_[i].page_id_);
      disk_manager_->WritePage(pages_[i].page_id_, pages_[i].data_);
      pages_[i].is_dirty_ = false;
      pages_[i].WUnlatch();
    }
  }
  // atomic operation, dont write like this!!
  //  for (size_t i = 0; i < pool_size_; ++i) {
  //    FlushPgImp(pages_[i].page_id_);
  //  }

  // You can do it!
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  std::lock_guard<std::mutex> lock_guard(latch_);
  frame_id_t frame_id;
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  if(!free_list_.empty()){
    frame_id = free_list_.front();
    free_list_.pop_front();
  }else if(!replacer_->Victim(&frame_id)){
    // 1. If all the pages in the buffer pool are pinned, return nullptr.
    return nullptr;
  }

  auto ret = &pages_[frame_id];
  auto new_page_id = AllocatePage();

  // before swap, need to writePage if it is dirty
  if (ret->is_dirty_) {
    ret->WLatch();
    disk_manager_->WritePage(ret->page_id_, ret->GetData());
    ret->is_dirty_ = false;
    ret->WUnlatch();
  }

  // update page table, update metadata, zero out memory,
  page_table_.erase(ret->page_id_);
  page_table_[new_page_id] = frame_id;

  ret->ResetMemory();
  ret->page_id_ = new_page_id;
  ret->pin_count_ = 1;
  ret->is_dirty_ = false;
//  LOG_DEBUG("new page with id: %d, frameid:%d", new_page_id, frame_id);

  // set page id as output parameter
  *page_id = new_page_id;
  return ret;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  std::lock_guard<std::mutex> lock_guard(latch_);
  Page *ret;

//  LOG_DEBUG("fetch page with pageid: %d", page_id);
  auto iter = page_table_.find(page_id);
  // 1.1    If P exists, pin it and return it immediately.
  if (iter != page_table_.end()) {
    ret = &pages_[iter->second];
    replacer_->Pin(iter->second);
    ret->pin_count_++;
    return ret;
  }

  frame_id_t frame_id;
  // find a replacement page (R) from either the free list or the replacer.
  // Note that pages are always found from the free list first.
  if(!free_list_.empty()){
    frame_id = free_list_.front();
    free_list_.pop_front();
//    replacer_->Unpin(frame_id);
  }else if(!replacer_->Victim(&frame_id)){
    // 1. If all the pages in the buffer pool are pinned, return nullptr.
    return nullptr;
  }
  ret = &pages_[frame_id];

  // 2.     If R is dirty, write it back to the disk.
  if (ret->is_dirty_) {
    ret->WLatch();
    disk_manager_->WritePage(ret->page_id_, ret->data_);
    ret->is_dirty_ = false;
    ret->WUnlatch();
  }

  // 3.     Delete R from the page table and insert P.
  page_table_.erase(ret->page_id_);
  page_table_[page_id] = frame_id;

  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  ret->ResetMemory();
  ret->RLatch();
  disk_manager_->ReadPage(page_id, ret->data_);
  ret->RUnlatch();
  ret->pin_count_ = 1;
  ret->is_dirty_ = false;
  ret->page_id_ = page_id;

  return ret;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> lock_guard(latch_);
  DeallocatePage(page_id);
  auto iter = page_table_.find(page_id);
//  LOG_DEBUG("delete page with page id: %d", page_id);
  if (iter == page_table_.end()) {
    return true;
  }


  auto frame_id = iter->second;
  auto p = &pages_[frame_id];

  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if (p->GetPinCount() > 0) {
    return false;
  }
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  page_table_.erase(page_id);
  replacer_->Pin(frame_id);
  p->ResetMemory();
  p->is_dirty_ = false;
  free_list_.push_back(frame_id);
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lock_guard(latch_);

  Page *p;
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  p = &pages_[iter->second];
  // this page: true, UnpinPage: false
  // should not let p->is_dirty = false
  p->is_dirty_ |= is_dirty;

  if (p->pin_count_ <= 0) {
    LOG_DEBUG("page-id: %d pin cnt %d <= 0!", page_id ,p->pin_count_);
    return false;
  }

  p->pin_count_ -= 1;
  if (p->pin_count_ == 0) {
    replacer_->Unpin(iter->second);
  }

  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
//  LOG_DEBUG("next page id:{%d}, instance_index:{%d}", next_page_id, instance_index_);
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
