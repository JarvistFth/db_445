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
#include "common/logger.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) : next_index_(0), each_pool_size_(pool_size), num_instance_(num_instances)
{
  // Allocate and create individual BufferPoolManagerInstances

  for (size_t i = 0; i < num_instances; i++) {
    instances_.push_back(
        std::make_shared<BufferPoolManagerInstance>(pool_size, num_instances, i, disk_manager, log_manager));
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() = default;

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return instances_.size() * each_pool_size_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.

  auto idx = page_id % instances_.size();
  return instances_[idx].get();

  //  return nullptr;
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance

  auto bfi = GetBufferPoolManager(page_id);
  return bfi->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  auto bfi = GetBufferPoolManager(page_id);
  return bfi->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  auto bfi = GetBufferPoolManager(page_id);
  return bfi->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  //  LOG_DEBUG("new page");
  //  auto loopIndex = start_idx_.load();
  //  LOG_DEBUG("loopIndex: %d, instance.size(): %lu", loopIndex, instances_.size());
  std::lock_guard<std::mutex> lock_guard(latch_);
  for (size_t index = 0; index < num_instance_; index++) {
    auto bfi = instances_[next_index_];
    auto page = bfi->NewPage(page_id);
    next_index_ = (next_index_ + 1) % num_instance_;
    if (page != nullptr) {
//      LOG_DEBUG("new page with pageid: %d, instance index - %d", *page_id, bfi->GetInstanceIndex());
      return page;
    }
  }
  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  auto bfi = GetBufferPoolManager(page_id);
  return bfi->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances

  for (auto &bfi : instances_) {
    bfi->FlushAllPages();
  }
}

}  // namespace bustub
