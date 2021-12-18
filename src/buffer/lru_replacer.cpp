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
#include "common/logger.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : capacity_(num_pages), head_(new ListNode(-1)), tail_(new ListNode(-1)) {
  head_->next_ = tail_;
  tail_->prev_ = head_;
}

LRUReplacer::~LRUReplacer() {
  delete head_;
  delete tail_;
}

// page 满的时候，应该将LRU最久未访问的对象删除，存储在输出参数中并返回True。 如果 Replacer 为空返回 False
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);

  if (cache_.empty()) {
    // LOG_DEBUG("Victim cache_ is empty!!");
    return false;
  }

  auto last = RemoveLast();
  *frame_id = last->val_;
  cache_.erase(last->val_);
  // LOG_DEBUG("Victim frame_id:%d!!", last->val_);
  delete last;
  return true;
}

// 当页从磁盘加载到buffer-pool里面的时候，应该调用这个，从LRU里面删除这个frame
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (cache_.count(frame_id) == 0U) {
    // LOG_DEBUG("Pin frame_id:%d already exist", frame_id);
    return;
  }

  auto delete_node = RemoveOne(cache_[frame_id]);
  cache_.erase(delete_node->val_);
  // LOG_DEBUG("Pin frame_id:%d delete!", frame_id);
  delete delete_node;
}

// 当page的pin_count 变为 0 时应调用此方法。此方法应将包含未固定页面的框架添加到LRU
void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  ListNode *new_node = new ListNode(frame_id);

  // already exist:
  // 1. remove it;
  // 2. add to first
  if (cache_.count(frame_id) != 0U) {
    //    auto old_node = RemoveOne(cache_[frame_id]);
    //    delete old_node;
    //    AddToFirst(new_node);
    //    cache_[frame_id] = new_node;
    // LOG_DEBUG("UnPin frame_id:%d already exists!", frame_id);
    return;
  }  // size > capacity ? remove last & add to first : add to first

  if (cache_.size() >= capacity_) {
    auto last = RemoveLast();
    cache_.erase(last->val_);
    //LOG_DEBUG("UnPin capacity:%lu, size:%lu, last-frameid:%d, new frame-id:%d", capacity_, cache_.size(), last->val_,frame_id);
    delete last;

    AddToFirst(new_node);
    cache_[frame_id] = new_node;
  } else {
    // LOG_DEBUG("UnPin frame_id:%d ..", frame_id);
    AddToFirst(new_node);
    cache_[frame_id] = new_node;
  }
}

size_t LRUReplacer::Size() { return cache_.size(); }

void LRUReplacer::AddToFirst(ListNode *node) {
  // head_ -> A, next_ = A
  auto next = head_->next_;
  // head_ -> node
  head_->next_ = node;
  // node -> A
  node->next_ = next;
  // node <- A
  next->prev_ = node;
  // head_ <- node
  node->prev_ = head_;
}
LRUReplacer::ListNode *LRUReplacer::RemoveOne(LRUReplacer::ListNode *node) {
  // A <-> B <-> C : node: B

  // A -> C
  node->prev_->next_ = node->next_;
  // C -> A
  node->next_->prev_ = node->prev_;

  return node;
}

LRUReplacer::ListNode *LRUReplacer::RemoveLast() {
  auto prev = tail_->prev_;
  auto ret = RemoveOne(prev);

  return ret;
}

LRUReplacer::ListNode::ListNode(frame_id_t val) : val_(val), prev_(nullptr), next_(nullptr) {}

}  // namespace bustub
