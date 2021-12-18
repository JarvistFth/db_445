//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"
// using namespace std;

namespace bustub {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  // REMOVE LAST
  bool Victim(frame_id_t *frame_id) override;

  // 当页从磁盘加载到buffer-pool里面的时候，应该调用这个，从LRU里面删除这个page
  void Pin(frame_id_t frame_id) override;
  // 当page的pin_count 变为 0 时应调用此方法。此方法应将包含未固定页面的框架添加到LRU
  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  // TODO(student): implement me!
  struct ListNode {
    ListNode() = default;
    explicit ListNode(frame_id_t val);
    ~ListNode() = default;
    frame_id_t val_;

    ListNode *prev_;
    ListNode *next_;
  };

  std::unordered_map<frame_id_t, ListNode *> cache_;

  size_t capacity_;

  ListNode *head_;
  ListNode *tail_;

  void AddToFirst(ListNode *node);

  ListNode *RemoveOne(ListNode *node);

  ListNode *RemoveLast();

  std::mutex latch_;
};

}  // namespace bustub
