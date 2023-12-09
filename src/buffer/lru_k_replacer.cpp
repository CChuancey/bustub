//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/config.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // 找到适合删除的frame：时间最早
  std::lock_guard<std::mutex> lock_guard(latch_);
  *frame_id = -1;

  // 比较器，找到访问时间较早的frame
  auto cmp = [&](frame_id_t a, frame_id_t b) {
    // 优先删除访问次数不满k次的frame
    if (frame_infos_[a].time_sequence_.size() < k_ && frame_infos_[b].time_sequence_.size() == k_) {
      return true;
    }

    if (frame_infos_[b].time_sequence_.size() < k_ && frame_infos_[a].time_sequence_.size() == k_) {
      return false;
    }

    // 如果访问次数都小于k或者访问次数都达到了，则比较谁的访问时间早；
    return frame_infos_[a].time_sequence_.front() < frame_infos_[b].time_sequence_.front();
  };

  for (const auto &kv : frame_infos_) {
    if (kv.second.evictable_) {
      if (*frame_id == -1 || cmp(kv.first, *frame_id)) {
        *frame_id = kv.first;
      }
    }
  }

  if (*frame_id != -1) {
    frame_infos_.erase(*frame_id);
    // 记得将缓冲区的统计个数更新！
    curr_size_--;
    return true;
  }

  // 找不到可以evcit的frame
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  // To do: 没找到对应的frame、或者frame已经满了 ???????????
  if (frame_infos_.count(frame_id) == 0 && replacer_size_ == frame_infos_.size()) {
    return;
  }

  if (frame_infos_[frame_id].time_sequence_.size() == k_) {
    frame_infos_[frame_id].time_sequence_.pop();
  }
  frame_infos_[frame_id].time_sequence_.push(current_timestamp_++);
}

// BufferPoolManager知晓调用该函数的时机
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (frame_infos_.count(frame_id) == 0) {
    return;
  }

  // 记录原始的evicatable状态
  bool flag = frame_infos_[frame_id].evictable_;
  frame_infos_[frame_id].evictable_ = set_evictable;

  if (!flag && set_evictable) {
    curr_size_++;
  } else if (flag && !set_evictable) {
    curr_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (frame_infos_.count(frame_id) == 0) {
    return;
  }

  // 不是可以移除的状态！
  if (!frame_infos_[frame_id].evictable_) {
    throw "Remove a non-evictable frame!";
  }

  frame_infos_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
