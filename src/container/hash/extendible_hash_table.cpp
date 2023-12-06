//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  std::lock_guard<std::mutex> lg(latch_);

  auto dir_index = IndexOf(key);
  auto &bucket = dir_[dir_index];

  return bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  std::lock_guard<std::mutex> lg(latch_);

  auto dir_index = IndexOf(key);
  auto &bucket = dir_[dir_index];

  return bucket->Remove(key);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  // 分裂的bucket
  num_buckets_++;
  auto bucket_depth = bucket->GetDepth();
  auto new_bucket = std::make_shared<Bucket>(bucket_size_, bucket_depth);

  // 重新分配bucket中的kv对
  auto &lists = bucket->GetItems();

  // 旧桶的hash值
  int old_mask = (1 << (bucket_depth - 1)) - 1;
  int curr_mask = (1 << bucket_depth) - 1;
  auto old_hash_value = std::hash<K>()(lists.front().first) & old_mask;

  for (auto iter = lists.begin(); iter != lists.end();) {
    auto curr_hash_value = std::hash<K>()(iter->first) & curr_mask;

    // 掩码左移一位后，计算出的hash值发生变化的，放到新的bucket中
    if (curr_hash_value != old_hash_value) {
      new_bucket->Insert(iter->first, iter->second);
      // 记得把原来的删除
      lists.erase(iter++);
    } else {
      iter++;
    }
  }

  // 改变dir目录的指针
  for (size_t i = 0; i < dir_.size(); i++) {
    if ((i & old_mask) == old_hash_value && (i & curr_mask) != old_hash_value) {
      dir_[i] = new_bucket;
    }
  }
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // UNREACHABLE("not implemented");
  std::lock_guard<std::mutex> lg(latch_);

  // 桶分裂一次不一定够，有可能n个元素的桶，分完后两个桶中的元素个数分别是n和0

  while (true) {
    auto index = IndexOf(key);
    auto &bucket = dir_[index];

    // Insert的时候，执行的动作刚好符合实验的描述：full或者没更新已经存在的key
    if (bucket->Insert(key, value)) {
      break;
    }

    // 插入失败，寻找位置
    // 注意策略：（实验中并未说明，需要本身理解可扩展hash的实现）
    // reference：https://www.geeksforgeeks.org/extendible-hashing-dynamic-approach-to-dbms/
    // 1. 如果溢出Bucket的局部深度等于全局深度，则需要进行Directory Expansion以及Bucket
    // Split。然后将全局深度和局部深度值加1。并分配适当的指针。
    // 2. 如果局部深度小于全局深度，则仅进行Bucket Split。然后仅将局部深度值增加 1。并且分配适当的指针。

    if (GetLocalDepthInternal(index) == GetGlobalDepthInternal()) {
      global_depth_++;
      // dir扩充1倍，仅仅是头部的二进制数变成1，不影响bucket的索引，因此只需要拷贝一份
      size_t dir_size = dir_.size();
      for (size_t i = 0; i < dir_size; i++) {
        dir_.emplace_back(dir_[i]);
      }
    } else {
      bucket->IncrementDepth();
      RedistributeBucket(bucket);
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if ((*it).first == key) {
      value = (*it).second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  for (auto it = list_.begin(); it != list_.end();) {
    if ((*it).first == key) {
      list_.erase(it++);
      return true;
    }
    it++;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // UNREACHABLE("not implemented");

  // if (std::any_of(list_.begin(), list_.end(), [&](auto &kv) {
  //       if (kv.first == key) {
  //         kv.second = value;
  //         return true;
  //       }
  //     })) {
  //   return true;
  // }
  // 如果key已经存在，更新key对应的值
  for (auto iter = list_.begin(); iter != list_.end(); iter++) {
    if (iter->first == key) {
      iter->second = value;
      return true;
    }
  }

  if (IsFull()) {
    return false;
  }

  list_.emplace_back(key, value);

  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
