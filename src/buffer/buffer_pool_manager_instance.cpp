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

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock_guard(latch_);

  frame_id_t frame_id = -1;

  /**
   * @brief 先从free_list_中找到free的frame，如果free_list_没有空余的。则进行LRUK换出
   * 如果换出的frame已经Dirty，需要写入磁盘
   *
   */

  // free_list中存储着所有为free类型的frame
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    }
    // 将对应的page—table中的记录删除
    page_table_->Remove(pages_[frame_id].GetPageId());
  } else {
    // 如果free list中没有空闲的frame，且replacer换出失败
    page_id = nullptr;
    return nullptr;
  }

  // 拿到的frame，将其初始化: 获取新的page id,插入page_table(根据page id找到buffer pool中的frame)
  *page_id = AllocatePage();
  page_table_->Insert(*page_id, frame_id);

  pages_[frame_id].ResetMemory();  // 重置内存
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;

  // 更新LRUK的记录信息
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  // 返回frame id所在的pages中的指针
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  assert(page_id != INVALID_PAGE_ID);
  std::lock_guard<std::mutex> lock_guard(latch_);

  // 类似NewPg，先从buffer pool中，寻找，如果没有，需要从disk中读取page到pool中
  frame_id_t frame_id = -1;

  // page_table中有没有对应的frame id
  if (page_table_->Find(page_id, frame_id)) {
    // 更新pin count
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    pages_[frame_id].pin_count_++;

    return &pages_[frame_id];
  }

  // 从free_list_中寻找对应空间
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    }
    page_table_->Remove(pages_[frame_id].GetPageId());
  } else {
    // free_list中没有空余的frame，且无法换出页面
    return nullptr;
  }

  // 更新page_id ==> frame_id 映射关系
  page_table_->Insert(page_id, frame_id);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;

  // 从disk中读取一个page
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> lock_guard(latch_);

  frame_id_t frame_id = -1;
  if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].GetPinCount() == 0) {
    return false;
  }

  pages_[frame_id].pin_count_--;
  pages_[frame_id].is_dirty_ |= is_dirty;

  // 记得标记count为0时，
  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock_guard(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  //
  disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
  pages_->is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // frame_id_t tmp;
  std::lock_guard<std::mutex> lock_guard(latch_);

  for (size_t frame_id = 0; frame_id < pool_size_; frame_id++) {
    frame_id_t tmp;
    if (page_table_->Find(pages_[frame_id].GetPageId(), tmp)) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      pages_->is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock_guard(latch_);

  DeallocatePage(page_id);
  frame_id_t frame_id = -1;

  // 未找到page id对应的frame id
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }

  // 如果被pinned，不能删除
  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }

  // dirty的要刷写到磁盘中
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    pages_->is_dirty_ = false;
  }

  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  page_table_->Remove(page_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
