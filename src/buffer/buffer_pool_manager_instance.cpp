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
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t* new_frame_id;
  if( free_list_.size() == 0 && !replacer_->Evict(new_frame_id)) {
    return nullptr;
  } else {
    if (free_list_.size() != 0){
      *new_frame_id = static_cast<frame_id_t>(*free_list_.begin());
    } else if( replacer_->Evict(new_frame_id) ){
      //nothing here
    } 

      page_id_t new_page_id = AllocatePage();
      Page* page = pages_+*new_frame_id;
      if(page->IsDirty()) {
        FlushPgImp(page->GetPageId());
      }

      page->Reset(new_page_id);
      page_table_->Insert(new_page_id,*new_frame_id);
      replacer_->RecordAccess(*new_frame_id);
      //how to pin?
      replacer_->SetEvictable(*new_frame_id, false);
      page->IncPinCnt();
      *page_id = page->GetPageId();

      return page;
  } 
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * { 
  frame_id_t frame_id;
  Page* page;
  if( !page_table_->Find(page_id,frame_id) ){
    if( (page = NewPgImp(&page_id)) == nullptr ){
      return nullptr;
    }
    disk_manager_->ReadPage(page_id,page->GetData());
  } else {
    page = pages_+frame_id; 
    //increase the count
    replacer_->SetEvictable(frame_id, false);
    replacer_->RecordAccess(frame_id);
    page->IncPinCnt();
  }

  return page; 
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool { 
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  page_table_->Find(page_id,frame_id);
  Page* page = pages_+frame_id;
  if ( page->GetPinCount() <= 0 ){
    return false;
  }
  page->DecPinCnt();
  page->SetIsDirty(is_dirty);
  if ( page->GetPinCount() == 0 ){
    replacer_->SetEvictable(frame_id,true);
  }
  return true; 
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool { 
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if( !page_table_->Find(page_id,frame_id) ){
    return false;
  }
  Page* page = pages_+frame_id;
  disk_manager_->WritePage(page_id, page->GetData());
  page->SetIsDirty(false);
  return true; 
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  std::unordered_map<frame_id_t,std::shared_ptr<FrameStatus>>* map = replacer_->GetFrameMap();
  frame_id_t frame_id;
  Page* page;
  for( auto i:*map){
    frame_id = i.first;
    page = pages_+frame_id;
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
    page->SetIsDirty(false);
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool { 
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if( !page_table_->Find(page_id,frame_id) ){
    return true;
  }
  Page* page = pages_+frame_id;
  if( page->GetPinCount() > 0 ){  //judge whether this page can be deleted
    return false;
  }

  if( !page_table_->Remove(page_id) ){
    return false;
  }

  replacer_->Remove(frame_id);
  free_list_.push_back(static_cast<int>(frame_id));
  DeallocatePage(page_id);
  return true; 
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
