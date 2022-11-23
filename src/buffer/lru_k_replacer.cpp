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

namespace bustub {

FrameStatus::FrameStatus(frame_id_t frame_id):frame_id_(frame_id),evictable_(false){
};
    
frame_id_t FrameStatus::GetFrameID() {return frame_id_;};

bool FrameStatus::GetEvictable() {return evictable_;};

std::vector<size_t> FrameStatus::Gettimestamp() {return timestamp_;};

size_t FrameStatus::GetSize() {return timestamp_.size();};

void FrameStatus::SetEvictable(bool set_evictable) {
    evictable_ = set_evictable;
};

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k),curr_size_(0) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { //finished
    std::scoped_lock<std::mutex> lock(latch_);
    std::vector<std::shared_ptr<FrameStatus>>* owner_vec;
    if( !frame_vec_.empty() ){
        owner_vec = &frame_vec_;
    } else if(!frame_vec_kth_.empty()){
        owner_vec = &frame_vec_;
    } else {  //no frame can be evcited
        return false;
    }

    std::sort(owner_vec->begin(),owner_vec->end(),[]( std::shared_ptr<FrameStatus> a,std::shared_ptr<FrameStatus> b){
        return a->Gettimestamp().at(0) < b->Gettimestamp().at(0);
    });

    *frame_id = owner_vec->at(0)->GetFrameID();
    Remove(*frame_id);
    return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) { //finished
    BUSTUB_ASSERT(frame_id >= replacer_size_,"wrong frame id");
    std::scoped_lock<std::mutex> lock(latch_);
    size_t cur_timestamp = GetCurTimeStamp();  //get the current timestamp
    if( frame_map_.find(frame_id) == frame_map_.end() ){
        std::shared_ptr<FrameStatus> new_frame_status_ptr (new FrameStatus(frame_id));
        new_frame_status_ptr->Gettimestamp().push_back(cur_timestamp);
        frame_map_.insert({frame_id, new_frame_status_ptr});
    } else {
        std::shared_ptr<FrameStatus> ptr = frame_map_.at(frame_id);
        ptr->Gettimestamp().push_back(cur_timestamp);
        if( ptr->GetSize() > k_ ){
            ptr->Gettimestamp().erase(ptr->Gettimestamp().begin());
        } 
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    BUSTUB_ASSERT( frame_id >= replacer_size_ || frame_map_.find(frame_id) == frame_map_.end(),"wrong frame id");
    std::scoped_lock<std::mutex> lock(latch_);
    if( frame_map_.at(frame_id)->GetEvictable() == set_evictable ) return;
    frame_map_.at(frame_id)->SetEvictable(set_evictable);
    if( set_evictable ){  //non-evictable to evictable
        if( frame_map_.at(frame_id)->GetSize() == k_ ){
            frame_vec_kth_.push_back(frame_map_.at(frame_id));
        } else {
            frame_vec_.push_back(frame_map_.at(frame_id));
        }   
    } else {    //evictable to non-evictable
        std::vector<std::shared_ptr<FrameStatus>>* owner_vec;
        if( frame_map_.at(frame_id)->GetSize() >= k_ ){ //check which vec contains the frame
            owner_vec = &frame_vec_kth_;
        } else {
            owner_vec = &frame_vec_kth_;
        }

        for( std::vector<std::shared_ptr<FrameStatus>>::iterator i = owner_vec->begin(); i != owner_vec->end();){
            if( i->get()->GetFrameID() == frame_id){
                owner_vec->erase(i);
                return;
            } else {
                i++;
            }
        }
    }
    UpdateSize();
    
}

void LRUKReplacer::Remove(frame_id_t frame_id) { //finished
    std::scoped_lock<std::mutex> lock(latch_);
    if( frame_map_.find(frame_id) == frame_map_.end() ){
        return;
    } else {
        BUSTUB_ASSERT( !frame_map_.at(frame_id)->GetEvictable(),"fail to remove unevictable frame" );
        std::vector<std::shared_ptr<FrameStatus>>* owner_vec;
        if( frame_map_.at(frame_id)->GetSize() == k_ ){ //check which vec contains the frame
            owner_vec = &frame_vec_kth_;
        } else {
            owner_vec = &frame_vec_kth_;
        }

        frame_map_.insert_or_assign(frame_id,nullptr);
        frame_map_.erase(frame_id);

        //earse from the vec
        for( std::vector<std::shared_ptr<FrameStatus>>::iterator i = owner_vec->begin(); i != owner_vec->end();){
            if( i->get()->GetFrameID() == frame_id){
                owner_vec->erase(i);
                break;
            } else {
                i++;
            }
        }
        UpdateSize();
    } 
}

auto LRUKReplacer::Size() -> size_t { 
    std::scoped_lock<std::mutex> lock(latch_);
    return curr_size_; 
}

auto LRUKReplacer::GetFrameMap() -> std::unordered_map<frame_id_t,std::shared_ptr<FrameStatus>>*{
    return &frame_map_;
}

void LRUKReplacer::UpdateSize(){
    std::scoped_lock<std::mutex> lock(latch_);
    curr_size_ = frame_vec_.size() + frame_vec_kth_.size();
}

}  // namespace bustub
