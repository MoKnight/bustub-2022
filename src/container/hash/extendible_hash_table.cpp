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
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size): global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
      std::shared_ptr new_bucket(new Bucket(bucket_size));
      dir_.push_back(new_bucket);
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
auto ExtendibleHashTable<K, V>::GetBucketsSize() const -> int{
  return GetBucketsSizeInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetBucketsSizeInternal() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return bucket_size_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if( GetNumBucketsInternal() == 0 ){ //hashtable is empty, perhaps it's not gonna happen
    return false;
  }

  size_t hash_val = IndexOf(key);  
  std::shared_ptr<Bucket> cur = dir_[hash_val];
  return cur->Find(key,value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if( GetNumBucketsInternal() == 0 ){ //hashtable is empty, perhaps it's not gonna happen
    return false;
  }

  size_t hash_val = IndexOf(key);
  std::shared_ptr<Bucket> cur = dir_[hash_val];
  if( cur->Find(key, value) ){
    return cur->remove(key, value);
  }

  return false;
  // UNREACHABLE("not implemented");
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t hash_val = IndexOf(key);
  const Bucket* cur = dir_[hash_val]->get();
  if( !cur->Insert(key,value) ) {  //try to insert
    if( cur->IsFull() ){
      if( cur->GetDepth < GetGlobalDepthInternal() ){ //split on busket level
        cur->IncrementDepth();
        std::shared_ptr<Bucket> new_busket = new Bucket( GetBucketsSizeInternal(), cur->GetDepth());
        std::shared_ptr<Bucket> replaced_busket = new Bucket( GetBucketsSizeInternal(), cur->GetDepth());
        dir_[hash_val] = new_busket;

        if( (hash_val) % 2 ){
          dir_[hash_val+1] = replaced_busket;
        } else {
          dir_[hash_val-1] = replaced_busket;
        }

        RedistributeBucket(cur);
        dir_[hash_val]->Insert(key,value);
      } else { //split on table level
        global_depth_++;
        cur->IncrementDepth();
        std::vector<std::shared_ptr<Bucket>> new_dir_;
        size_t old_hash_val = hash_val;
        for( int i = 0; i < pow(2,GetGlobalDepthInternal);i+=2 ) {
          if( i/2 == old_hash_val ){
            std::shared_ptr<Bucket> new_busket = new Bucket( GetBucketsSizeInternal(), cur->GetDepth());
            std::shared_ptr<Bucket> replaced_busket = new Bucket( GetBucketsSizeInternal(), cur->GetDepth());
            new_dir_.push_back(new_busket);
            new_dir_.push_back(replaced_busket);
            RedistributeBucket(cur);
            hash_val = IndexOf(key);
            new_dir_[hash_val]->Insert(key,value);
          }
          new_dir_.push_back(dir_[i/2]);
          new_dir_.push_back(dir_[i/2]);
        }
      }
      num_buckets_++;  //change the num of buckets;
    }   
  }
  // UNREACHABLE("not implemented");
}

template <typename K, typename V>
auto RedistributeBucket(std::shared_ptr<ExtendibleHashTable<K, V>::Bucket> bucket) -> void{
  for( auto i:bucket->GetItems()){ 
    size_t hash_val = IndexOf(key);
    dir_[hash_val]->Insert(key,value);
  }
  return;
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for( auto i : list_ ){
    if( i.first == key ){
      value = i.second;
      return true;
    }
  }
  return false
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for( std::list<std::pair<K, V>>::iterator iter = list_.begin(); iter != list_.end(); )
  {
    if( iter->first == key ){
      list_.erase(iter);
      return true;
    }
      else
        iter++;
  }
  return false;
  // UNREACHABLE("not implemented");
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if( IsFull() ){
    reutrn false;
  }

  for( auto& i : list_ ){
    if( i.first == key ){
      i.second = value;
      return true;
    }
  }

  list_.emplace(list_.end(),std::move(std::pair<K,V>{key,value}));
  return true;

}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
