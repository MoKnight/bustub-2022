//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
    //next_page_id?
    SetPageType(IndexPageType::LEAF_PAGE);
    SetParentPageId(parent_id);
    SetPageId(page_id);
    SetMaxSize(max_size);
    SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { 
  return next_page_id_; 
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key = array_.at[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::LookUp(const KeyType &key, ValueType& result,const KeyComparator& comparator) const {
  int first = 0;
  int last = GetSize()-1;
  int mid;
  int comp_res;

  while( first <= last ){
    mid = (first + last)/2;
    comp_res = comparator(KeyAt(mid),key);
    if( comp_res > 0 ){
      last = mid - 1;
    } else if( comp_res < 0 ) {
      first = mid+1;
    } else {
      break;
    }
  }

  if (comparator(array[mid].first, key) == 0) {
    result = array_[mid].second;
    return true;
  } else {
    return false;
  }
} 

INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, ValueType& value,const KeyComparator& comparator) const {
  int first = 0;
  int last = GetSize()-1;
  int mid;
  int comp_res;

  while( first <= last ){
    mid = (first + last)/2;
    comp_res = comparator(ValueAt(mid),key);
    if( comp_res > 0 ){
      last = mid - 1;
    } else if( comp_res < 0 ) {
      first = mid + 1;
    } else {
      break;
    }

    if (comparator(array[mid].first, key) == 0) {
      return false; //duplicate key
    } else {
      MoveData(mid+1,1);
      array_[mid+1] = value;
    }
  } 
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator& comparator) const{
  int first = 0;
  int last = GetSize()-1;
  int mid;
  int comp_res;

  while( first <= last ){
    mid = (first + last)/2;
    comp_res = comparator(KeyAt(mid),key);
    if( comp_res > 0 ){
      last = mid - 1;
    } else if( comp_res < 0 ) {
      first = mid+1;
    } else {
      break;
    }
  }

  if (comparator(array[mid].first, key) == 0) {
    MoveData(mid,-1);
    IncreaseSize(-1);
  } 
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveData(const int mid,const int distance) {
  memmove(array_[mid+distance], array_[mid],GetSize()-mid);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage* new_page) {
  int mid = GetSize()/2;
  memmove(array_[mid], new_page->GetArray(),GetSize()-mid);
  IncreaseSize(-mid);
  page->IncreaseSize(mid);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveToLast(BPlusTreeLeafPage* sibling_page){
  memmove(sibling_page->GetArray()+sibling_page->GetSize(), array_[0], GetSize());
  sibling_page->IncreaseSize(GetSize());
  IncreaseSize(GetSize());
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveToFirst(BPlusTreeLeafPage* sibling_page){
  sibling_page->MoveData(0,GetSize());
  memmove(sibling_page->GetArray(), array_[0], GetSize());
  sibling_page->IncreaseSize(GetSize());
  IncreaseSize(GetSize());
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFirst(BPlusTreeLeafPage* page){
  page->MoveData(0,1);
  memmove(page->GetArray()+page->GetSize()-1, array_[0],1);
  IncreaseSize(-1);
  page->IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToLast(BPlusTreeLeafPage* page){
  memmove(page->GetArray()+page->GetSize(), array_[0],1);
  MoveData(1,-1);
  IncreaseSize(-1);
  page->IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
MappingType* B_PLUS_TREE_LEAF_PAGE_TYPE::GetArray(){
  return array_;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
