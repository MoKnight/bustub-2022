//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetParentPageId(parent_id);
  SetPageId(page_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key = array_.at[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_.at[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { 
  ValueType value = array_.at[index].second;
  return value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const->int {
    for (int i = 0; i < GetSize(); i++) {
        if (value == ValueAt(i)) {
            return i;
        }
    }
    return -1;
}

//add zjx
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const{  
  int first = 1;
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

  if (comparator(array[mid].first, key) <= 0) {
      return array[mid].second;
  } else {
      return array[mid - 1].second;
  }
}
                                       

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(
    const KeyType &key, 
    const ValueType & new_leaf_id, 
    const KeyComparator& comparator)->bool{
  int first = 1;
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
    return false;
  }

  if (comparator(array[mid].first, key) < 0) {
    MoveData(mid+1,1);
    array_[mid+1].first = key;
    array_[mid+1].second = value;
  } else { //insert before never happen
    MoveData(mid,1);
    array_[mid].first = key;
    array_[mid].second = value;
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertByIndex(
    const int index,
    const KeyType &key, 
    const ValueType & new_leaf_id)->bool{
  assert( index>=0 && index < GetMaxSize()-1 );
  MoveData(index+1,1);
  array_[index+1].first = key;
  array_[index+1].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveData(const int mid,const int distance) {
  memmove(array_[mid+distance], array_[mid],GetSize()-mid);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage* new_page) {
  int mid = GetSize()/2;
  memmove(array_[mid], new_page->array_[0],GetSize()-mid);
  //maybe set an InValid id is needed?
  //new_page->array_[0].first = INVLAID_PAGE_ID;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
