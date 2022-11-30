/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(LeafPage* leaf_page,int index,BufferPoolManager* buffer_pool_manager):
    leaf_page_(leaf_page),
    index_(index),
    buffer_pool_manager(buffer_pool_manager){
};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { 
    return (leaf_page_ == nullptr) || (index_ >= leaf_page_->GetSize());
    // throw std::runtime_error("unimplemented"); 
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { 
    if( IsEnd() ) return {};
    return leaf_page_->GetItem();
    // throw std::runtime_error("unimplemented"); 
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & { 
    if( IsEnd() ) return *this;//maybe not return the original one
    if( index_ < leaf_page->GetSize()-1){
        index_++;
    } else if( leaf_page_->GetNextPageId() != INVALID_PAGE_ID ){
        Page* buffer_next_page = buffer_pool_manager_->FetchPage(leaf_page_->GetNextPageId());
        if( buffer_next_page == nullptr ){
            return {};
        }
        buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId());
        BPlusTreeLeafPage* next_page = reinterpret_cast<BPlusTreeLeafPage*>(buffer_next_page->GetData())
        index_ = 0;
        leaf_page_ = next_page;
    }
    return *this;
    // throw std::runtime_error("unimplemented"); 
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
