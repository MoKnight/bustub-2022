#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  return root_page_id_ == INVALID_PAGE_ID; 
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = FindLeafPage(key, OperationType::Get, transaction);
  if( leaf_page == nullptr ){
    return false;
  }

  result->resize(1);
  auto ret = leaf_page->Lookup(key, result[0], comparator_);
  
  UnLatchAndUnpinPageSet(transaction,OperationType::Get);
  //buffer_pool_manager_->UnpinPage(leaf_page->GetPageId());//suppose need to relase now

  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  BPlusTreeLeafPage *leaf_page = FindLeafPage(key, OperationType::Insert, transaction);
  root_id_mutex_.lock();
  if( leaf_page == nullptr ){
    if( leaf_page = NewTree() == nullptr) {
      root_id_mutex_.unlock();
      return false;
    }
  }
  root_id_mutex_.unlock();
  return InsertIntoLeaf(key, value, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction)->bool{
  // check if the value exists
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf = FindLeafPage(key, OperationType::INSERT, transaction);
  ValueType v;
  bool isExit = leaf->Lookup(key, v, comparator_);
  if (isExit) {
    if( !buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false)){ //unpin current leaf page
      return false;
    } 
    // UnLatchAndUnpinPageSet(transaction, OperationType::INSERT); //unpin the other pages
    return false;
  }

  if( page->GetSize == page->GetMaxSize() ){
    //split the old pages
    BPlusTreeLeafPage *new_leaf = SplitPage(leaf)
    if( new_leaf == nullptr ){
      return false;
    }
    InsertIntoLeafs(leaf, new_leaf, key, value, comparator_);
    //insert into parent page
    InsertIntoParent(leaf, new_leaf->KeyAt(0), new_leaf, transaction);

  } else {
    leaf->Insert(key, value, comparator_ );//no need to check
    if( !buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true) ){
      return false;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeafs( 
    LeafPage *leaf, 
    LeafPage *new_leaf,
    const KeyType &key, 
    const ValueType &value, 
    Transaction *transaction)->bool{
  if( comparator_(new_leaf->KeyAt(0),key) >= 0  ){
    leaf->Insert(key, value, comparator_ );
  } else {
    new_leaf->Insert(key, value, comparator_ );
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoInters( 
    InternalPage *new_inter, 
    InternalPage *inter,
    const ValueType &old_value,
    const KeyType &key, 
    const ValueType &value, 
    Transaction *transaction)->bool{
  int index = inter->ValueIndex(old_value);
  if( index == -1 ){
    index = new_inter->ValueIndex(old_value);
  }
  new_inter->InsertByIndex(index,key,value); //insert into internal page
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitPage(InternalPage *page)->InternalPage *{
    page_id_t new_page_id;
    Page *new_buffer_page = buffer_pool_manager_->NewPage(new_page_id);
    if (new_buffer_page == nullptr) {
        return nullptr;
    }

    InternalPage *new_page = reinterpret_cast<InternalPage *>(new_buffer_page->GetData());
    if( Page_type )
    new_page->Init(new_page_id, page->GetParentPageId(),internal_max_size_);
    page->MoveHalfTo(new_page);
    return new_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitPage(LeafPage *page)->LeafPage *{
    page_id_t new_page_id;
    Page *new_buffer_page = buffer_pool_manager_->NewPage(new_page_id);
    if (new_buffer_page == nullptr) {
        return nullptr;
    }

    LeafPage *new_page = reinterpret_cast<LeafPage *>(new_buffer_page->GetData());
    new_page->Init(new_page_id, page->GetParentPageId(),leaf_max_size_);
    page->MoveHalfTo(new_page);
    return new_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoParent(
  BPlusTreePage* leaf, 
  const KeyType &key, 
  BPlusTreePage* new_leaf, 
  Transaction *transaction)->bool
{
  root_id_mutex_.lock();
  if( root_page_id_ == leaf->GetPageId() ) {
    page_id_t new_parent_page_id;
    Page *new_buffer_page = buffer_pool_manager_->NewPage(new_parent_page_id);
    if (new_buffer_page == nullptr) {
        return false;
    }
    root_page_id_ = new_parent_page_id;
    UpdateRootPageId();
    root_id_mutex_.unlock();
    BPlusTreeInternalPage *new_parent_page = reinterpret_cast<BPlusTreeInternalPage *>(new_buffer_page->GetData());
    new_parent_page->Init(new_parent_page_id,INVALID_PAGE_ID,internal_max_size_);
    new_parent_page->InsertByIndex(-1,INVALID_PAGE_ID,leaf->GetPageId());
    new_parent_page->InsertByIndex(0,key,new_leaf->GetPageId());
    //need to unpin
    return true;
  } else {
    root_id_mutex_.unlock();
    Page* buffer_parent_page = buffer_pool_manager_->FetchPage(page->GetParentPageId());
    if( buffer_parent_page == nullptr ){
      return false;
    } 
    BPlusTreeInternalPage* parent_page = reinterpret_cast<BPlusTreeInternalPage*>(buffer_parent_page->GetData());
    if( parent_page->GetSize() == parent_page->GetMaxSize() ){
      BPlusTreeInternalPage* new_parent_page = SplitPage(parent_page);
      if( new_parent_page == nullptr ){
        return false;
      }
      //insert into new pages
      InsertIntoInters(new_parent_page, parent_page, leaf, key, new_leaf, comparator_);
      return InsertIntoParent(parent_page,new_parent_page->KeyAt(0), new_parent_page, transaction); //again
    } else {
      return parent_page->Insert(key,new_leaf,comparator_); //insert the two pages into parent page
    }
  }

  
}


// INDEX_TEMPLATE_ARGUMENTS
// void BPLUSTREE_TYPE::UnLatchAndUnpinPageSet(Transaction* transaction, const OperationType operation){
//   if( operation == OperationType::Get() ){

//   } else {

//   }
// }

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if( IsEmpty() ){
    return;
  } 

  BPlusTreeLeafPage* page = FindLeafPage(key, OperationType::DELETE, transaction);
  if( page == nullptr ){
    return 
  }
  RemoveFromLeaf(page,key,transaction);
  buffer_pool_manager_->UnpinPage(page);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromLeaf(LeafPage* leaf_page,const KeyType &key, Transaction *transaction){
  leaf_page->Remove(key);
  if( leaf_page->GetSize() < leaf_page->GetMinSize()){
    
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { 
  return root_page_id_; 
}

/**
 * @brief 
 * 
 * @param key 
 * @param operation 
 * @param transaction 
 * @return B_PLUS_TREE_LEAF_PAGE_TYPE* remember to unpin by the caller
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,const OperationType operation,Transaction* transaction)->B_PLUS_TREE_LEAF_PAGE_TYPE*{
  // root_id_mutex_.lock();
  if( IsEmpty() ) {
      // root_id_mutex_.unlock();
      return nullptr;
  }
  
  auto page = buffer_pool_manager_->FetchPage(GetRootPageId());
  if( page == nullptr ){
    return nullptr;
  }

  BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while( !tree_page->IsLeafPage() ){    
    page_id_t next_page_id = internalPage->Lookup(key, comparator_);
    Page* last_page = page;  //maintain the parent page
    page = buffer_pool_manager_->FetchPage(next_page_id);
    if( page == nullptr ){
      return nullptr;
    }

    tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

    //lock check
    if (operation == OperationType::GET) {
        page->RLatch();
    } else {
        page->WLatch();
    }

    if (transaction != nullptr) {
      // if (operation == OperationType::GET) {
      //     // 释放祖先节点的锁
      //     UnLatchAndUnpinPageSet(transaction, operation);
      //     assert(transaction->GetPageSet()->size() == 0);
      // } else {
      //     // 当前节点是safe的情况下，才释祖先父节点的锁
      //     if (bp->IsSafe(operation)) {
      //         UnLatchAndUnpinPageSet(transaction, operation);
      //     }
        // }
    } else {
      assert(operation == OperationType::GET);
      last_page->RUnlatch();
      // if (tree_page->IsRootPage()) {
      //     root_id_mutex_.unlock();
      // }
      buffer_pool_manager_->UnpinPage(lastPage->GetPageId(), false);
    }
  }
  return static_cast<BPlusTreeLeafPage *>(tree_page); 
}

/**
 * @brief create a new bplus tree, don't need to lock
 * 
 * @return B_PLUS_TREE_LEAF_PAGE_TYPE* 
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::NewTree()->B_PLUS_TREE_LEAF_PAGE_TYPE* {
  page_id_t page_id;
  Page* new_page = buffer_pool_manager_->NewPage(page_id);
  if( new_page == nullptr ) {
    return nullptr;
  }

  root_page_id_ = page_id;
  BPlusTreeLeafPage* leaf_page = reinterpret_cast<BPlusTreeLeafPage*>(new_page->GetData());
  buffer_pool_manager_->UnpinPage(page_id);
  leaf_page->Init(page_id, INVALID_PAGE_ID,leaf_max_size_);
  return leaf_page;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
