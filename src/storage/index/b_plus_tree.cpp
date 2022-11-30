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
  
  // UnLatchAndUnpinPageSet(transaction,OperationType::Get);
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
    new_inter->InsertByIndex(index,key,value); //insert into internal page
  } else {
    inter->InsertByIndex(index,key,value); //insert into internal page
  }
  
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
    new_page->Init(new_page_id, page->GetParentPageId(),page->GetNextPageId(),leaf_max_size_);
    page->SetNextPageId(new_page_id);
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
  leaf_page->Remove(key,comparator_);
  if( leaf_page->GetSize() < leaf_page->GetMinSize() ){
    MergeOrRedistribute(leaf_page,transaction);
  } else {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),true);
  }
}

/**
 * @brief Ajust the Rootpage in 2 cases:
 *        1.the root page is a leaf page, need to be deleted
 *        2.the root page is internal page, only has 1 child, change the root page to the child
 * 
 * @param page 
 * @param transaction 
 * @return true 
 * @return false 
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::AjustRoot(BPlusTreePage* page,Transaction *transaction)->bool{
  if( page->IsLeafPage() ){
    if( page->GetSize() != 0 ) return true;
    root_id_mutex_.lock();
    root_page_id_ = INVALID_PAGE_ID;
    root_id_mutex_.unlock();
    DeleteRootPageId();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return buffer_pool_manager_->DeletePage(page->GetPageId());
  }

  if( page->GetSize() != 1 ) return true;
  BPlusTreeInternalPage* root_page = reinterpret_cast<BPlusTreeInternalPage*>(page);
  Page* buffer_page = buffer_pool_manager_->Fetch(root_page->array[0].second);
  if( buffer_page == nullptr ){
    return false;
  }
  BPlusTree* leaf_page = reinterpret_cast<BPlusTreeInternalPage*>(buffer_page->GetData());
  root_id_mutex_.lock();
  root_page_id_ = leaf_page->GetPageId();
  root_id_mutex_.unlock();
  UpdateRootPageId();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  return buffer_pool_manager_->DeletePage(page->GetPageId());
}

/**
 * @brief 
 * 
 * @param node 
 * @param isLeftSibling 
 * @return Page_type* 
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename Page_type>
auto BPLUSTREE_TYPE::FindSibling(Page_type *node,  bool &isLeftSibling)->Page_type * {
  Page* buffer_page = buffer_pool_manager_->Fetch(node->GetParentPageId());
  if( buffer_page == nullptr ){
    return nullptr;
  }
  BPInternalPage *parent_page = reinterpret_cast<BPInternalPage *>(page->GetData());
  int index = parent_page->ValueIndex(node->GetPageId());
  int siblingIndex;
  if (index == 0) {
      siblingIndex = index + 1;
      isLeftSibling = false;
  } else {
      siblingIndex = index - 1;
      isLeftSibling = true;
  }

  Page* buffer_sibling_page = buffer_pool_manager_->Fetch(parent_page->ValueAt(siblingIndex));
  if( buffer_sibling_page == nullptr ){
    return false;
  }
  Page_type* sibling_page = reinterpret_cast<Page_type*>(buffer_sibling_page->GetData());
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
  return sibling_page;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename Page_type> auto BPLUSTREE_TYPE::MergeOrRedistribute(Page_type* page, Transaction* transaction)->bool{
  if( page->IsRootPage() ){
    return AjustRoot(page,transaction);
  } else {
    bool isLeft;
    Page_type* sibling_page = FindSibling(page, isLeft);
    if( sibling_page == nullptr ){
      return false;
    }
 
    Page* buffer_parent_page = buffer_pool_manager_->Fetch(page->GetParentID());
    if( buffer_parent_page == nullptr ){
      return false;
    }
    BPlusTreeInternalPage* parent_page = reinterpret_cast<BPlusTreeInternalPage*>(buffer_parent_page->GetData());

    if( sibling_page->GetSize() + page->GetSize() <= page->GetMaxSize() ){
      Merge(sibling_page,page,isLeft);
      parent_page->Remove(page->GetPageId(),comparator_);
      if( parent_page->GetSize() < parent_page->GetMinSize() ){
        return MergeOrRedistribute(parent_page,transaction);
      } 
    } else {
      Redistribute(sibling_page,page,parent_page,isLeft);
    }
    
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(),true);
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename Page_type> auto BPLUSTREE_TYPE::Merge(Page_type* page, Page_type* sibling_page,bool isLeft)->bool{
  if( isLeft ){
    page->MoveToLast(sibling_page);  
  } else {
    sibling_page->MoveToFisrt(page);
  }
  buffer_pool_manager_->UnpinPage(sibling_page,page->GetPageId(),true);
  buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
  buffer_pool_manager_->DeletePage(page->GetPageId());
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename Page_type> auto BPLUSTREE_TYPE::Redistribute(Page_type* page, Page_type* sibling_page,InternalPage* parent_page,bool isLeft)->bool{
  if( isLeft ){
    KeyType newkey = (sibling_page->GetArray()+(sibling_page->GetSize()-1)).first;
    int index = parent_page->ValueIndex(page->GetPageId());
    sibling_page->MoveLastToFisrt(page);
    parent_page->SetKeyAt(index,newkey);
  } else {
    KeyType newkey = (sibling_page->GetArray()+1).first;
    int index = parent_page->ValueIndex(sibling_page->GetPageId());
    sibling_page->MoveFirstToLast(page);
    parent_page->SetKeyAt(index,newkey);
  }
  buffer_pool_manager_->UnpinPage(sibling_page,page->GetPageId(),true);
  buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
  return true;
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { 
  BPlusTreeLeafPage* leaf_page =  FindLeafPage(INVALID_PAGE_ID,OperationType::Get,comparator_,-1);
  if( leaf_page == nullptr ){
    return {};
  }
  return INDEXITERATOR_TYPE(leaf_page,0,buffer_pool_manager_); 
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  BPlusTreeLeafPage* leaf_page =  FindLeafPage(key,OperationType::Get,comparator_);
  if( leaf_page == nullptr ){
    return {};
  }
  int index = leaf_page->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(leaf_page,index,buffer_pool_manager_);
  // return INDEXITERATOR_TYPE(); 
  }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { 
  BPlusTreeLeafPage* leaf_page =  FindLeafPage(key,OperationType::Get,comparator_);
  if( leaf_page == nullptr ){
    return {}; //maybe need to change
  }
  return INDEXITERATOR_TYPE(leaf_page,leaf_page->GetSize()+1,buffer_pool_manager_);
  // return INDEXITERATOR_TYPE(); 
  
}

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
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,const OperationType operation,Transaction* transaction,int location)->B_PLUS_TREE_LEAF_PAGE_TYPE*{
  root_id_mutex_.lock();
  if( IsEmpty() ) {
      root_id_mutex_.unlock();
      return nullptr;
  }
  
  auto page = buffer_pool_manager_->FetchPage(GetRootPageId());
  if( page == nullptr ){
    root_id_mutex_.unlock();
    return nullptr;
  }
  root_id_mutex_.unlock();

  BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while( !tree_page->IsLeafPage() ){   
    BPlusTreeInternalPage* inter_page =  reinterpret_cast<BPlusTreeInternalPage*>(tree_page);
    page_id_t next_page_id;
    if (location == -1) { //leftmost
      next_page_id = inter_page->ValueAt(0);
    } else if(location == 1) { //rightmost
      next_page_id = inter_page->ValueAt(inter_page->GetSize()-1);
    } else {
      next_page_id = inter_page->Lookup(key, comparator_);
    }
    Page* last_page = page;  //maintain the parent buffer page
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
      buffer_pool_manager_->UnpinPage(last_page->GetPageId(), false);
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

//add zjx
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteRootPageId() {
    HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
    header_page->DeleteRecord(index_name_);
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
