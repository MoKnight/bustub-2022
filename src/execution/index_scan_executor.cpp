//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),plan_(plan) {}

void IndexScanExecutor::Init() { 
    index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
    table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
    tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
    iterator_ = &tree_->GetBeginIterator();
    // throw NotImplementedException("IndexScanExecutor is not implemented"); 
    
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if( (iterator_ != &tree_->GetEndIterator()) ) {
        RID tmp_rid{(**iterator_).second};
        table_info_->table_->GetTuple(tmp_rid,tuple,exec_ctx_->GetTransaction());
        rid = &(tuple->GetRid());
        iterator_++;
        return true;
    }
    return false; 
}

}  // namespace bustub
