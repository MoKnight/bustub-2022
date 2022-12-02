//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx),plan_(plan) {}

void SeqScanExecutor::Init() { 
    table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_name_);
    //do not have the trans id, just suppose the running trasaction is what i need
    iterator_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
    // throw NotImplementedException("SeqScanExecutor is not implemented"); 
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if( !(iterator_ == table_info_->table_->End()) ) {
        *tuple = *iterator_;
        *rid = tuple->GetRid();
        // memcpy(tuple,&(*iterator_),sizeof(Tuple));
        // Tuple tmp_tuple = const_cast<Tuple&>(*iterator_);
        return true;
    }
    return false; 
}

}  // namespace bustub
