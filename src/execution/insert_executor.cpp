//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::make_unique<AbstractExecutor>(child_executor)) {
}

auto InsertExecutor::GetPlanNode()->const AbstractPlanNode*{
    return plan_;
}

void InsertExecutor::Init() { 
    table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
    if( child_executor_->GetPlanNode()->GetType() != PlanType::Values ){
        throw NotImplementedException("wrong type of child executors"); 
    }
    child_executor_->Init();
}

auto InsertExecutor::Next(Tuple *tuple, [[maybe_unused]]RID *rid) -> bool { 
    TableHeap* table_heap = table_info_->table_.get();
    int size = 0;
    while (child_executor_->Next(tuple, rid)) {
      if(!table_heap->InsertTuple(*tuple,rid,exec_ctx_->GetTransaction())){
          return false;
      }
      size++;
    }
    // how to produce a integer Tuple
    std::vector<Value> values{};
    values.reserve(GetOutputSchema().GetColumnCount());
    values.push_back({INTEGER,size});
    *tuple = Tuple{values, &GetOutputSchema()};
    return true; 
}

}  // namespace bustub
