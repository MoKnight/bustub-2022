//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::make_unique<AbstractExecutor>(child_executor)) {}

void DeleteExecutor::Init() { 
    table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
    // throw NotImplementedException("DeleteExecutor is not implemented"); 
    child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
    TableHeap* table_heap = table_info_->table_.get();
    int size = 0;
    while (child_executor_->Next(tuple, rid)) {
      if(!table_heap->MarkDelete(*rid,exec_ctx_->GetTransaction())){
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
    // return false; 
}

}  // namespace bustub
