//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),plan_(plan),child_(std::move(child)),aht_(plan->group_bys_,plan->agg_types_),aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
    child_->Init();
    Tuple cur_tuple{};
    RID rid{};
    if( plan_->aggregates_.size() != 0){
        while (child_->Next(&cur_tuple,&rid)) {
        aht_.InsertCombine(MakeAggregateKey(&cur_tuple), MakeAggregateValue(&cur_tuple));
        }
        aht_iterator_ = aht_.Begin();
    }
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    std::vector<Value> values;
    const Schema output_schema = GetOutputSchema();

    if( plan_->aggregates_.size() != 0 ){
        if( aht_iterator_ != aht_.End() ){
            values = aht_iterator_.Val().aggregates_;
            *tuple = Tuple(values, &GetOutputSchema()); //it's ok
            aht_iterator_++;
        }
    } else {
        return child_->Next(tuple, rid);
    } 
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
