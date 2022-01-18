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
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();

  Tuple tuple;
  RID rid;

  while (child_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  aht_iterator_ = aht_.Begin();
  output_columns_ = plan_->OutputSchema()->GetColumns();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  std::vector<Value> group_bys;
  std::vector<Value> aggregates;
  do {
    if (aht_iterator_ == aht_.End()) {
      return false;
    }

    group_bys = aht_iterator_.Key().group_bys_;
    aggregates = aht_iterator_.Val().aggregates_;

    ++aht_iterator_;
  } while (plan_->GetHaving() != nullptr &&
           !plan_->GetHaving()->EvaluateAggregate(group_bys, aggregates).GetAs<bool>());

  std::vector<Value> values;
  for (auto &column : output_columns_) {
    values.emplace_back(column.GetExpr()->EvaluateAggregate(group_bys, aggregates));
  }

  *tuple = Tuple(values, plan_->OutputSchema());
  return true;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
