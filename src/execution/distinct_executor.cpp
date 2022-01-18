//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"
#include "execution/expressions/abstract_expression.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() {
  child_executor_->Init();
  output_columns_ = plan_->OutputSchema()->GetColumns();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple input_tuple;
  RID tuple_rid;

  while (child_executor_->Next(&input_tuple, &tuple_rid)) {
    std::vector<Value> values;
    hash_t tuple_hash = 0;
    for (auto &column : output_columns_) {
      Value value = input_tuple.GetValue(plan_->GetChildPlan()->OutputSchema(),
                                         plan_->GetChildPlan()->OutputSchema()->GetColIdx(column.GetName()));
      tuple_hash = HashUtil::CombineHashes(tuple_hash, HashUtil::HashValue(&value));
      values.emplace_back(value);
    }
    if (!IsDuplicate(values, tuple_hash)) {
      ht_[tuple_hash].emplace_back(values);
      *tuple = Tuple(values, plan_->OutputSchema());
      return true;
    }
  }
  return false;
}

bool DistinctExecutor::IsDuplicate(const std::vector<Value> &values, const hash_t tuple_hash) const {
  auto iter = ht_.find(tuple_hash);
  if (iter == ht_.end()) {
    return false;
  }
  const std::vector<std::vector<Value>> &tuples_raw_values = iter->second;
  for (const auto &raw_values : tuples_raw_values) {
    bool is_same = true;
    for (size_t i = 0; i < values.size(); i++) {
      if (values[i].CompareNotEquals(raw_values[i]) == CmpBool::CmpTrue) {
        is_same = false;
        break;
      }
    }
    if (is_same) {
      return true;
    }
  }
  return false;
}

}  // namespace bustub
