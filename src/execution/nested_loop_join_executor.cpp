//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  predicate_ = plan_->Predicate();
  left_schema_ = left_executor_->GetOutputSchema();
  right_schema_ = right_executor_->GetOutputSchema();
  output_schema_ = plan_->OutputSchema();
  output_columns_ = plan_->OutputSchema()->GetColumns();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple right_tuple;
  RID right_rid;
  bool should_join = false;

  while (Advance(&left_tuple_, &left_rid_, &right_tuple, &right_rid) &&
         !(should_join =
               predicate_->EvaluateJoin(&left_tuple_, left_schema_, &right_tuple, right_schema_).GetAs<bool>())) {
  }

  if (!should_join) {
    return false;
  }

  std::vector<Value> values;
  for (auto &column : output_columns_) {
    values.emplace_back(column.GetExpr()->EvaluateJoin(&left_tuple_, left_schema_, &right_tuple, right_schema_));
  }

  *tuple = Tuple(values, output_schema_);
  return true;
}

bool NestedLoopJoinExecutor::Advance(Tuple *left_tuple, RID *left_rid, Tuple *right_tuple, RID *right_rid) {
  if (left_tuple_.GetLength() == 0) {
    if (!left_executor_->Next(left_tuple, left_rid)) {
      return false;
    }
  }

  while (!right_executor_->Next(right_tuple, right_rid)) {
    if (!left_executor_->Next(left_tuple, left_rid)) {
      return false;
    }
    right_executor_->Init();
  }
  return true;
}

}  // namespace bustub
