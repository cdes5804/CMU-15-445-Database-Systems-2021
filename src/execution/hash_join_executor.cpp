//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/abstract_expression.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  const AbstractExpression *left_expression = plan_->LeftJoinKeyExpression();

  Tuple left_tuple;
  RID dummy_rid;  // not actually used

  while (left_executor_->Next(&left_tuple, &dummy_rid)) {
    Value value = left_expression->Evaluate(&left_tuple, plan_->GetLeftPlan()->OutputSchema());
    hash_t hash_value = HashUtil::HashValue(&value);
    ht_[hash_value].emplace_back(left_tuple);
  }

  if (!ht_.empty()) {
    end_iter_ = ht_.begin()->second.end();
    tuple_iter_ = end_iter_;  // set the tuple iterator to an invalid iterator for initialization
    output_columns_ = plan_->OutputSchema()->GetColumns();
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (ht_.empty()) {
    return false;
  }

  bool found_target = false;

  while (!found_target) {
    while (tuple_iter_ == end_iter_) {
      if (!right_executor_->Next(&right_tuple_, &right_rid_)) {
        return false;
      }
      right_value_ = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple_, plan_->GetRightPlan()->OutputSchema());
      hash_t hash_value = HashUtil::HashValue(&right_value_);
      if (ht_.find(hash_value) != ht_.end()) {
        tuple_iter_ = ht_[hash_value].begin();
        end_iter_ = ht_[hash_value].end();
      }
    }

    while (tuple_iter_ != end_iter_) {
      Value left_value =
          plan_->LeftJoinKeyExpression()->Evaluate(&(*tuple_iter_), plan_->GetLeftPlan()->OutputSchema());
      if (left_value.CompareEquals(right_value_) == CmpBool::CmpTrue) {
        found_target = true;
        break;
      }
      tuple_iter_++;
    }
  }

  std::vector<Value> values;
  for (auto &column : output_columns_) {
    values.emplace_back(column.GetExpr()->EvaluateJoin(&(*tuple_iter_), plan_->GetLeftPlan()->OutputSchema(),
                                                       &right_tuple_, plan_->GetRightPlan()->OutputSchema()));
  }
  tuple_iter_++;
  *tuple = Tuple(values, plan_->OutputSchema());
  return true;
}

}  // namespace bustub
