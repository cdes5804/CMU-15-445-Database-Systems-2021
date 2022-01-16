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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_iter_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr) {}

void SeqScanExecutor::Init() {
  const auto table_oid = plan_->GetTableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
  predicate_ = plan_->GetPredicate();
  output_columns_ = plan_->OutputSchema()->GetColumns();
  table_iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (table_iter_ != table_info_->table_->End() && predicate_ != nullptr &&
         !predicate_->Evaluate(&(*table_iter_), &table_info_->schema_).GetAs<bool>()) {
    table_iter_++;
  }

  if (table_iter_ == table_info_->table_->End()) {
    return false;
  }

  std::vector<Value> values;
  for (auto &column : output_columns_) {
    values.emplace_back(column.GetExpr()->Evaluate(&(*table_iter_), &table_info_->schema_));
  }

  *tuple = Tuple(values, plan_->OutputSchema());
  *rid = table_iter_->GetRid();
  table_iter_++;
  return true;
}

}  // namespace bustub
