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
  table_iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (table_iter_ != table_info_->table_->End() && plan_->GetPredicate() != nullptr &&
         !plan_->GetPredicate()->Evaluate(&(*table_iter_), &table_info_->schema_).GetAs<bool>()) {
    table_iter_++;
  }

  if (table_iter_ == table_info_->table_->End()) {
    return false;
  }

  switch (exec_ctx_->GetTransaction()->GetIsolationLevel()) {
    case IsolationLevel::READ_COMMITTED:
      if (!exec_ctx_->GetTransaction()->IsSharedLocked(table_iter_->GetRid()) &&
          !exec_ctx_->GetTransaction()->IsExclusiveLocked(table_iter_->GetRid()) &&
          !(
              // S lock
              exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), table_iter_->GetRid()) &&
              // but release immediately
              exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), table_iter_->GetRid()))) {
        return false;
      }
      break;
    case IsolationLevel::REPEATABLE_READ:
      if (!exec_ctx_->GetTransaction()->IsSharedLocked(table_iter_->GetRid()) &&
          !exec_ctx_->GetTransaction()->IsExclusiveLocked(table_iter_->GetRid()) &&
          !exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), table_iter_->GetRid())) {
        return false;
      }
      break;
    default:
      break;
  }

  std::vector<Value> values;
  for (auto &column : plan_->OutputSchema()->GetColumns()) {
    values.emplace_back(column.GetExpr()->Evaluate(&(*table_iter_), &table_info_->schema_));
  }

  *tuple = Tuple(values, plan_->OutputSchema());
  *rid = table_iter_->GetRid();
  table_iter_++;
  return true;
}

}  // namespace bustub
