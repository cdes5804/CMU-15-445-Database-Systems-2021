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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  const auto table_oid = plan_->TableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  transaction_ = exec_ctx_->GetTransaction();

  if (child_executor_) {
    child_executor_->Init();
  }
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple tuple_to_delete;
  RID tuple_to_delete_rid;
  if (!child_executor_->Next(&tuple_to_delete, &tuple_to_delete_rid)) {
    return false;
  }

  if (!table_info_->table_->MarkDelete(tuple_to_delete_rid, transaction_)) {
    return false;
  }

  for (const auto &index_info : table_indexes_) {
    auto &index = index_info->index_;
    Tuple old_key_tuple =
        tuple_to_delete.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index->GetKeyAttrs());
    index->DeleteEntry(old_key_tuple, tuple_to_delete_rid, transaction_);
  }

  return true;
}

}  // namespace bustub
