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
    : AbstractExecutor(exec_ctx), child_executor_(std::move(child_executor)), plan_(plan) {}

void InsertExecutor::Init() {
  const auto table_oid = plan_->TableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  transaction_ = exec_ctx_->GetTransaction();

  if (child_executor_) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple tuple_to_insert;
  RID tuple_to_insert_rid;
  if (plan_->IsRawInsert()) {
    if (index_for_tuple_to_insert_ >= plan_->RawValues().size()) {
      return false;
    }
    tuple_to_insert = Tuple(plan_->RawValuesAt(index_for_tuple_to_insert_), &table_info_->schema_);
    tuple_to_insert_rid = tuple_to_insert.GetRid();
    index_for_tuple_to_insert_++;
  } else {
    if (!child_executor_->Next(&tuple_to_insert, &tuple_to_insert_rid)) {
      return false;
    }
  }

  if (!table_info_->table_->InsertTuple(tuple_to_insert, &tuple_to_insert_rid, transaction_)) {
    return false;
  }

  for (const auto &index_info : table_indexes_) {
    auto &index = index_info->index_;
    Tuple key_tuple = tuple_to_insert.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index->GetKeyAttrs());
    index->InsertEntry(key_tuple, tuple_to_insert_rid, transaction_);
  }

  return true;
}
}  // namespace bustub
