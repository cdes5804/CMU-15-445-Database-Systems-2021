//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  const auto table_oid = plan_->TableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  transaction_ = exec_ctx_->GetTransaction();

  if (child_executor_) {
    child_executor_->Init();
  }
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple tuple_to_update;
  RID tuple_to_update_rid;
  if (!child_executor_->Next(&tuple_to_update, &tuple_to_update_rid)) {
    return false;
  }

  Tuple updated_tuple = GenerateUpdatedTuple(tuple_to_update);
  if (!table_info_->table_->UpdateTuple(updated_tuple, tuple_to_update_rid, transaction_)) {
    return false;
  }

  for (const auto &index_info : table_indexes_) {
    auto &index = index_info->index_;
    Tuple old_key_tuple =
        tuple_to_update.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index->GetKeyAttrs());
    index->DeleteEntry(old_key_tuple, tuple_to_update_rid, transaction_);
    Tuple new_key_tuple =
        updated_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index->GetKeyAttrs());
    index->InsertEntry(new_key_tuple, tuple_to_update_rid, transaction_);
  }

  return true;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
