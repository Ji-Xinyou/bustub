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
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())),
      index_infos_(exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)),
      child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  if (child_executor_) {
    child_executor_->Init();
  }
}

/** NOTE: parameters should never be used */
auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (!child_executor_) {
    return false;
  }

  bool updated = false;
  Tuple t;
  RID r;

  if (child_executor_->Next(&t, &r)) {
    // update table
    Tuple updated_tuple = GenerateUpdatedTuple(t);
    updated = table_info_->table_->UpdateTuple(updated_tuple, r, exec_ctx_->GetTransaction());

    // update index
    if (updated && !index_infos_.empty()) {
      for (auto &it : index_infos_) {
        Tuple new_key, old_key;
        new_key = updated_tuple.KeyFromTuple(table_info_->schema_, it->key_schema_, it->index_->GetKeyAttrs());
        old_key = t.KeyFromTuple(table_info_->schema_, it->key_schema_, it->index_->GetKeyAttrs());
        it->index_->DeleteEntry(old_key, r, exec_ctx_->GetTransaction());
        it->index_->InsertEntry(new_key, r, exec_ctx_->GetTransaction());
      }
    }
  }

  return updated;
}

auto UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) -> Tuple {
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
