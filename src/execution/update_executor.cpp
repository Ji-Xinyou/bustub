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
  Transaction *txn = exec_ctx_->GetTransaction();

  if (child_executor_->Next(&t, &r)) {
    // update table
    Tuple updated_tuple = GenerateUpdatedTuple(t);
    Lock(r);
    updated = table_info_->table_->UpdateTuple(updated_tuple, r, txn);
    Unlock(r);

    // update index
    if (updated && !index_infos_.empty()) {
      for (auto &it : index_infos_) {
        Tuple new_key = updated_tuple.KeyFromTuple(table_info_->schema_, it->key_schema_, it->index_->GetKeyAttrs());
        Tuple old_key = t.KeyFromTuple(table_info_->schema_, it->key_schema_, it->index_->GetKeyAttrs());
        it->index_->DeleteEntry(old_key, r, txn);
        it->index_->InsertEntry(new_key, r, txn);

        // When rollback, txn_mgr delete new_key, insert old_key
        IndexWriteRecord record(r, table_info_->oid_, WType::UPDATE, updated_tuple, t, it->index_oid_,
                                exec_ctx_->GetCatalog());
        txn->GetIndexWriteSet()->emplace_back(record);
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

void UpdateExecutor::Lock(const RID &rid) {
  Transaction *txn = exec_ctx_->GetTransaction();
  if (!txn->IsExclusiveLocked(rid)) {
    if (txn->IsSharedLocked(rid)) {
      GetExecutorContext()->GetLockManager()->LockUpgrade(txn, rid);
    } else {
      GetExecutorContext()->GetLockManager()->LockExclusive(txn, rid);
    }
  }
}

// 2PL unlocks when commit/abort
void UpdateExecutor::Unlock(const RID &rid) {}

}  // namespace bustub
