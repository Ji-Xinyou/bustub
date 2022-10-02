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

#include "common/logger.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())),
      index_infos_(exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_)),
      child_executor_(std::move(child_executor)),
      pos_(0) {}

void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  bool inserted = false;
  Tuple t;
  RID r;
  Transaction *txn = exec_ctx_->GetTransaction();

  // insert to table
  if (plan_->IsRawInsert()) {
    if (pos_ < plan_->RawValues().size()) {  // valid position
      std::vector<Value> raw_value = plan_->RawValues()[pos_++];
      t = Tuple(raw_value, &table_info_->schema_);
    } else {
      return false;
    }
  } else {
    // Lock(r);
    if (!child_executor_->Next(&t, &r)) {
      return false;
    }
  }

  inserted = table_info_->table_->InsertTuple(t, &r, txn);

  // update index
  if (inserted && !index_infos_.empty()) {
    for (auto &it : index_infos_) {
      Tuple key;
      key = t.KeyFromTuple(table_info_->schema_, it->key_schema_, it->index_->GetKeyAttrs());
      it->index_->InsertEntry(key, r, txn);

      // When rollback, txn_mgr delete new_key, does not use old_key
      IndexWriteRecord record(r, table_info_->oid_, WType::INSERT, t, Tuple{}, table_info_->oid_,
                              exec_ctx_->GetCatalog());
      txn->GetIndexWriteSet()->emplace_back(record);
    }
  }

  return inserted;
}

// the locking behavior on X-lock for all isolation level should be the same
void InsertExecutor::Lock(const RID &rid) {
  // LOG_DEBUG("InsertExec: Lock()");
  Transaction *txn = exec_ctx_->GetTransaction();
  exec_ctx_->GetLockManager()->LockExclusive(txn, rid);
}

// for 2PL, X-lock are released on commit/abort
void InsertExecutor::Unlock(const RID &rid) {
  // LOG_DEBUG("InsertExec: Unlock()");
}

}  // namespace bustub
