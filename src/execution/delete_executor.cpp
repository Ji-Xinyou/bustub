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
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())),
      index_infos_(exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_)) {}

void DeleteExecutor::Init() {
  if (child_executor_) {
    child_executor_->Init();
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  bool deleted = false;
  Tuple t;
  RID r;
  Transaction *txn = exec_ctx_->GetTransaction();

  if (child_executor_->Next(&t, &r)) {
    // update table
    Lock(r);
    deleted = table_info_->table_->MarkDelete(r, txn);
    Unlock(r);
  }

  if (deleted && !index_infos_.empty()) {
    // update index
    for (auto &it : index_infos_) {
      Tuple key = t.KeyFromTuple(table_info_->schema_, it->key_schema_, it->index_->GetKeyAttrs());
      it->index_->DeleteEntry(key, r, txn);

      // When rollback, txn_mgr insert t, does not use old_tuple
      IndexWriteRecord record(r, table_info_->oid_, WType::DELETE, t, Tuple{}, table_info_->oid_,
                              exec_ctx_->GetCatalog());
      txn->GetIndexWriteSet()->emplace_back(record);
    }
  }

  return deleted;
}

void DeleteExecutor::Lock(const RID &rid) {
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
void DeleteExecutor::Unlock(const RID &rid) {}

}  // namespace bustub
