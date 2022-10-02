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
#include "concurrency/transaction.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())),
      predicate_(plan->GetPredicate()),
      current_(nullptr, RID{}, nullptr),
      end_(nullptr, RID{}, nullptr) {}

void SeqScanExecutor::Init() {
  current_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
  end_ = table_info_->table_->End();
}

/**
 * return true if a tuple is emitted, false if no more tuple
 */
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (current_ != end_) {
    TableIterator iter = current_++;
    *rid = iter->GetRid();
    Lock(*rid);
    *tuple = *iter;

    if (predicate_ == nullptr || predicate_->Evaluate(&(*tuple), &table_info_->schema_).GetAs<bool>()) {
      // generate tuple from output schema
      std::vector<Value> values;
      for (const auto &col : plan_->OutputSchema()->GetColumns()) {
        values.push_back(col.GetExpr()->Evaluate(tuple, &table_info_->schema_));
      }
      *tuple = Tuple(values, plan_->OutputSchema());
      Unlock(*rid);
      return true;
    }
    Unlock(*rid);
  }
  return false;
}

// SeqScan is RDONLY, only requires a S-lock
void SeqScanExecutor::Lock(const RID &rid) {
  Transaction *txn = exec_ctx_->GetTransaction();
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    return;
  }
  if (txn->IsSharedLocked(rid)) {
    return;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return;
  }
  exec_ctx_->GetLockManager()->LockShared(txn, rid);
}

void SeqScanExecutor::Unlock(const RID &rid) {
  Transaction *txn = exec_ctx_->GetTransaction();
  // READ_UNCOMMITED: No Shared Lock
  // READ_COMMITED: Shared Lock unlocked manually
  // REPEATABLE_READ && SERIALIZABLE: Unlocked on commit/abort
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    exec_ctx_->GetLockManager()->Unlock(txn, rid);
  }
}

}  // namespace bustub
