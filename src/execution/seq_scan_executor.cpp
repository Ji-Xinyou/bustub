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
    if (!predicate_ || predicate_->Evaluate(&(*iter), &table_info_->schema_).GetAs<bool>()) {
      // generate tuple from output schema
      std::vector<Value> values;
      *tuple = *iter;
      for (const auto &col: plan_->OutputSchema()->GetColumns()) {
        values.push_back(col.GetExpr()->Evaluate(tuple, &table_info_->schema_));
      }
      *tuple = Tuple(values, plan_->OutputSchema());
      *rid = iter->GetRid();
      return true;
    }
  }
  return false;
}

}  // namespace bustub
