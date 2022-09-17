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

  // insert to table
  if (plan_->IsRawInsert()) {
    if (pos_ < plan_->RawValues().size()) {  // valid position
      std::vector<Value> raw_value = plan_->RawValues()[pos_++];
      t = Tuple(raw_value, &table_info_->schema_);
      inserted = table_info_->table_->InsertTuple(t, &r, exec_ctx_->GetTransaction());
    }
  } else {
    if (child_executor_->Next(&t, &r)) {
      inserted = table_info_->table_->InsertTuple(t, &r, exec_ctx_->GetTransaction());
    }
  }

  // update index
  if (inserted && !index_infos_.empty()) {
    for (auto &it : index_infos_) {
      Tuple key;
      key = t.KeyFromTuple(table_info_->schema_, it->key_schema_, it->index_->GetKeyAttrs());
      it->index_->InsertEntry(key, r, exec_ctx_->GetTransaction());
    }
  }

  return inserted;
}

}  // namespace bustub
