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
  if (child_executor_->Next(&t, &r)) {
    // update table
    deleted = table_info_->table_->MarkDelete(r, exec_ctx_->GetTransaction());
  }

  if (deleted && !index_infos_.empty()) {
    // update index
    for (auto &it : index_infos_) {
      Tuple key = t.KeyFromTuple(table_info_->schema_, it->key_schema_, it->index_->GetKeyAttrs());
      it->index_->DeleteEntry(key, r, exec_ctx_->GetTransaction());
    }
  }

  return deleted;
}

}  // namespace bustub
