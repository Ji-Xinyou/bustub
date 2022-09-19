//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // build hash set
  child_executor_->Init();
  Tuple t;
  RID r;
  while (child_executor_->Next(&t, &r)) {
    std::vector<Value> values;
    for (size_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); ++i) {
      values.emplace_back(t.GetValue(child_executor_->GetOutputSchema(), i));
    }
    dhs_.emplace(DistinctKey{values});
  }
  dhs_iterator_ = dhs_.begin();
}

void DistinctExecutor::Init() {
  child_executor_->Init();
  dhs_iterator_ = dhs_.begin();  
}

auto DistinctExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (dhs_iterator_ == dhs_.end()) {
    return false;
  }

  *tuple = Tuple(dhs_iterator_->tuple_val_, plan_->OutputSchema());
  dhs_iterator_++;
  return true;
}

}  // namespace bustub
