//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "include/execution/expressions/column_value_expression.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  predicate_ = plan->Predicate();
}

void NestedLoopJoinExecutor::Init() {
  if (left_executor_) {
    left_executor_->Init();
  }
  if (right_executor_) {
    right_executor_->Init();
  }
  // The initial point we start
  left_done_ = !left_executor_->Next(left_tuple_, left_rid_);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
begin:
  Tuple *right_tuple = nullptr;
  RID *right_rid = nullptr;
  // left table is traversed through, return false
  if (left_done_) {
    return false;
  }

  // step on right, if right is traversed through, correspond
  if (!right_executor_->Next(right_tuple, right_rid)) {
    // right table is traversed through, fresh right and step left
    right_executor_->Init();
    left_done_ = !left_executor_->Next(left_tuple_, left_rid_);
    if (left_done_) {
      return false;
    }
  }

  // now we are sure we have left tuple and right tuple, we join and evaluate them
  if (predicate_) {
    // use predicate to evaluate them
    Value eval = predicate_->EvaluateJoin(left_tuple_, plan_->GetLeftPlan()->OutputSchema(), right_tuple,
                                          plan_->GetRightPlan()->OutputSchema());
    if (!eval.GetAs<bool>()) {
      // not qualified, go to begin
      goto begin;
    }
  }

  // now rather the predicate is qualified, or there is no predicate
  std::vector<Value> values;
  for (const Column &col : plan_->OutputSchema()->GetColumns()) {
    // output schema's expr, maintains the tuple index and column index
    auto col_val_expr = reinterpret_cast<const ColumnValueExpression *>(col.GetExpr());
    if (col_val_expr->GetTupleIdx() == 0) {  // lhs
      values.emplace_back(left_tuple_->GetValue(plan_->GetLeftPlan()->OutputSchema(), col_val_expr->GetColIdx()));
    } else {  // rhs
      values.emplace_back(right_tuple->GetValue(plan_->GetRightPlan()->OutputSchema(), col_val_expr->GetColIdx()));
    }
  }
  *tuple = Tuple(values, plan_->OutputSchema());
  return true;
  // FIXME: rid untouched?
}

}  // namespace bustub
