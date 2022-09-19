//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)),
      outer_ptr_(0) {
  left_executor_->Init();
  right_executor_->Init();
  BuildHashTable();
}

void HashJoinExecutor::BuildHashTable() {
  // traverse the left table, and hash each tuple, insert into ht_
  Tuple t;
  RID r;
  while (left_executor_->Next(&t, &r)) {
    Value k_val = plan_->LeftJoinKeyExpression()->Evaluate(&t, left_executor_->GetOutputSchema());
    HashJoinKey k{k_val};
    // build Value in hashtable, i.e. tuple
    std::vector<Value> values;
    for (const Column &col : plan_->GetLeftPlan()->OutputSchema()->GetColumns()) {
      values.emplace_back(col.GetExpr()->Evaluate(&t, left_executor_->GetOutputSchema()));
    }
    Tuple v = Tuple(values, left_executor_->GetOutputSchema());

    // insert into ht_
    if (ht_.count(k) == 0) {
      ht_.insert({k, {v}});
    } else {
      ht_[k].emplace_back(v);
    }
  }
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  outer_buf_.clear();
  outer_ptr_ = 0;
}

/**
 * First, iterate the right table, and hash each tuple to a key
 * Then, from the key, found the matched tuple(s) from the left table inside ht_
 * Finally, combine the right tuple and left tuple to form a joint tuple and emit
 *
 * Since each time only emits one tuple, we have to maintain a index on the `found tuples`
 * for outer table, so each time we are calling next, we know where we are
 * Also, we maintain the tuple set we found, as a checkpoint
 */
auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const Schema *left_schema = left_executor_->GetOutputSchema();
  const Schema *right_schema = right_executor_->GetOutputSchema();
  Tuple t;
  RID r;

  if (outer_ptr_ >= outer_buf_.size()) {
    // the outer_buf is empty now, we need to reacquire the outer_buf
    // if outer_ptr_ < size(), we just need to proceed and combine the tuple
    while (right_executor_->Next(&t, &r)) {
      Value rval = plan_->RightJoinKeyExpression()->Evaluate(&t, right_schema);
      HashJoinKey k{rval};
      auto it = ht_.find(k);
      if (it != ht_.end()) {
        outer_ptr_ = 0;
        outer_buf_ = it->second;
        goto out;
      }
    }
    return false;
  }

out:
  // combine tuples from outer_buf_, and right_tuple
  std::vector<Value> values;
  for (const Column &col : plan_->OutputSchema()->GetColumns()) {
    // TODO(bug): here, if use outer_ptr_++ will cause error, idk why
    values.emplace_back(col.GetExpr()->EvaluateJoin(&outer_buf_[outer_ptr_], left_schema, &t, right_schema));
  }
  outer_ptr_++;
  *tuple = Tuple(values, plan_->OutputSchema());
  *rid = r;  // rid is not used anyway

  return true;
}

}  // namespace bustub
