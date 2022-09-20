//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/distinct_plan.h"

namespace bustub {

struct DistinctKey {
  /** The value of the tuple */
  std::vector<Value> tuple_val_;

  auto operator==(const DistinctKey &other) const -> bool {
    std::vector<Value> other_val = other.tuple_val_;
    if (other_val.size() != tuple_val_.size()) {
      return false;
    }
    for (size_t i = 0; i < tuple_val_.size(); ++i) {
      if (tuple_val_[i].CompareEquals(other_val[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace bustub

namespace std {

/** Implement std::hash on DistinctKey */
template <>
struct hash<bustub::DistinctKey> {
  auto operator()(const bustub::DistinctKey &distinct_key) const -> std::size_t {
    size_t cur_hash = 0;
    for (const auto &key : distinct_key.tuple_val_) {
      if (!key.IsNull()) {
        cur_hash = bustub::HashUtil::CombineHashes(cur_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return cur_hash;
  }
};

}  // namespace std

namespace bustub {

/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the distinct */
  auto GetOutputSchema() -> const Schema * override { return plan_->OutputSchema(); };

 private:
  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** The distinct hash set */
  std::unordered_set<DistinctKey> dhs_;
  /** The iterator of the hash set */
  std::unordered_set<DistinctKey>::iterator dhs_iterator_;
};
}  // namespace bustub
