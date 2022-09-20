//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * This is the key to put into the unordered_map
 * To put this key into unordered_map, we need to implement operator== and std::hash
 */
struct HashJoinKey {
  Value key_;

  auto operator==(const HashJoinKey &other) const -> bool { return key_.CompareEquals(other.key_) == CmpBool::CmpTrue; }
};

}  // namespace bustub

namespace std {

/** Implements std::hash on HashJoinKey */
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &key) const -> size_t { return bustub::HashUtil::HashValue(&key.key_); }
};

}  // namespace std

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() -> const Schema * override { return plan_->OutputSchema(); };

 private:
  /** Build the ht_ on initialization */
  void BuildHashTable();

  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;

  /** The hashtable, a key may correspond to a number of tuples */
  std::unordered_map<HashJoinKey, std::vector<Tuple>> ht_;

  /** The pointer on the tuples for current key (generated from inner_table.next.t) */
  size_t outer_ptr_;
  /** The checkpoint buffer of tuples we found for current key */
  std::vector<Tuple> outer_buf_;
  /** Current inner tuple */
  Tuple cur_inner_;
};

}  // namespace bustub
