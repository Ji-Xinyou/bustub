//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_plan.h
//
// Identification: src/include/execution/plans/distinct_plan.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/util/hash_util.h"
#include "execution/plans/abstract_plan.h"

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

/**
 * Distinct removes duplicate rows from the output of a child node.
 */
class DistinctPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new DistinctPlanNode instance.
   * @param child The child plan from which tuples are obtained
   */
  DistinctPlanNode(const Schema *output_schema, const AbstractPlanNode *child)
      : AbstractPlanNode(output_schema, {child}) {}

  /** @return The type of the plan node */
  auto GetType() const -> PlanType override { return PlanType::Distinct; }

  /** @return The child plan node */
  auto GetChildPlan() const -> const AbstractPlanNode * {
    BUSTUB_ASSERT(GetChildren().size() == 1, "Distinct should have at most one child plan.");
    return GetChildAt(0);
  }
};

}  // namespace bustub

namespace std {

/** Implement std::hash on DistinctKey */
template<>
struct hash<bustub::DistinctKey> {
  auto operator()(const bustub::DistinctKey &distinct_key) const -> std::size_t {
    size_t cur_hash = 0;
    for (const auto &key: distinct_key.tuple_val_) {
      if (!key.IsNull()) {
        cur_hash = bustub::HashUtil::CombineHashes(cur_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return cur_hash;
  }
};

}
