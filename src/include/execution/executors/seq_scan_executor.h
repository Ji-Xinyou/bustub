//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  /** Initialize the sequential scan */
  void Init() override;

  /**
   * Yield the next tuple from the sequential scan.
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sequential scan */
  auto GetOutputSchema() -> const Schema * override { return plan_->OutputSchema(); }

  /** Holds the share lock on a tuple */
  void Lock(const RID &rid);

  /** Unlock the share lock on a tuple */
  void Unlock(const RID &rid);

 private:
  /** The sequential scan plan node to be executed */
  const SeqScanPlanNode *plan_;
  /** The table information in the catalog */
  const TableInfo *table_info_;
  /** The predicate of this query plan */
  const AbstractExpression *predicate_;
  /** The current iterator position, continued from this when come back */
  TableIterator current_;
  /** The end of table's iterator, indicating the table is fully traversed */
  TableIterator end_;
};

}  // namespace bustub
