//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.h
//
// Identification: src/include/execution/executors/insert_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/insert_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * InsertExecutor executes an insert on a table.
 *
 * Unlike UPDATE and DELETE, inserted values may either be
 * embedded in the plan itself or be pulled from a child executor.
 */
class InsertExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new InsertExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The insert plan to be executed
   * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`)
   */
  InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the insert */
  void Init() override;

  /**
   * Yield the next tuple from the insert.
   * @param[out] tuple The next tuple produced by the insert
   * @param[out] rid The next tuple RID produced by the insert
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   *
   * NOTE: InsertExecutor::Next() does not use the `tuple` out-parameter.
   * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
   */
  auto Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the insert */
  auto GetOutputSchema() -> const Schema * override { return plan_->OutputSchema(); };

  /** Lock a tuple */
  void Lock(const RID &rid);

  void Unlock(const RID &rid);

 private:
  /** The insert plan node to be executed*/
  const InsertPlanNode *plan_;
  /** The table info */
  const TableInfo *table_info_;
  /** The index info array, need to be updated for every valid insertion */
  const std::vector<IndexInfo *> index_infos_;
  /** If the insert data is from a child executor, use this */
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** The pos pointer of raw_data from the plan */
  size_t pos_;
};

}  // namespace bustub
