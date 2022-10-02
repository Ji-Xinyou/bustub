//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
  enum class LockMode { SHARED, EXCLUSIVE };

  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}

    txn_id_t txn_id_;
    LockMode lock_mode_;
    bool granted_;
  };

  class LockRequestQueue {
   public:
    std::list<LockRequest> request_queue_;
    // for notifying blocked transactions on this rid
    std::condition_variable cv_{};
    // txn_id of an upgrading transaction (if any)
    txn_id_t upgrading_ = INVALID_TXN_ID;
    // the # of txn holding S-lock
    size_t nsharing_;
    // if there is a txn holding X-lock
    bool is_exclusive_;
  };

 public:
  /**
   * Creates a new lock manager configured for the deadlock prevention policy.
   */
  LockManager() = default;

  ~LockManager() = default;

  /*
   * [LOCK_NOTE]: For all locking functions, we:
   * 1. return false if the transaction is aborted; and
   * 2. block on wait, return true when the lock request is granted; and
   * 3. it is undefined behavior to try locking an already locked RID in the
   * same transaction, i.e. the transaction is responsible for keeping track of
   * its current locks.
   */

  /**
   * Acquire a lock on RID in shared mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the shared lock
   * @param rid the RID to be locked in shared mode
   * @return true if the lock is granted, false otherwise
   */
  auto LockShared(Transaction *txn, const RID &rid) -> bool;

  /**
   * Acquire a lock on RID in exclusive mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the exclusive lock
   * @param rid the RID to be locked in exclusive mode
   * @return true if the lock is granted, false otherwise
   */
  auto LockExclusive(Transaction *txn, const RID &rid) -> bool;

  /**
   * Upgrade a lock from a shared lock to an exclusive lock.
   * @param txn the transaction requesting the lock upgrade
   * @param rid the RID that should already be locked in shared mode by the
   * requesting transaction
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockUpgrade(Transaction *txn, const RID &rid) -> bool;

  /**
   * Release the lock held by the transaction.
   * @param txn the transaction releasing the lock, it should actually hold the
   * lock
   * @param rid the RID that is locked by the transaction
   * @return true if the unlock is successful, false otherwise
   */
  auto Unlock(Transaction *txn, const RID &rid) -> bool;

  /**
   * FOR SHARED LOCK
   * Returns whether the transaction can stop waiting on the queue
   *
   * @param txn the transaction waiting on the lock?
   * @param q the q that the rid(shipped with txn) corresponds
   * @return true if the txn can stop waiting, false otherwise
   */
  auto SharedStopWait(Transaction *txn, LockRequestQueue *q) -> bool;

  /**
   * FOR EXCLUSIVE LOCK
   * Returns whether the transaction can stop waiting on the queue
   * @param txn the transaction waiting on the lock?
   * @param q the q that the rid(shipped with txn) corresponds
   * @return true if the txn can stop waiting, false otherwise
   */
  auto ExclusiveStopWait(Transaction *txn, LockRequestQueue *q) -> bool;

  /**
   * FOR UPGRADING LOCK
   * Returns whether the transaction can stop waiting on the queue
   * @param txn the transaction waiting on the lock
   * @param q the q that the rid(shipped with txn) corresponds
   * @return true if txn can stop waiting, false otherwise
   */
  auto UpgradeStopWait(Transaction *txn, LockRequestQueue *q) -> bool;

  /**
   * Returns the iterator corresponds to the txn_id
   * @param txn the transaction to query
   * @param q the queue where request resides
   * @return the iterator of the txn's query, if not exist, return q.end()
   */
  auto TxnToIter(Transaction *txn, LockRequestQueue *q) -> std::list<LockRequest>::iterator;

  /**
   * Aborts and throw an error if the transaction is in shrink state
   * Also check
   * @param txn the transaction we are testing
   */
  void TryAbortOnShrink(Transaction *txn);

  /**
   * If the txn is set to aborted due to deadlock, we abort it and throw an error
   * @param txn the transaction we are testing
   * @param q the request queue corresponding to the RID(shipped w/ transaction in the parameter)
   */
  bool TryAbortOnDeadlock(Transaction *txn, LockRequestQueue *q);

  /**
   * Deadlock prevention, called only when you are sure that the transaction will be waiting
   * @param txn the transaction TRYING to hold the lock
   * @param q the request queue corresponding to the RID(shipped w/ transaction in the parameter)
   */
  void WoundWait(Transaction *txn, LockRequestQueue *q, LockMode into);

  /**
   * If the rid is the FIRST time on the lock_table_, we initialize it with an empty queue
   * If not the FIRST time, we do nothing
   *
   * @param txn
   * @param rid
   */
  void TryInitLockQueue(const RID &rid);

  std::mutex latch_;

  /** Lock table for lock requests, the queue is per-RID */
  std::unordered_map<RID, LockRequestQueue> lock_table_;
};

}  // namespace bustub
