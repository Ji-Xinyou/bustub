//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

/**
 * ======== FOR DIFFERENT LEVEL OF ISOLATION ========
 *  For READ_UNCOMMITTED:
 *    there is no S-lock, hold X-lock until the txn is over
 *  For READ_COMMITTED:
 *    when read, hold S-lock, but immediately unlock
 *  For REPEATABLE_READ:
 *    no index lock, others the same as SERIALIABLE
 *  For SERIALIABLE:
 *    holds all lock first, release on commit/abort(strict 2PL)
 * ==============================
 */

namespace bustub {

// basically, for all locking operation we follow the procedure
// 1. Prechecks
// 2. Setup the metadata, including request, fields
// 3. Wait for conditions
// 4. If deadlock(cycle), abort
//    If unblocked, grant the locks

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lk(latch_);

  // refuse READ_UNCOMMITTED, since there is no share lock at all, throw an error
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }

  TryAbortOnShrink(txn);
  TryInitLockQueue(rid);

  txn_table_[txn->GetTransactionId()] = txn;

  LockRequestQueue *q = &lock_table_.find(rid)->second;
  q->request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);

  if (q->is_exclusive_) {
    WoundWait(txn, q, true);
    q->cv_.wait(lk, [txn, q, this] { return SharedStopWait(txn, q); });
  }
  TryAbortOnDeadlock(txn, q);

  // now we can grant the S-lock
  txn->GetSharedLockSet()->emplace(rid);
  TxnToIter(txn, q)->granted_ = true;
  q->nsharing_++;

  BUSTUB_ASSERT(q->is_exclusive_ == false, "Expect no writer when a reader comes in");

  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lk(latch_);

  TryAbortOnShrink(txn);
  TryInitLockQueue(rid);

  txn_table_[txn->GetTransactionId()] = txn;

  LockRequestQueue *q = &lock_table_.find(rid)->second;
  q->request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);

  if (q->is_exclusive_ || q->nsharing_ != 0) {
    WoundWait(txn, q, false);
    q->cv_.wait(lk, [txn, q, this] { return ExclusiveStopWait(txn, q); });
  }
  TryAbortOnDeadlock(txn, q);

  // now we can grant the X-lock
  txn->GetExclusiveLockSet()->emplace(rid);
  TxnToIter(txn, q)->granted_ = true;
  q->is_exclusive_ = true;

  BUSTUB_ASSERT(q->nsharing_ == 0, "Expect no sharing when a writer comes in");

  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lk(latch_);

  TryAbortOnShrink(txn);

  LockRequestQueue *q = &lock_table_.find(rid)->second;
  if (q->upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  txn_table_[txn->GetTransactionId()] = txn;

  // emplace the request
  txn->GetSharedLockSet()->erase(rid);
  auto iter = TxnToIter(txn, q);
  BUSTUB_ASSERT(iter->txn_id_ == txn->GetTransactionId(), "transaction id must holds");
  BUSTUB_ASSERT(iter->lock_mode_ == LockMode::SHARED, "only shared can be upgraded");
  iter->granted_ = false;
  iter->lock_mode_ = LockMode::EXCLUSIVE;

  q->nsharing_--;
  q->upgrading_ = txn->GetTransactionId();

  if (q->is_exclusive_ || q->nsharing_ != 0) {
    WoundWait(txn, q, false);
    q->cv_.wait(lk, [this, txn, q] { return UpgradeStopWait(txn, q); });
  }
  TryAbortOnDeadlock(txn, q);

  // now we can grant the request
  txn->GetExclusiveLockSet()->emplace(rid);
  BUSTUB_ASSERT(q->is_exclusive_ == false, "Invariant: upgrade when no writer");
  BUSTUB_ASSERT(q->upgrading_ != INVALID_TXN_ID, "Invariant: upgrading set when upgrade");
  q->is_exclusive_ = true;
  q->upgrading_ = INVALID_TXN_ID;
  TxnToIter(txn, q)->granted_ = true;

  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lk(latch_);

  LockRequestQueue *q = &lock_table_.find(rid)->second;

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  // look for the txn's request inside the q
  auto iter = TxnToIter(txn, q);
  LockMode mode = iter->lock_mode_;
  BUSTUB_ASSERT(iter->granted_ == true, "Cannot unlock ungranted lock");
  BUSTUB_ASSERT(iter->txn_id_ == txn->GetTransactionId(), "txn id must be consistent");
  q->request_queue_.erase(iter);

  // If a shared lock is released in READ_COMMITED state, it means nothing
  // Other kinds of unlocking with cause a Growing 2PL txn changing to a Shrinking txn
  bool relse_slock_on_read_commited =
      (mode == LockMode::SHARED) && (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED);
  if ((txn->GetState() == TransactionState::GROWING) && !(relse_slock_on_read_commited)) {
    txn->SetState(TransactionState::SHRINKING);
  }

  // now the lock is logically unlocked, we can notify the threads waiting on the lock now
  if (txn->GetState() != TransactionState::ABORTED) {
    switch (mode) {
      case LockMode::SHARED: {
        BUSTUB_ASSERT(q->nsharing_ > 0, "Unlocking a shared lock with no sharing txn");
        BUSTUB_ASSERT(q->is_exclusive_ == false, "When unlocking shared lock, no writer");
        q->nsharing_--;
        if (q->nsharing_ == 0) {
          q->cv_.notify_all();
        }
        break;
      }

      case LockMode::EXCLUSIVE: {
        BUSTUB_ASSERT(q->nsharing_ == 0, "Unlocking a X-lock should have no reader");
        BUSTUB_ASSERT(q->is_exclusive_, "Unlocking X-lock without is_exclusive_ set");
        q->is_exclusive_ = false;
        q->cv_.notify_all();
        break;
      }

      default:
        BUSTUB_ASSERT(false, "UNREACHABLE");
    }
  }

  return true;
}

// ==============================
// ==============================
// |
// |        UTILITIES
// |
// ==============================
// ==============================

auto LockManager::TxnToIter(Transaction *txn, LockRequestQueue *q) -> std::list<LockRequest>::iterator {
  for (auto it = q->request_queue_.begin(); it != q->request_queue_.end(); it++) {
    if (it->txn_id_ == txn->GetTransactionId()) {
      return it;
    }
  }
  return q->request_queue_.end();
}

void LockManager::TryAbortOnShrink(Transaction *txn) {
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
}

// on deadlock, the deadlock prevention checks for cycles, and set victim transactions to abort state
// therefore, after the cond.wait(), the txn may be aborted due to deadlock, we check for this
void LockManager::TryAbortOnDeadlock(Transaction *txn, LockRequestQueue *q) {
  if (txn->GetState() == TransactionState::ABORTED) {
    auto iter = TxnToIter(txn, q);
    q->request_queue_.erase(iter);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
}

void LockManager::TryInitLockQueue(const RID &rid) {
  if (lock_table_.find(rid) == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }
}

// return true if can stop, false to keep waiting
auto LockManager::SharedStopWait(Transaction *txn, LockRequestQueue *q) -> bool {
  return (txn->GetState() == TransactionState::ABORTED) || (!q->is_exclusive_);
}

auto LockManager::ExclusiveStopWait(Transaction *txn, LockRequestQueue *q) -> bool {
  return (txn->GetState() == TransactionState::ABORTED) || ((!q->is_exclusive_) && (q->nsharing_ == 0));
}

auto LockManager::UpgradeStopWait(Transaction *txn, LockRequestQueue *q) -> bool {
  return (txn->GetState() == TransactionState::ABORTED) || ((!q->is_exclusive_) && (q->nsharing_ == 0));
}

//! CALL THIS FUNCTION WHEN YOU ARE SURE THAT THE TXN WILL BE BLOCKED
// For all the transactions in the [q], since we are going to wait on the lock
// If there exists transactions with a lower txn_id than [txn], set it to aborted
void LockManager::WoundWait(Transaction *txn, LockRequestQueue *q, bool shared) {
  bool notify = false;
  for (auto it = q->request_queue_.begin(); it != q->request_queue_.end(); it++) {
    if (it->granted_ && it->txn_id_ > txn->GetTransactionId()) {
      switch (it->lock_mode_) {
        case LockMode::SHARED: {
          if (!shared) {
            // an exclusive lock is waiting, abort the S-lock
            txn_table_[it->txn_id_]->SetState(TransactionState::ABORTED);
            q->nsharing_--;
          }
          break;
        }

        case LockMode::EXCLUSIVE: {
          // if a S-lock is waiting, this X-lock should abort
          // if an X-lock is waiting, this X-lock should also abort
          txn_table_[it->txn_id_]->SetState(TransactionState::ABORTED);
          q->is_exclusive_ = false;
          break;
        }

        default: BUSTUB_ASSERT(false, "UNREACHABLE");
      }

      // if this txn is upgrading
      if (it->txn_id_ == q->upgrading_) {
        q->upgrading_ = INVALID_TXN_ID;
      }
      notify = true;
    }
  }
  if (notify) {
    q->cv_.notify_all();
  }
}

}  // namespace bustub
