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
#include "concurrency/transaction_manager.h"

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
  LOG_DEBUG("txn %d LockShared(), on RID %s", txn->GetTransactionId(), rid.ToString().c_str());

  // refuse READ_UNCOMMITTED, since there is no share lock at all, throw an error
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  TryAbortOnShrink(txn);
  TryInitLockQueue(rid);
  txn->GetSharedLockSet()->emplace(rid);

  LockRequestQueue *q = &lock_table_[rid];
  q->request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);

  if (q->is_exclusive_) {
    LOG_DEBUG("txn %d try to wait", txn->GetTransactionId());
    WoundWait(txn, q, LockMode::SHARED);
    q->cv_.wait(lk, [txn, q, this] { return SharedStopWait(txn, q); });
  }
  if (!TryAbortOnDeadlock(txn, q)) {
    LOG_DEBUG("txn %d aborted due to deadlock", txn->GetTransactionId());
    return false;
  }

  // now we can grant the S-lock
  LOG_DEBUG("txn %d granted S-lock on rid %s", txn->GetTransactionId(), rid.ToString().c_str());
  TxnToIter(txn, q)->granted_ = true;
  q->nsharing_++;

  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lk(latch_);
  LOG_DEBUG("txn %d LockExclusive(), on RID %s", txn->GetTransactionId(), rid.ToString().c_str());

  TryAbortOnShrink(txn);
  TryInitLockQueue(rid);
  txn->GetExclusiveLockSet()->emplace(rid);

  LockRequestQueue *q = &lock_table_[rid];
  q->request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);

  if (q->is_exclusive_ || q->nsharing_ != 0) {
    LOG_DEBUG("txn %d try to wait", txn->GetTransactionId());
    WoundWait(txn, q, LockMode::EXCLUSIVE);
    q->cv_.wait(lk, [txn, q, this] { return ExclusiveStopWait(txn, q); });
  }

  if (!TryAbortOnDeadlock(txn, q)) {
    LOG_DEBUG("txn %d aborted due to deadlock", txn->GetTransactionId());
    return false;
  }

  // now we can grant the X-lock
  LOG_DEBUG("txn %d granted X-lock on rid %s", txn->GetTransactionId(), rid.ToString().c_str());
  TxnToIter(txn, q)->granted_ = true;
  q->is_exclusive_ = true;

  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lk(latch_);
  LOG_DEBUG("txn %d LockUpgrade on rid %s", txn->GetTransactionId(), rid.ToString().c_str());

  TryAbortOnShrink(txn);
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (lock_table_.find(rid) == lock_table_.end()) {
    return false;
  }

  LockRequestQueue *q = &lock_table_[rid];
  if (q->upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
  auto iter = TxnToIter(txn, q);
  if (iter == q->request_queue_.end() || !iter->granted_ || iter->lock_mode_ != LockMode::SHARED) {
    return false;
  }

  iter->granted_ = false;
  iter->lock_mode_ = LockMode::EXCLUSIVE;
  q->nsharing_--;
  q->upgrading_ = txn->GetTransactionId();

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);

  if (q->is_exclusive_ || q->nsharing_ != 0) {
    LOG_DEBUG("txn %d try to wait", txn->GetTransactionId());
    WoundWait(txn, q, LockMode::EXCLUSIVE);
    q->cv_.wait(lk, [this, txn, q] { return UpgradeStopWait(txn, q); });
  }

  if (!TryAbortOnDeadlock(txn, q)) {
    LOG_DEBUG("txn %d aborted due to deadlock", txn->GetTransactionId());
    return false;
  }

  // now we can grant the request
  LOG_DEBUG("txn %d upgraded the lock on rid %s", txn->GetTransactionId(), rid.ToString().c_str());
  q->is_exclusive_ = true;
  q->upgrading_ = INVALID_TXN_ID;
  TxnToIter(txn, q)->granted_ = true;

  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lk(latch_);
  LOG_DEBUG("txn %d Unlock() on %s", txn->GetTransactionId(), rid.ToString().c_str());

  LockRequestQueue *q = &lock_table_[rid];

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  // look for the txn's request inside the q
  auto iter = TxnToIter(txn, q);
  if (iter == q->request_queue_.end()) {
    LOG_DEBUG("return false");
    return false;
  }
  LockMode mode = iter->lock_mode_;
  q->request_queue_.erase(iter);

  // If a shared lock is released in READ_COMMITED state, it means nothing
  // Other kinds of unlocking with cause a Growing 2PL txn changing to a Shrinking txn
  bool relse_slock_on_read_commited =
      (mode == LockMode::SHARED) && (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED);
  if ((txn->GetState() == TransactionState::GROWING) && !(relse_slock_on_read_commited)) {
    LOG_DEBUG("txn %d GROWING -> SHRINKING", txn->GetTransactionId());
    txn->SetState(TransactionState::SHRINKING);
  }

  // now the lock is logically unlocked, we can notify the threads waiting on the lock now
  if (!Wounded(txn)) {
    if (mode == LockMode::SHARED) {
      q->nsharing_--;
      if (q->nsharing_ == 0) {
        q->cv_.notify_all();
      }
    } else {
      LOG_DEBUG("txn %d unlock on %s, cause is_exclusive to false", txn->GetTransactionId(), rid.ToString().c_str());
      q->is_exclusive_ = false;
      q->cv_.notify_all();
    }
  }
  LOG_DEBUG("txn %d Unlock() Done on %s", txn->GetTransactionId(), rid.ToString().c_str());
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
bool LockManager::TryAbortOnDeadlock(Transaction *txn, LockRequestQueue *q) {
  if (txn->GetState() == TransactionState::ABORTED) {
    auto iter = TxnToIter(txn, q);
    iter->granted_ = false;
    q->cv_.notify_all();
    return false;
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
  return true;
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
  LOG_DEBUG("q->is_exclusive_ = %d, q->n_sharing_ = %zu", q->is_exclusive_, q->nsharing_);
  return (txn->GetState() == TransactionState::ABORTED) || ((!q->is_exclusive_) && (q->nsharing_ == 0));
}

auto LockManager::UpgradeStopWait(Transaction *txn, LockRequestQueue *q) -> bool {
  return (txn->GetState() == TransactionState::ABORTED) || ((!q->is_exclusive_) && (q->nsharing_ == 0));
}

void LockManager::WoundWait(Transaction *txn, LockRequestQueue *q, LockMode into) {
  for (auto it = q->request_queue_.begin(); it != q->request_queue_.end(); it++) {
    if (it->txn_id_ > txn->GetTransactionId()) {
      // if the one holding an X-lock, it has to be aborted
      //   - but if it is upgrading, we cannot set is_exclusive to false，since it is not granted
      // if the one is S-lock, if into is X-lock, it has to be aborted
      // if the aborted is the upgrading one, abort that

      if (it->lock_mode_ == LockMode::EXCLUSIVE && it->txn_id_ != q->upgrading_) {
        LOG_DEBUG("aborting txn %d, setting is_exclusive to false", it->txn_id_);
        if (it->granted_) {
          q->is_exclusive_ = false;
        }
        it->granted_ = false;
        TransactionManager::GetTransaction(it->txn_id_)->SetState(TransactionState::ABORTED);
        WoundTransaction(txn);
        q->cv_.notify_all();
      } else if (it->lock_mode_ == LockMode::SHARED) {  // S-lock
        if (into == LockMode::EXCLUSIVE) {              // X-lock want to holds, meeting S-lock
          LOG_DEBUG("aborting txn %d, n_sharing: %zu -> %zu", it->txn_id_, q->nsharing_, q->nsharing_ - 1);
          if (it->granted_) {
            q->nsharing_--;
          }
          it->granted_ = false;
          TransactionManager::GetTransaction(it->txn_id_)->SetState(TransactionState::ABORTED);
          WoundTransaction(txn);
          q->cv_.notify_all();
        }
      }
      if (q->upgrading_ == it->txn_id_) {
        LOG_DEBUG("aborting txn %d, upgrade aborted", it->txn_id_);
        q->upgrading_ = INVALID_TXN_ID;
        TransactionManager::GetTransaction(it->txn_id_)->SetState(TransactionState::ABORTED);
        WoundTransaction(txn);
        q->cv_.notify_all();
      }
    }
  }
}

void LockManager::WoundTransaction(Transaction *txn) {
  if (std::find(wounded_txns_.begin(), wounded_txns_.end(), txn->GetTransactionId()) == wounded_txns_.end()) {
    wounded_txns_.emplace_back(txn->GetTransactionId());
  }
}

bool LockManager::Wounded(Transaction *txn) {
  return (std::find(wounded_txns_.begin(), wounded_txns_.end(), txn->GetTransactionId()) != wounded_txns_.end());
}

}  // namespace bustub
