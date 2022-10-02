/**
 * lock_manager_test.cpp
 */

#include <random>
#include <thread>  // NOLINT

#include "common/config.h"
#include "common/logger.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

namespace bustub {

/*
 * This test is only a sanity check. Please do not rely on this test
 * to check the correctness.
 */

// --- Helper functions ---
void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::GROWING); }

void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::SHRINKING); }

void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::ABORTED); }

void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED); }

void CheckTxnLockSize(Transaction *txn, size_t shared_size, size_t exclusive_size) {
  EXPECT_EQ(txn->GetSharedLockSet()->size(), shared_size);
  EXPECT_EQ(txn->GetExclusiveLockSet()->size(), exclusive_size);
}

// Basic shared lock test under REPEATABLE_READ
void BasicTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  std::vector<RID> rids;
  std::vector<Transaction *> txns;
  int num_rids = 10;
  for (int i = 0; i < num_rids; i++) {
    RID rid{i, static_cast<uint32_t>(i)};
    rids.push_back(rid);
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }
  // test

  auto task = [&](int txn_id) {
    bool res;
    for (const RID &rid : rids) {
      res = lock_mgr.LockShared(txns[txn_id], rid);
      EXPECT_TRUE(res);
      CheckGrowing(txns[txn_id]);
    }
    for (const RID &rid : rids) {
      res = lock_mgr.Unlock(txns[txn_id], rid);
      EXPECT_TRUE(res);
      CheckShrinking(txns[txn_id]);
    }
    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);
  };
  std::vector<std::thread> threads;
  threads.reserve(num_rids);

  for (int i = 0; i < num_rids; i++) {
    threads.emplace_back(std::thread{task, i});
  }

  for (int i = 0; i < num_rids; i++) {
    threads[i].join();
  }

  for (int i = 0; i < num_rids; i++) {
    delete txns[i];
  }
}
TEST(LockManagerTest, BasicTest) { BasicTest1(); }

void TwoPLTest() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid0{0, 0};
  RID rid1{0, 1};

  auto txn = txn_mgr.Begin();
  EXPECT_EQ(0, txn->GetTransactionId());

  bool res;
  res = lock_mgr.LockShared(txn, rid0);
  EXPECT_TRUE(res);
  CheckGrowing(txn);
  CheckTxnLockSize(txn, 1, 0);

  res = lock_mgr.LockExclusive(txn, rid1);
  EXPECT_TRUE(res);
  CheckGrowing(txn);
  CheckTxnLockSize(txn, 1, 1);

  res = lock_mgr.Unlock(txn, rid0);
  EXPECT_TRUE(res);
  CheckShrinking(txn);
  CheckTxnLockSize(txn, 0, 1);

  try {
    lock_mgr.LockShared(txn, rid0);
    CheckAborted(txn);
    // Size shouldn't change here
    CheckTxnLockSize(txn, 0, 1);
  } catch (TransactionAbortException &e) {
    // std::cout << e.GetInfo() << std::endl;
    CheckAborted(txn);
    // Size shouldn't change here
    CheckTxnLockSize(txn, 0, 1);
  }

  // Need to call txn_mgr's abort
  txn_mgr.Abort(txn);
  CheckAborted(txn);
  CheckTxnLockSize(txn, 0, 0);

  delete txn;
}
TEST(LockManagerTest, TwoPLTest) { TwoPLTest(); }

void UpgradeTest() {
  try {
    LockManager lock_mgr{};
    TransactionManager txn_mgr{&lock_mgr};
    RID rid{0, 0};
    Transaction txn(0);
    txn_mgr.Begin(&txn);

    bool res = lock_mgr.LockShared(&txn, rid);
    EXPECT_TRUE(res);
    CheckTxnLockSize(&txn, 1, 0);
    CheckGrowing(&txn);

    res = lock_mgr.LockUpgrade(&txn, rid);
    EXPECT_TRUE(res);
    CheckTxnLockSize(&txn, 0, 1);
    CheckGrowing(&txn);

    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_TRUE(res);
    CheckTxnLockSize(&txn, 0, 0);
    CheckShrinking(&txn);

    txn_mgr.Commit(&txn);
    CheckCommitted(&txn);
  } catch (TransactionAbortException &e) {
    std::cout << e.GetInfo() << std::endl;
  }
}
TEST(LockManagerTest, UpgradeLockTest) { UpgradeTest(); }

void WoundWaitBasicTest() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  int id_hold = 0;
  int id_die = 1;

  std::promise<void> t1done;
  std::shared_future<void> t1_future(t1done.get_future());

  auto wait_die_task = [&]() {
    // younger transaction acquires lock first
    Transaction txn_die(id_die);
    txn_mgr.Begin(&txn_die);
    bool res = lock_mgr.LockExclusive(&txn_die, rid);
    EXPECT_TRUE(res);

    CheckGrowing(&txn_die);
    CheckTxnLockSize(&txn_die, 0, 1);

    t1done.set_value();

    // wait for txn 0 to call lock_exclusive(), which should wound us
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    CheckAborted(&txn_die);

    // unlock
    txn_mgr.Abort(&txn_die);
  };

  Transaction txn_hold(id_hold);
  txn_mgr.Begin(&txn_hold);

  // launch the waiter thread
  std::thread wait_thread{wait_die_task};

  // wait for txn1 to lock
  t1_future.wait();

  bool res = lock_mgr.LockExclusive(&txn_hold, rid);
  EXPECT_TRUE(res);

  wait_thread.join();

  CheckGrowing(&txn_hold);
  txn_mgr.Commit(&txn_hold);
  CheckCommitted(&txn_hold);
}

void WoundUpgradeTest() {
  // txn1 Begin, txn2 Begin, txn3 Begin
  // txn2 S-lock()
  // txn3 S-lock()
  // txn3 upgrade() -> wait
  // txn1 X-lock()
  // check txn2, txn3 aborted

  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  int id1 = 1;
  int id2 = 2;
  int id3 = 3;

  Transaction txn1(id1), txn2(id2);

  auto upgrade_die_task = [&]() {
    Transaction txn3(id3);
    txn_mgr.Begin(&txn3);
    LOG_DEBUG("======== txn3 begins, with txn_id %d", txn1.GetTransactionId());

    LOG_DEBUG("======== txn3 Slock begins");
    bool res = lock_mgr.LockShared(&txn3, rid);
    EXPECT_TRUE(res);
    LOG_DEBUG("======== txn3 Slock ends");

    CheckGrowing(&txn3);
    CheckTxnLockSize(&txn3, 1, 0);

    LOG_DEBUG("======== txn3 upgrade begins and waits...");
    lock_mgr.LockUpgrade(&txn3, rid);  // waits.... the thread blocks here
    LOG_DEBUG("======== txn3 upgrade ends");

    LOG_DEBUG("======== txn3 check aborted");
    CheckAborted(&txn3);

    LOG_DEBUG("======== Aborting txn3");
    txn_mgr.Abort(&txn3);
    LOG_DEBUG("======== Aborting txn3 done");
  };

  txn_mgr.Begin(&txn1);
  LOG_DEBUG("======== txn1 begins, with txn_id %d", txn1.GetTransactionId());

  txn_mgr.Begin(&txn2);
  LOG_DEBUG("======== txn2 begins, with txn_id %d", txn1.GetTransactionId());

  LOG_DEBUG("======== txn2 Slock begins");
  bool r1 = lock_mgr.LockShared(&txn2, rid);
  EXPECT_TRUE(r1);
  LOG_DEBUG("======== txn2 Slock ends");

  std::thread upgrade_thread{upgrade_die_task};

  std::this_thread::sleep_for(std::chrono::microseconds(500));

  // now t2 and t3 holds S-lock and t3 is waiting to upgrading
  LOG_DEBUG("======== txn1 Xlock begins");
  bool r2 = lock_mgr.LockExclusive(&txn1, rid);
  EXPECT_TRUE(r2);
  LOG_DEBUG("======== txn1 Xlock ends");

  EXPECT_TRUE(txn2.GetState() == TransactionState::ABORTED);

  // now t2 and t3 are aborted
  upgrade_thread.join();

  LOG_DEBUG("======== checking abort txn2");
  CheckAborted(&txn2);
  LOG_DEBUG("======== aborting txn2");
  txn_mgr.Abort(&txn2);

  txn_mgr.Commit(&txn1);
  CheckCommitted(&txn1);
}

// TEST(LockManagerTest, WoundWaitBasicTest) { WoundUpgradeTest(); }

TEST(LockManagerTest, WoundUpgrade) { WoundUpgradeTest(); }

}  // namespace bustub
