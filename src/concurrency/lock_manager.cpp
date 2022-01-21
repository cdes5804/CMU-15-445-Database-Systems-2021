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

namespace bustub {

void LockManager::AbortTransaction(Transaction *txn, const AbortReason reason) {
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), reason);
}

void LockManager::WoundTransactions(const std::vector<txn_id_t> &transaction_ids) {
  for (txn_id_t transaction_id : transaction_ids) {
    auto txn = TransactionManager::GetTransaction(transaction_id);
    txn->SetState(TransactionState::ABORTED);
  }
}

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING) {
    AbortTransaction(txn, AbortReason::LOCK_ON_SHRINKING);
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    AbortTransaction(txn, AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }

  latch_.lock();
  LockRequestQueue &request_queue = lock_table_[rid];
  latch_.unlock();

  std::unique_lock queue_lock(request_queue.queue_latch_);
  request_queue.SubmitLockRequest(txn, LockMode::SHARED);

  while (txn->GetState() != TransactionState::ABORTED && !request_queue.IsLockGranted(txn->GetTransactionId())) {
    request_queue.cv_.wait(queue_lock);
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    AbortTransaction(txn, AbortReason::DEADLOCK);
    return false;
  }

  WoundTransactions(request_queue.GetAbortedTransactions());
  txn->GetSharedLockSet()->emplace(rid);

  request_queue.aborted_transaction_ids_.clear();
  request_queue.cv_.notify_all();

  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::SHRINKING) {
    AbortTransaction(txn, AbortReason::LOCK_ON_SHRINKING);
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  if (txn->IsSharedLocked(rid)) {
    return LockUpgrade(txn, rid);
  }

  latch_.lock();
  LockRequestQueue &request_queue = lock_table_[rid];
  latch_.unlock();

  std::unique_lock queue_lock(request_queue.queue_latch_);
  request_queue.SubmitLockRequest(txn, LockMode::EXCLUSIVE);

  while (txn->GetState() != TransactionState::ABORTED && !request_queue.IsLockGranted(txn->GetTransactionId())) {
    request_queue.cv_.wait(queue_lock);
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    AbortTransaction(txn, AbortReason::DEADLOCK);
    return false;
  }

  WoundTransactions(request_queue.GetAbortedTransactions());
  txn->GetExclusiveLockSet()->emplace(rid);

  request_queue.aborted_transaction_ids_.clear();
  request_queue.cv_.notify_all();

  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::SHRINKING) {
    AbortTransaction(txn, AbortReason::LOCK_ON_SHRINKING);
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  if (!txn->IsSharedLocked(rid)) {
    return false;
  }

  latch_.lock();
  LockRequestQueue &request_queue = lock_table_[rid];
  latch_.unlock();

  std::unique_lock queue_lock(request_queue.queue_latch_);
  if (!request_queue.SubmitUpgradeRequest(txn)) {
    AbortTransaction(txn, AbortReason::UPGRADE_CONFLICT);
  }

  while (txn->GetState() != TransactionState::ABORTED && !request_queue.IsLockGranted(txn->GetTransactionId())) {
    request_queue.cv_.wait(queue_lock);
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    AbortTransaction(txn, AbortReason::DEADLOCK);
    return false;
  }

  WoundTransactions(request_queue.GetAbortedTransactions());
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);

  request_queue.aborted_transaction_ids_.clear();
  request_queue.cv_.notify_all();

  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  latch_.lock();
  LockRequestQueue &request_queue = lock_table_[rid];
  latch_.unlock();

  std::unique_lock queue_lock(request_queue.queue_latch_);
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  request_queue.Unlock(txn->GetTransactionId());

  WoundTransactions(request_queue.GetAbortedTransactions());
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  request_queue.aborted_transaction_ids_.clear();
  request_queue.cv_.notify_all();

  return true;
}

}  // namespace bustub
