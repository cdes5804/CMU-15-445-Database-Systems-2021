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

void LockManager::AbortCurrentTransaction(Transaction *txn) { txn->SetState(TransactionState::ABORTED); }

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING) {
    AbortCurrentTransaction(txn);
    return false;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    AbortCurrentTransaction(txn);
    return false;
  }
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }

  std::unique_lock manager_lock(latch_);
  LockRequestQueue &request_queue = lock_table_[rid];

  auto queue_iter = request_queue.request_queue_.begin();
  while (queue_iter != request_queue.request_queue_.end()) {
    if (queue_iter->lock_mode_ == LockMode::EXCLUSIVE && txn->GetTransactionId() < queue_iter->txn_id_) {
      if (queue_iter->txn_id_ == request_queue.upgrading_) {
        request_queue.upgrading_ = INVALID_TXN_ID;
      }
      Transaction *queued_transaction = TransactionManager::GetTransaction(queue_iter->txn_id_);
      queued_transaction->SetState(TransactionState::ABORTED);
      queue_iter = request_queue.request_queue_.erase(queue_iter);
    } else {
      queue_iter++;
    }
  }

  if (request_queue.exclusive_lock_holder_id_ != INVALID_TXN_ID &&
      txn->GetTransactionId() < request_queue.exclusive_lock_holder_id_) {
    Transaction *exclusive_lock_holder_transaction =
        TransactionManager::GetTransaction(request_queue.exclusive_lock_holder_id_);
    exclusive_lock_holder_transaction->SetState(TransactionState::ABORTED);
    request_queue.exclusive_lock_holder_id_ = INVALID_TXN_ID;
  }

  if (request_queue.exclusive_lock_holder_id_ == INVALID_TXN_ID) {
    request_queue.shared_lock_holders_.insert(txn->GetTransactionId());
  } else {
    request_queue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);
    ProcessQueue(&request_queue);
    while (txn->GetState() != TransactionState::ABORTED && !request_queue.IsLockGranted(txn->GetTransactionId())) {
      request_queue.cv_.wait(manager_lock);
    }
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    AbortCurrentTransaction(txn);
    return false;
  }

  txn->GetSharedLockSet()->emplace(rid);
  request_queue.cv_.notify_all();

  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    AbortCurrentTransaction(txn);
    return false;
  }
  if (txn->IsSharedLocked(rid)) {
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  std::unique_lock manager_lock(latch_);
  LockRequestQueue &request_queue = lock_table_[rid];

  auto queue_iter = request_queue.request_queue_.begin();
  while (queue_iter != request_queue.request_queue_.end()) {
    if (txn->GetTransactionId() < queue_iter->txn_id_) {
      if (queue_iter->lock_mode_ == LockMode::EXCLUSIVE && queue_iter->txn_id_ == request_queue.upgrading_) {
        request_queue.upgrading_ = INVALID_TXN_ID;
      }
      Transaction *queued_transaction = TransactionManager::GetTransaction(queue_iter->txn_id_);
      queued_transaction->SetState(TransactionState::ABORTED);
      queue_iter = request_queue.request_queue_.erase(queue_iter);
    } else {
      queue_iter++;
    }
  }

  if (request_queue.exclusive_lock_holder_id_ != INVALID_TXN_ID &&
      txn->GetTransactionId() < request_queue.exclusive_lock_holder_id_) {
    Transaction *exclusive_lock_holder_transaction =
        TransactionManager::GetTransaction(request_queue.exclusive_lock_holder_id_);
    exclusive_lock_holder_transaction->SetState(TransactionState::ABORTED);
    request_queue.exclusive_lock_holder_id_ = INVALID_TXN_ID;
  }

  ExclusiveLockPreemptsSharedLock(&request_queue, txn->GetTransactionId());

  if (request_queue.exclusive_lock_holder_id_ == INVALID_TXN_ID && request_queue.shared_lock_holders_.empty()) {
    request_queue.exclusive_lock_holder_id_ = txn->GetTransactionId();
  } else {
    request_queue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
    ProcessQueue(&request_queue);
    while (txn->GetState() != TransactionState::ABORTED && !request_queue.IsLockGranted(txn->GetTransactionId())) {
      request_queue.cv_.wait(manager_lock);
    }
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    AbortCurrentTransaction(txn);
    return false;
  }

  txn->GetExclusiveLockSet()->emplace(rid);
  request_queue.cv_.notify_all();

  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    AbortCurrentTransaction(txn);
    return false;
  }
  if (!txn->IsSharedLocked(rid)) {
    return false;
  }

  std::unique_lock manager_lock(latch_);
  LockRequestQueue &request_queue = lock_table_[rid];

  if (request_queue.upgrading_ != INVALID_TXN_ID) {
    // another transaction is trying to upgrade, abort the current one.
    AbortCurrentTransaction(txn);
    return false;
  }

  request_queue.shared_lock_holders_.erase(txn->GetTransactionId());

  auto queue_iter = request_queue.request_queue_.begin();
  while (queue_iter != request_queue.request_queue_.end()) {
    if (txn->GetTransactionId() < queue_iter->txn_id_) {
      Transaction *queued_transaction = TransactionManager::GetTransaction(queue_iter->txn_id_);
      queued_transaction->SetState(TransactionState::ABORTED);
      queue_iter = request_queue.request_queue_.erase(queue_iter);
    } else {
      queue_iter++;
    }
  }

  if (request_queue.exclusive_lock_holder_id_ != INVALID_TXN_ID &&
      txn->GetTransactionId() < request_queue.exclusive_lock_holder_id_) {
    Transaction *exclusive_lock_holder_transaction =
        TransactionManager::GetTransaction(request_queue.exclusive_lock_holder_id_);
    exclusive_lock_holder_transaction->SetState(TransactionState::ABORTED);
    request_queue.exclusive_lock_holder_id_ = INVALID_TXN_ID;
  }

  ExclusiveLockPreemptsSharedLock(&request_queue, txn->GetTransactionId());

  if (request_queue.exclusive_lock_holder_id_ == INVALID_TXN_ID && request_queue.shared_lock_holders_.empty()) {
    request_queue.exclusive_lock_holder_id_ = txn->GetTransactionId();
  } else {
    request_queue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
    request_queue.upgrading_ = txn->GetTransactionId();
    ProcessQueue(&request_queue);
    while (txn->GetState() != TransactionState::ABORTED && !request_queue.IsLockGranted(txn->GetTransactionId())) {
      request_queue.cv_.wait(manager_lock);
    }
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    AbortCurrentTransaction(txn);
    return false;
  }

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  request_queue.cv_.notify_all();

  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock manager_lock(latch_);
  LockRequestQueue &request_queue = lock_table_[rid];

  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  bool item_is_unlocked = false;

  if (request_queue.exclusive_lock_holder_id_ == txn->GetTransactionId()) {
    request_queue.exclusive_lock_holder_id_ = INVALID_TXN_ID;
    item_is_unlocked = true;
  }
  if (auto shared_lock_iter = request_queue.shared_lock_holders_.find(txn->GetTransactionId());
      shared_lock_iter != request_queue.shared_lock_holders_.end()) {
    request_queue.shared_lock_holders_.erase(shared_lock_iter);
    item_is_unlocked = true;
  }

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  ProcessQueue(&request_queue);
  request_queue.cv_.notify_all();

  return item_is_unlocked;
}

void LockManager::ProcessQueue(LockRequestQueue *request_queue) {
  auto queue_iter = request_queue->request_queue_.begin();
  while (queue_iter != request_queue->request_queue_.end()) {
    if (queue_iter->lock_mode_ == LockMode::SHARED) {
      if (request_queue->exclusive_lock_holder_id_ == INVALID_TXN_ID) {
        request_queue->shared_lock_holders_.insert(queue_iter->txn_id_);
        queue_iter = request_queue->request_queue_.erase(queue_iter);
      } else {
        break;
      }
    } else {
      ExclusiveLockPreemptsSharedLock(request_queue, queue_iter->txn_id_);
      if (request_queue->shared_lock_holders_.empty()) {
        request_queue->exclusive_lock_holder_id_ = queue_iter->txn_id_;
        if (queue_iter->txn_id_ == request_queue->upgrading_) {
          request_queue->upgrading_ = INVALID_TXN_ID;
        }
        request_queue->request_queue_.erase(queue_iter);
      }
      break;
    }
  }
}

void LockManager::ExclusiveLockPreemptsSharedLock(LockRequestQueue *request_queue,
                                                  const txn_id_t exclusive_lock_requester_id) {
  auto shared_lock_iter = request_queue->shared_lock_holders_.begin();
  while (shared_lock_iter != request_queue->shared_lock_holders_.end()) {
    if (exclusive_lock_requester_id < *shared_lock_iter) {
      Transaction *shared_lock_holder_transaction = TransactionManager::GetTransaction(*shared_lock_iter);
      shared_lock_holder_transaction->SetState(TransactionState::ABORTED);
      shared_lock_iter = request_queue->shared_lock_holders_.erase(shared_lock_iter);
    } else {
      shared_lock_iter++;
    }
  }
}

}  // namespace bustub
