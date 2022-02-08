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

  bool should_wait = false;
  auto queue_iter = request_queue.request_queue_.begin();
  while (queue_iter != request_queue.request_queue_.end()) {
    if (txn->GetTransactionId() < queue_iter->txn_id_ && queue_iter->lock_mode_ == LockMode::EXCLUSIVE) {
      Transaction *queued_transaction = TransactionManager::GetTransaction(queue_iter->txn_id_);
      queued_transaction->SetState(TransactionState::ABORTED);
      queue_iter = request_queue.request_queue_.erase(queue_iter);
    } else {
      if (queue_iter->lock_mode_ == LockMode::EXCLUSIVE) {
        should_wait = true;
      }
      queue_iter++;
    }
  }

  if (request_queue.upgrading_ != INVALID_TXN_ID) {
    if (txn->GetTransactionId() < request_queue.upgrading_) {
      Transaction *upgrading_transaction = TransactionManager::GetTransaction(request_queue.upgrading_);
      upgrading_transaction->SetState(TransactionState::ABORTED);
      request_queue.upgrading_ = INVALID_TXN_ID;
    } else {
      should_wait = true;
    }
  }

  if (request_queue.exclusive_lock_holder_id_ != INVALID_TXN_ID) {
    if (txn->GetTransactionId() < request_queue.exclusive_lock_holder_id_) {
      Transaction *exclusive_lock_holder_transaction =
          TransactionManager::GetTransaction(request_queue.exclusive_lock_holder_id_);
      exclusive_lock_holder_transaction->SetState(TransactionState::ABORTED);
      request_queue.exclusive_lock_holder_id_ = INVALID_TXN_ID;
    } else {
      should_wait = true;
    }
  }

  if (should_wait) {
    request_queue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);
    while (txn->GetState() != TransactionState::ABORTED && !request_queue.IsLockGranted(txn->GetTransactionId())) {
      request_queue.cv_.wait(manager_lock);
    }
  } else {
    request_queue.shared_lock_holders_.insert(txn->GetTransactionId());
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
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  if (txn->IsSharedLocked(rid)) {
    return LockUpgrade(txn, rid);
  }

  std::unique_lock manager_lock(latch_);
  LockRequestQueue &request_queue = lock_table_[rid];

  bool should_wait = false;
  auto queue_iter = request_queue.request_queue_.begin();
  while (queue_iter != request_queue.request_queue_.end()) {
    if (txn->GetTransactionId() < queue_iter->txn_id_) {
      Transaction *queued_transaction = TransactionManager::GetTransaction(queue_iter->txn_id_);
      queued_transaction->SetState(TransactionState::ABORTED);
      queue_iter = request_queue.request_queue_.erase(queue_iter);
    } else {
      if (queue_iter->lock_mode_ == LockMode::EXCLUSIVE) {
        should_wait = true;
      }
      queue_iter++;
    }
  }

  if (request_queue.upgrading_ != INVALID_TXN_ID) {
    if (txn->GetTransactionId() < request_queue.upgrading_) {
      Transaction *upgrading_transaction = TransactionManager::GetTransaction(request_queue.upgrading_);
      upgrading_transaction->SetState(TransactionState::ABORTED);
      request_queue.upgrading_ = INVALID_TXN_ID;
    } else {
      should_wait = true;
    }
  }

  if (request_queue.exclusive_lock_holder_id_ != INVALID_TXN_ID) {
    if (txn->GetTransactionId() < request_queue.exclusive_lock_holder_id_) {
      Transaction *exclusive_lock_holder_transaction =
          TransactionManager::GetTransaction(request_queue.exclusive_lock_holder_id_);
      exclusive_lock_holder_transaction->SetState(TransactionState::ABORTED);
      request_queue.exclusive_lock_holder_id_ = INVALID_TXN_ID;
    } else {
      should_wait = true;
    }
  }

  auto shared_lock_iter = request_queue.shared_lock_holders_.begin();
  while (shared_lock_iter != request_queue.shared_lock_holders_.end()) {
    if (txn->GetTransactionId() < *shared_lock_iter) {
      Transaction *shared_lock_holder_transaction = TransactionManager::GetTransaction(*shared_lock_iter);
      shared_lock_holder_transaction->SetState(TransactionState::ABORTED);
      shared_lock_iter = request_queue.shared_lock_holders_.erase(shared_lock_iter);
    } else {
      should_wait = true;
      shared_lock_iter++;
    }
  }

  if (should_wait) {
    request_queue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
    while (txn->GetState() != TransactionState::ABORTED && !request_queue.IsLockGranted(txn->GetTransactionId())) {
      request_queue.cv_.wait(manager_lock);
    }
  } else {
    request_queue.exclusive_lock_holder_id_ = txn->GetTransactionId();
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
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  if (!txn->IsSharedLocked(rid)) {
    return false;
  }

  std::unique_lock manager_lock(latch_);
  LockRequestQueue &request_queue = lock_table_[rid];

  if (request_queue.upgrading_ != INVALID_TXN_ID) {
    AbortCurrentTransaction(txn);
    return false;
  }

  request_queue.shared_lock_holders_.erase(txn->GetTransactionId());

  bool should_wait = false;
  auto queue_iter = request_queue.request_queue_.begin();
  while (queue_iter != request_queue.request_queue_.end()) {
    if (txn->GetTransactionId() < queue_iter->txn_id_) {
      Transaction *queued_transaction = TransactionManager::GetTransaction(queue_iter->txn_id_);
      queued_transaction->SetState(TransactionState::ABORTED);
      queue_iter = request_queue.request_queue_.erase(queue_iter);
    } else {
      if (queue_iter->lock_mode_ == LockMode::EXCLUSIVE) {
        should_wait = true;
      }
      queue_iter++;
    }
  }

  if (request_queue.exclusive_lock_holder_id_ != INVALID_TXN_ID) {
    if (txn->GetTransactionId() < request_queue.exclusive_lock_holder_id_) {
      Transaction *exclusive_lock_holder_transaction =
          TransactionManager::GetTransaction(request_queue.exclusive_lock_holder_id_);
      exclusive_lock_holder_transaction->SetState(TransactionState::ABORTED);
      request_queue.exclusive_lock_holder_id_ = INVALID_TXN_ID;
    } else {
      should_wait = true;
    }
  }

  auto shared_lock_iter = request_queue.shared_lock_holders_.begin();
  while (shared_lock_iter != request_queue.shared_lock_holders_.end()) {
    if (txn->GetTransactionId() < *shared_lock_iter) {
      Transaction *shared_lock_holder_transaction = TransactionManager::GetTransaction(*shared_lock_iter);
      shared_lock_holder_transaction->SetState(TransactionState::ABORTED);
      shared_lock_iter = request_queue.shared_lock_holders_.erase(shared_lock_iter);
    } else {
      should_wait = true;
      shared_lock_iter++;
    }
  }

  if (should_wait) {
    request_queue.upgrading_ = txn->GetTransactionId();
    request_queue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
    while (txn->GetState() != TransactionState::ABORTED && !request_queue.IsLockGranted(txn->GetTransactionId())) {
      request_queue.cv_.wait(manager_lock);
    }
  } else {
    request_queue.exclusive_lock_holder_id_ = txn->GetTransactionId();
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

  if (request_queue.exclusive_lock_holder_id_ == txn->GetTransactionId()) {
    request_queue.exclusive_lock_holder_id_ = INVALID_TXN_ID;
  }
  if (auto shared_lock_iter = request_queue.shared_lock_holders_.find(txn->GetTransactionId());
      shared_lock_iter != request_queue.shared_lock_holders_.end()) {
    request_queue.shared_lock_holders_.erase(shared_lock_iter);
  }

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  ProcessQueue(&request_queue);
  request_queue.cv_.notify_all();

  return true;
}

void LockManager::ProcessQueue(LockRequestQueue *request_queue) {
  auto queue_iter = request_queue->request_queue_.begin();
  while (queue_iter != request_queue->request_queue_.end()) {
    if (queue_iter->lock_mode_ == LockMode::SHARED) {
      request_queue->shared_lock_holders_.insert(queue_iter->txn_id_);
      queue_iter = request_queue->request_queue_.erase(queue_iter);
    } else {
      if (request_queue->shared_lock_holders_.empty()) {
        request_queue->exclusive_lock_holder_id_ = queue_iter->txn_id_;
        if (queue_iter->txn_id_ == request_queue->upgrading_) {
          request_queue->upgrading_ = INVALID_TXN_ID;
        }
        request_queue->request_queue_.erase(queue_iter);
        break;
      }
    }
  }
}

}  // namespace bustub
