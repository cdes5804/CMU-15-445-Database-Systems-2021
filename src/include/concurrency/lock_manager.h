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
#include <deque>
#include <memory>
#include <mutex>  // NOLINT
#include <set>
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
    LockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode) {}

    txn_id_t txn_id_;
    LockMode lock_mode_;
  };

  class LockRequestQueue {
   public:
    std::mutex queue_latch_;
    std::set<txn_id_t> shared_lock_holders_;
    std::deque<LockRequest> request_queue_;
    std::vector<txn_id_t> aborted_transaction_ids_;
    // for notifying blocked transactions on this rid
    std::condition_variable cv_;
    // txn_id of an upgrading transaction (if any)
    txn_id_t upgrading_ = INVALID_TXN_ID;
    // txn_id of the tranaction holding the exclusive lock
    txn_id_t exclusive_lock_holder_id_ = INVALID_TXN_ID;

    bool IsLockGranted(txn_id_t txn_id) const {
      return upgrading_ != txn_id &&
             (shared_lock_holders_.find(txn_id) != shared_lock_holders_.end() || exclusive_lock_holder_id_ == txn_id);
    }

    void SubmitLockRequest(Transaction *txn, LockMode lock_mode) {
      if (txn->GetState() == TransactionState::ABORTED) {
        return;
      }

      while (!request_queue_.empty()) {
        if (txn->GetTransactionId() < request_queue_.back().txn_id_) {
          aborted_transaction_ids_.emplace_back(request_queue_.back().txn_id_);
          request_queue_.pop_back();
        }
      }
      request_queue_.emplace_back(txn->GetTransactionId(), lock_mode);
      ProcessQueue();
    }

    bool SubmitUpgradeRequest(Transaction *txn) {
      if (txn->GetState() == TransactionState::ABORTED) {
        return false;
      }

      if (upgrading_ != INVALID_TXN_ID && upgrading_ != txn->GetTransactionId()) {
        return false;
      }
      upgrading_ = txn->GetTransactionId();
      ProcessQueue();
      return true;
    }

    void Unlock(txn_id_t txn_id) {
      if (auto iter = shared_lock_holders_.find(txn_id); iter != shared_lock_holders_.end()) {
        shared_lock_holders_.erase(iter);
      }
      if (exclusive_lock_holder_id_ == txn_id) {
        exclusive_lock_holder_id_ = INVALID_TXN_ID;
      }
      ProcessQueue();
    }

    void ProcessQueue() {
      // check if there is a pending upgrade request, which has precedence over other requests
      if (upgrading_ != INVALID_TXN_ID) {
        if (upgrading_ <= GetSharedLockOldestTransactionId()) {
          for (auto shared_lock_holders : shared_lock_holders_) {
            if (shared_lock_holders != upgrading_) {
              aborted_transaction_ids_.emplace_back(shared_lock_holders);
            }
          }
          shared_lock_holders_.clear();
          exclusive_lock_holder_id_ = upgrading_;
          upgrading_ = INVALID_TXN_ID;
        }
        return;
      }

      while (!request_queue_.empty()) {
        const LockRequest &request = request_queue_.front();
        if (request.lock_mode_ == LockMode::SHARED) {
          if (exclusive_lock_holder_id_ == INVALID_TXN_ID || request.txn_id_ < exclusive_lock_holder_id_) {
            if (exclusive_lock_holder_id_ != INVALID_TXN_ID) {
              aborted_transaction_ids_.emplace_back(exclusive_lock_holder_id_);
              exclusive_lock_holder_id_ = INVALID_TXN_ID;
            }
            shared_lock_holders_.insert(request.txn_id_);
            request_queue_.pop_front();
          } else {
            break;
          }
        } else if (request.lock_mode_ == LockMode::EXCLUSIVE) {
          if (exclusive_lock_holder_id_ != INVALID_TXN_ID && request.txn_id_ < exclusive_lock_holder_id_) {
            aborted_transaction_ids_.emplace_back(exclusive_lock_holder_id_);
            exclusive_lock_holder_id_ = request.txn_id_;
            request_queue_.pop_front();
          } else if (auto oldest_id = GetSharedLockOldestTransactionId();
                     exclusive_lock_holder_id_ == INVALID_TXN_ID &&
                     (oldest_id == INVALID_TXN_ID || request.txn_id_ < oldest_id)) {
            for (auto shared_lock_holders : shared_lock_holders_) {
              aborted_transaction_ids_.emplace_back(shared_lock_holders);
            }
            shared_lock_holders_.clear();
            exclusive_lock_holder_id_ = request.txn_id_;
            request_queue_.pop_front();
          }
          break;  // no matter the exclusive lock is granted or not, the processing stops
        }
      }
    }

    txn_id_t GetSharedLockOldestTransactionId() const {
      return shared_lock_holders_.empty() ? INVALID_TXN_ID : *shared_lock_holders_.begin();
    }

    std::vector<txn_id_t> GetAbortedTransactions() const { return aborted_transaction_ids_; }
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
  bool LockShared(Transaction *txn, const RID &rid);

  /**
   * Acquire a lock on RID in exclusive mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the exclusive lock
   * @param rid the RID to be locked in exclusive mode
   * @return true if the lock is granted, false otherwise
   */
  bool LockExclusive(Transaction *txn, const RID &rid);

  /**
   * Upgrade a lock from a shared lock to an exclusive lock.
   * @param txn the transaction requesting the lock upgrade
   * @param rid the RID that should already be locked in shared mode by the
   * requesting transaction
   * @return true if the upgrade is successful, false otherwise
   */
  bool LockUpgrade(Transaction *txn, const RID &rid);

  /**
   * Release the lock held by the transaction.
   * @param txn the transaction releasing the lock, it should actually hold the
   * lock
   * @param rid the RID that is locked by the transaction
   * @return true if the unlock is successful, false otherwise
   */
  bool Unlock(Transaction *txn, const RID &rid);

 private:
  std::mutex latch_;

  /** Lock table for lock requests. */
  std::unordered_map<RID, LockRequestQueue> lock_table_;
  void AbortTransaction(Transaction *txn, AbortReason reason);
  void WoundTransactions(const std::vector<txn_id_t> &transaction_ids);
};

}  // namespace bustub
