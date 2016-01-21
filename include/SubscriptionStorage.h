//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>
#include <vector>

#include "Types.h"

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC visibility push(default)
#endif
namespace rocketspeed {

class BaseEnv;
class Logger;
class Status;
class SubscriptionParameters;

/** Defines how the RocketSpeed Client saves and restores subscription data. */
class SubscriptionStorage {
 public:
  /**
   * Creates subscription storage backed by a file.
   *
   * The file must not be concurrently used by two different instances of the
   * storage, and this must be ensured by the creator of the storage.
   *
   * @param env Environment used by the storage,
   * @param info_log Log for info messages.
   * @param file_path Path to a file in which subscription state is persisted,
   */
  static Status File(BaseEnv* env,
                     std::shared_ptr<Logger> info_log,
                     std::string file_path,
                     std::unique_ptr<SubscriptionStorage>* out);

  /** Represents a snapshot being written. */
  class Snapshot {
   public:
    virtual ~Snapshot() {}

    /**
     * Adds entry for subscription with provided parameters to the snapshot.
     * Calling thread must identify itself with a nuemric ID which is lower than
     * number of threads provided when creating the snapshot (IDs start form 0).
     *
     * @param thread_id A numeric ID of the writing thread.
     * @return Status::OK() iff append was successfull.
     *
     * Rest of the parameters are self explanatory.
     */
    virtual Status Append(size_t thread_id,
                          TenantID tenant_id,
                          const NamespaceID& namespace_id,
                          const Topic& topic_name,
                          SequenceNumber start_seqno) = 0;

    /**
     * Commits all data written by all threads.
     *
     * This call must be the last operation on the snapshot, and the caller must
     * provide external synchronisation between all writers and the thread
     * calling this function. A snapshot cannot be committed if any append from
     * any thread failed, and it must be ensured externally.
     *
     * @return Status::OK() iff append was successfull.
     */
    virtual Status Commit() = 0;
  };

  virtual ~SubscriptionStorage() {}

  /**
   * Reads all subscriptions saved by strategy selected when opening the client.
   *
   * @subscriptions An out parameter with a list of restored subscriptions.
   * @return Status::OK iff subscriptions were restored successfully.
   */
  virtual Status RestoreSubscriptions(
      std::vector<SubscriptionParameters>* subscriptions) = 0;

  /**
   * Creates a new snapshot context.
   *
   * Subscription state must be written into a snapshot, and can be written by
   * up to provided number of different threads.
   *
   * @param num_threads Number of threads that can be used to write snapshot.
   * @param snapshot An out parameter for snapshot handle.
   * @return Status::OK() iff append was successfull.
   */
  virtual Status CreateSnapshot(size_t num_threads,
                                std::shared_ptr<Snapshot>* snapshot) = 0;
};

}  // namespace rocketspeed
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC visibility pop
#endif
