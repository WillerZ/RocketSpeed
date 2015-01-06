// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <functional>
#include <memory>
#include <vector>

#include "include/Logger.h"
#include "include/Status.h"
#include "include/Types.h"

namespace rocketspeed {

class MsgLoopBase;

/**
 * Callback to notify Client about loaded subscription data.
 */
typedef std::function<void(const std::vector<SubscriptionRequest>&)>
    LoadCallback;

/**
 * Callback to notify Client about finished update.
 */
typedef std::function<void(const SubscriptionRequest&)> UpdateCallback;

/**
 * Callback to notify Client about finished snapshot.
 */
typedef std::function<void(Status)> SnapshotCallback;

/**
 * Defines how the RocketSpeed Client saves and restores subscription data.
 * Subscription storage is needed for every application, which requires
 * subscriptions to be resumed after it the application is restarted.
 * Application can provide its own mechanism to save and restore subscription
 * state, which must implement the following interface.
 */
class SubscriptionStorage {
 public:
  /**
   * Creates subscription storage backed by a file.
   * The file must not be concurrently used by two different instances of the
   * storage, and this must be ensured by the caller.
   * @param file_path path to a file in which subscription state is persisted
   * @param info_log log for info messages.
   */
  static Status File(const std::string& file_path,
                     std::shared_ptr<Logger> info_log,
                     std::unique_ptr<SubscriptionStorage>* out);

  virtual ~SubscriptionStorage() {}

  /**
   * Sets callback functions which consume asynchronous events.
   * Also provides storage implementation with message loop.
   * This method is called by the Client before any other method of this class.
   */
  virtual void Initialize(LoadCallback load_callback,
                          UpdateCallback update_callback,
                          SnapshotCallback write_snapshot_callback,
                          MsgLoopBase* msg_loop) = 0;

  /**
   * Fils subscription storage with persisted data.
   * Can be called on initialized storage only, before backing message loop
   * starts. User can choose not to invoke this method, in which case the
   * storage will be initially empty.
   */
  virtual Status ReadSnapshot() = 0;

  /**
   * Asynchronously stores or removes given subscription data for given topic.
   * This should be invoked on every subscribe/unsubscribe, as well as every
   * message acknowledged by the application.
   */
  virtual void Update(SubscriptionRequest message) = 0;

  /**
   * Loads subscription data asynchronously for given topics, appropriate
   * callback is invoked with all retrieved subscriptions.
   */
  virtual void Load(std::vector<SubscriptionRequest> requests) = 0;

  /**
   * Loads all subscription data, appropriate callback is invoked with all
   * retrieved subscriptions.
   */
  virtual void LoadAll() = 0;

  /**
   * Asynchronously initiates snapshot creation. After successfull snapshot
   * appropriate callback is invoked. All subscription data which was included
   * in the snapshot, must be available after the application restarts.
   *
   * Implementation must ensure that all update operations that succeeded before
   * this method is called, are included in the snapshot. In other words, if an
   * update callback was invoked for a certain operation, then corresponding
   * changes to subscription state will be included in the snapshot.
   */
  virtual void WriteSnapshot() = 0;
};

}  // namespace rocketspeed
