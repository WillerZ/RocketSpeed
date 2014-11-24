// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

namespace rocketspeed {

/**
 * Callback to notify Client about loaded subscription data.
 */
typedef std::function<void(const std::vector<SubscriptionRequest>&)>
    LoadCallback;

/**
 * Defines how client saves and restores subscription data.
 */
class SubscriptionStorage {
 public:
  virtual ~SubscriptionStorage();

  /**
   * Initializes the storage, which includes setting callbacks to be
   * executed when subscription data is successfully stored or loaded.
   * The method is called by a client only once, before any other method.
   *
   * Any of the arguments can be nullptr.
   */
  virtual void Initialize(LoadCallback load_callback);

  /**
   * Asynchronously stores given subscription data.
   */
  virtual void Store(const SubscriptionRequest& message);

  /**
   * Asynchronously removes subscription data entry for given topic (in given
   * namespace).
   */
  virtual void Remove(const SubscriptionRequest& message);

  /**
   * Loads subscription data asynchronously for given topics, appropriate
   * callback is invoked with all retrieved subscriptions.
   */
  virtual void Load(const std::vector<SubscriptionRequest>& requests);

  /**
   * Loads all subscription data asynchronously.
   */
  virtual void LoadAll();
};

}  // namespace rocketspeed
