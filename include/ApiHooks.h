// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//

#pragma once
#include "include/Types.h"

namespace rocketspeed {

/**
 * Interface letting user to trace or debug Client with a set of hooks
 * corresponding to its API.
 * Multithread clients will callback from thread specific
 * to particular subscription.
 */
class ClientHooks {
 public:
  virtual ~ClientHooks() = default;

  /**
   * Called after hooks are installed iff subscription exists.
   */
  virtual void SubscriptionExists() = 0;
  /**
   * Called when Subscribe() was called on the client.
   */
  virtual void OnSubscribe() = 0;
  /**
   * Called when Unsubscribe() was called on the client.
   */
  virtual void OnUnsubscribe() = 0;
  /**
   * Called when Acknowledge() was called on the client.
   */
  virtual void OnAcknowledge(SequenceNumber seqno) = 0;
  /**
   * Called when MessageReceived() was called on the observer.
   */
  virtual void OnMessageReceived(MessageReceived* msg) = 0;
  /**
   * Called when OnSubscriptionStatusChange() was called on observer.
   */
  virtual void OnSubscriptionStatusChange(const SubscriptionStatus&) = 0;
  /**
   * Called when OnDataLoss() was called on observer.
   */
  virtual void OnDataLoss(const DataLossInfo& info) = 0;
};

}  // namespace rocketspeed
