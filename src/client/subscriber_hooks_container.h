// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//

#pragma once

#include <assert.h>
#include <memory>
#include <unordered_map>

#include "src/client/subscriber_if.h"

namespace rocketspeed {

/**
 * Utility class for keeping SubscriberIf implementation hooks.
 * Indexes hooks by HooksParameters and SubscriptionID for efficient access.
 */
class SubscriberHooksContainer {
 public:
  /**
   * Add new hooks to container.
   */
  void Install(const HooksParameters& params,
               std::shared_ptr<SubscriberHooks> hooks) {
    bool inserted = to_be_hooked_.emplace(params, hooks).second;
    assert(inserted);
  }

  /**
   * Remove hooks from container.
   */
  void UnInstall(const HooksParameters& params) {
    if (to_be_hooked_.erase(params)) {
      return;
    }
    auto it = currently_hooked_.find(params);
    RS_ASSERT(it != currently_hooked_.end()) << "wrong call";
    if (it != currently_hooked_.end()) {
      bool erased = hooks_.erase(it->second);
      assert(erased);
      currently_hooked_.erase(it);
    }
  }

  /**
   * Should be called on new subscription.
   */
  void SubscriptionStarted(const HooksParameters& params, SubscriptionID id) {
    assert(currently_hooked_.find(params) == currently_hooked_.end());
    auto it = to_be_hooked_.find(params);
    if (it != to_be_hooked_.end()) {
      auto in = currently_hooked_.emplace(params, id).second;
      assert(in);
      hooks_.emplace(id, HooksWithParams(it->second, params));
      assert(in);
      to_be_hooked_.erase(it);
    }
  }

  /**
   * Should be called when subscription ended.
   */
  void SubscriptionEnded(SubscriptionID id) {
    auto it = hooks_.find(id);
    if (it != hooks_.end()) {
      auto erased = currently_hooked_.erase(it->second.params);
      assert(erased);
      to_be_hooked_.emplace(it->second.params, it->second.hooks);
      hooks_.erase(it);
    }
  }

  /**
   * Return hooks for specific subscriptions if any.
   * If no hooks are installed for the subscription no-op hooks are returned.
   */
  SubscriberHooks& operator[](SubscriptionID id) {
    auto it = hooks_.find(id);
    if (it != hooks_.end()) {
      return *it->second.hooks;
    } else {
      return noop_hooks_;
    }
  }

 private:
  class NoopHooks : public SubscriberHooks {
    virtual void SubscriptionExists() final {}
    virtual void OnStartSubscription() final {}
    virtual void OnAcknowledge(SequenceNumber) final {}
    virtual void OnTerminateSubscription() final {}
    virtual void OnReceiveTerminate() final {};
    virtual void OnMessageReceived(MessageReceived*) final {}
    virtual void OnSubscriptionStatusChange(const SubscriptionStatus&) final {}
    virtual void OnDataLoss(const DataLossInfo&) final {}
  };
  /** Called when no hooks for subscription were installed */
  NoopHooks noop_hooks_;
  /** Hooks are kept here when there's now subscription (yet) for the parameters
   */
  std::unordered_map<HooksParameters, std::shared_ptr<SubscriberHooks>>
      to_be_hooked_;
  /** Mapping kept when subscriptions exists (i.e. has SubscriptionID assigned)
   */
  std::unordered_map<HooksParameters, SubscriptionID> currently_hooked_;
  struct HooksWithParams {
    explicit HooksWithParams(std::shared_ptr<SubscriberHooks> _hooks,
                             const HooksParameters& _params)
        : hooks(_hooks), params(_params) {}

    std::shared_ptr<SubscriberHooks> hooks;
    HooksParameters params;
  };
  /** Hooks are kept here when subscription exists.  */
  std::unordered_map<SubscriptionID, HooksWithParams> hooks_;
};

}  // namespace rocketspeed
