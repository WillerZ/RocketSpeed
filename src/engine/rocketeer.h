// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <cassert>
#include <limits>
#include <string>

#include "include/Types.h"
#include "src/messages/messages.h"
#include "src/messages/stream_socket.h"
#include "src/util/common/hash.h"

namespace rocketspeed {

/** Uniquely identifies subscription within a service. */
class InboundID {
 public:
  // TODO(stupaq) remove
  InboundID() : worker_id(-1) {}

  InboundID(StreamID _stream_id, SubscriptionID _sub_id, size_t _worker_id)
  : stream_id(_stream_id)
  , sub_id(_sub_id)
  , worker_id(static_cast<int>(_worker_id)) {}

  StreamID stream_id;
  SubscriptionID sub_id;
  // TODO(stupaq) reversible allocators
  int worker_id;

  bool operator==(const InboundID& other) const {
    return stream_id == other.stream_id && sub_id == other.sub_id;
  }

  size_t Hash() const {
    return MurmurHash2<StreamID, SubscriptionID>()(stream_id, sub_id);
  }

  std::string ToString() const;
};

class Rocketeer {
 public:
  Rocketeer() : below_rocketeer_(nullptr) {}

  virtual ~Rocketeer() = default;

  /**
   * Notifies about new inbound subscription.
   * This method is guarenteed to always be called on the same thread.
   *
   * @param inbound_id Globally unique ID of this subscription.
   * @param params Parameters of the subscription provided by the subscriber.
   */
  virtual void HandleNewSubscription(InboundID inbound_id,
                                     SubscriptionParameters params) = 0;

  /**
   * Notifies that given subscription was terminated by the user.
   * This method is guarenteed to always be called on the same thread.
   *
   * @param inbound_id ID of the subscription to be terminated.
   * @param source Who terminated the subscription.
   */
  enum class TerminationSource {
    Subscriber,
    Rocketeer,
  };
  virtual void HandleTermination(InboundID inbound_id,
                                 TerminationSource source) = 0;

  /**
   * Sends a message on given subscription.
   * This method needs to be called on the thread this instance runs on.
   *
   * @param inbound_id ID of the subscription to send message on.
   * @param seqno Sequence number of the message.
   * @param payload Payload of the message.
   * @param msg_id The ID of the message.
   */
  virtual void Deliver(InboundID inbound_id,
                       SequenceNumber seqno,
                       std::string payload,
                       MsgId msg_id = MsgId());

  /**
   * Advances next expected sequence number on a subscription without sending
   * data on it. Client might be notified, so that the next time it
   * resubscribes, it will send advanced sequence number as a part of
   * subscription parameters.
    * This method needs to be called on the thread this instance runs on.
   *
   * @param inbound_id ID of the subscription to advance.
   * @param seqno The subscription will be advanced, so that it expects the
   *              next sequence number.
   * @return true iff operation was successfully sheduled.
   */
  virtual void Advance(InboundID inbound_id, SequenceNumber seqno);

  /**
   * Terminates given subscription.
   * This method needs to be called on the thread this instance runs on.
   *
   * @param inbound_id ID of the subscription to terminate.
   * @param reason A reason why this subscription was terminated.
   * @return true iff operation was successfully sheduled.
   */
  virtual void Terminate(InboundID inbound_id,
                         MessageUnsubscribe::Reason reason);

  /**
   * Returns a pointer to the Rocketeer below this one in the stack, which
   * provides implementation of Deliver, Advance and Terminate that this
   * Rocketeer should use.
   */
  Rocketeer* GetBelowRocketeer() {
    assert(below_rocketeer_);
    return below_rocketeer_;
  }

  /** Provides the Rocketeer below this one in the stack. */
  void SetBelowRocketeer(Rocketeer* rocketeer) { below_rocketeer_ = rocketeer; }

 private:
  Rocketeer* below_rocketeer_;
};

}  // namespace rocketspeed

namespace std {
template <>
struct hash<rocketspeed::InboundID> {
  size_t operator()(const rocketspeed::InboundID& x) const { return x.Hash(); }
};
}  // namespace std
