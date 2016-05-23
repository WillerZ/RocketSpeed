// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <limits>
#include <string>
#include <vector>

#include "Types.h"

namespace rocketspeed {

/**
 * An opaque type which carries an information about the origin of a request for
 * end-to-end flow control purposes.
 *
 * User of the Rocketeer should either pass the pointer between Rocketeers in
 * the stack, or, in rare cases, provide nullptr in place of this argument to
 * ignore backpressure.
 * The object provided to Rocketeer's callbacks is only guaranteed to exist for
 * the duration of the callback, hence it is strongly advised NOT to retain it.
 */
class Flow;

typedef unsigned long long int StreamID;
static_assert(sizeof(StreamID) == 8, "Invalid StreamID size.");

/** Uniquely identifies subscription within a service. */
class InboundID {
 public:
  InboundID() : stream_id(std::numeric_limits<StreamID>::max()), sub_id(0) {}

  InboundID(StreamID _stream_id, uint64_t _sub_id)
  : stream_id(_stream_id), sub_id(_sub_id) {}

  StreamID stream_id;
  uint64_t sub_id;

  bool operator==(const InboundID& other) const {
    return stream_id == other.stream_id && sub_id == other.sub_id;
  }

  size_t Hash() const;

  std::string ToString() const;
};

struct RocketeerMessage {
  RocketeerMessage(uint64_t _sub_id,
                   SequenceNumber _seqno,
                   std::string _payload,
                   MsgId _msg_id = MsgId())
  : sub_id(std::move(_sub_id))
  , seqno(_seqno)
  , payload(std::move(_payload))
  , msg_id(_msg_id) {}

  uint64_t sub_id;
  SequenceNumber seqno;
  std::string payload;
  MsgId msg_id;
};

class Rocketeer {
 public:
  Rocketeer() : below_rocketeer_(nullptr) {}

  virtual ~Rocketeer() = default;

  /**
   * Notifies about new inbound subscription.
   * This method is guaranteed to always be called on the same thread.
   *
   * @param inbound_id Globally unique ID of this subscription.
   * @param params Parameters of the subscription provided by the subscriber.
   */
  virtual void HandleNewSubscription(InboundID inbound_id,
                                     SubscriptionParameters params) = 0;

  /**
   * Notifies that given subscription was terminated by the user.
   * This method is guaranteed to always be called on the same thread.
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
  virtual void Deliver(Flow* flow,
                       InboundID inbound_id,
                       SequenceNumber seqno,
                       std::string payload,
                       MsgId msg_id = MsgId());

  /**
   * Sends a batch of messages on multiple subscriptions.
   * This method needs to be called on the thread this instance runs on.
   *
   * @param stream_id ID of the stream where to send messages.
   * @param messages List of messages to send.
   */
  virtual void DeliverBatch(Flow* flow,
                            StreamID stream_id,
                            std::vector<RocketeerMessage> messages);

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
   * @return true iff operation was successfully scheduled.
   */
  virtual void Advance(Flow* flow, InboundID inbound_id, SequenceNumber seqno);

  /**
   * Terminates given subscription.
   * This method needs to be called on the thread this instance runs on.
   *
   * @param inbound_id ID of the subscription to terminate.
   * @param reason A reason why this subscription was terminated.
   * @return true iff operation was successfully scheduled.
   */
  enum class UnsubscribeReason {
    /** The unsubscribe was requested by the subscriber. */
    Requested,

    /** The subscription parameters are invalid and cannot be served. */
    Invalid,
  };
  virtual void Terminate(Flow* flow,
                         InboundID inbound_id,
                         UnsubscribeReason reason);

  /**
   * Returns a pointer to the Rocketeer below this one in the stack, which
   * provides implementation of Deliver, Advance and Terminate that this
   * Rocketeer should use.
   */
  Rocketeer* GetBelowRocketeer() {
    RS_ASSERT(below_rocketeer_);
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
