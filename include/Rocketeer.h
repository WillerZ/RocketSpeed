// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <chrono>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "BackPressure.h"
#include "Types.h"

namespace rocketspeed {

template <typename> class RetryLaterSink;

struct RocketeerMetadataMessage;
class SubscriptionID;

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

  SubscriptionID GetSubID() const;

  size_t GetShard() const;

  bool operator==(const InboundID& other) const {
    return stream_id == other.stream_id && sub_id == other.sub_id;
  }

  size_t Hash() const;

  std::string ToString() const;

  friend bool operator<(const InboundID& lhs, const InboundID& rhs) {
    return lhs.stream_id < rhs.stream_id ||
      (lhs.stream_id == rhs.stream_id && lhs.sub_id < rhs.sub_id);
  }
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

  RocketeerMessage(uint64_t _sub_id,
                   NamespaceID _namespace_id,
                   Topic _topic,
                   SequenceNumber _seqno,
                   std::string _payload,
                   MsgId _msg_id = MsgId())
  : sub_id(std::move(_sub_id))
  , namespace_id(std::move(_namespace_id))
  , topic(std::move(_topic))
  , seqno(_seqno)
  , payload(std::move(_payload))
  , msg_id(_msg_id) {}

  uint64_t sub_id;
  NamespaceID namespace_id;
  Topic topic;
  SequenceNumber seqno;
  std::string payload;
  MsgId msg_id;

  SubscriptionID GetSubID() const;
};

class Rocketeer {
 public:
  Rocketeer();

  virtual ~Rocketeer();

  /**
   * Notifies about new inbound subscription.
   * This method is guaranteed to always be called on the same thread.
   * Implementations should provide either TryHandleNewSubscription or
   * HandleNewSubscription, but not both. If both are provided, this version
   * is ignored.
   *
   * @param inbound_id Globally unique ID of this subscription.
   * @param params Parameters of the subscription provided by the subscriber.
   * @return Back pressure status. Use BackPressure::None() if message was
   *         processed successfully, otherwise specify type of back pressure.
   */
  virtual BackPressure TryHandleNewSubscription(
      InboundID inbound_id, SubscriptionParameters params);

  /**
   * Notifies about new inbound subscription.
   * This method is guaranteed to always be called on the same thread.
   * Implementations should provide either TryHandleNewSubscription or
   * HandleNewSubscription, but not both. If both are provided, this version
   * is preferred.
   *
   * @param flow Flow control handle for exerting back-pressure.
   * @param inbound_id Globally unique ID of this subscription.
   * @param params Parameters of the subscription provided by the subscriber.
   */
  virtual void HandleNewSubscription(Flow* flow,
                                     InboundID inbound_id,
                                     SubscriptionParameters params);

  /**
   * Notifies that given subscription was terminated by the user.
   * This method is guaranteed to always be called on the same thread.
   * Implementations should provide either TryHandleTermination or
   * HandleTermination, but not both. If both are provided, this version is
   * ignored.
   *
   * @param inbound_id ID of the subscription to be terminated.
   * @param source Who terminated the subscription.
   * @return Back pressure status. Use BackPressure::None() if message was
   *         processed successfully, otherwise specify type of back pressure.
   */
  enum class TerminationSource {
    Subscriber,
    Rocketeer,
  };
  virtual BackPressure TryHandleUnsubscribe(
      InboundID inbound_id,
      NamespaceID namespace_id,
      Topic topic,
      TerminationSource source);

  // DEPRECATED
  virtual BackPressure TryHandleTermination(
      InboundID inbound_id, TerminationSource source);

  /**
   * Notifies that given subscription was terminated by the user.
   * This method is guaranteed to always be called on the same thread.
   * Implementations should provide either TryHandleTermination or
   * HandleTermination, but not both. If both are provided, this version is
   * preferred.
   *
   * @param flow Flow control handle for exerting back-pressure.
   * @param inbound_id ID of the subscription to be terminated.
   * @param source Who terminated the subscription.
   */
  virtual void HandleUnsubscribe(
      Flow* flow,
      InboundID inbound_id,
      NamespaceID namespace_id,
      Topic topic,
      TerminationSource source);

  // DEPRECATED
  virtual void HandleTermination(
      Flow* flow, InboundID inbound_id, TerminationSource source);

  /**
   * Notifies about a new HasMessageSince request.
   * The implementation must successfully invoke HasMessageSinceResponse at
   * least once on the same inbound_id, namespace, and topic, after this call
   * has begun.
   *
   * The response must be true iff there has been a message on the specified
   * topic, strictly after (epoch, seqno) and before the subscription's current
   * position.
   *
   * Implementations should provide either TryHandleHasMessageSince or
   * HandleHasMessageSince, but not both. If both are provided, this version is
   * preferred.
   *
   * This method needs to be called on the thread this instance runs on.
   *
   * @param flow Flow control handle for exerting back-pressure.
   * @param inbound_id ID of the subscription to test against.
   * @param namespace_id The namespace of the topic to test.
   * @param topic The topic name of the topic to test.
   * @param epoch The epoch of the starting point of the test.
   * @param seqno The sequence number of the starting point of the test.
   */
  virtual void HandleHasMessageSince(
      Flow* flow, InboundID inbound_id, NamespaceID namespace_id, Topic topic,
      Epoch epoch, SequenceNumber seqno);

  /**
   * Notifies about a new HasMessageSince request.
   * The implementation must successfully invoke HasMessageSinceResponse at
   * least once on the same inbound_id, namespace, and topic, after this call
   * has begun.
   *
   * The response must be true iff there has been a message on the specified
   * topic, strictly after (epoch, seqno) and before the subscription's current
   * position.
   *
   * Implementations should provide either TryHandleHasMessageSince or
   * HandleHasMessageSince, but not both. If both are provided, this version is
   * ignored.
   *
   * This method needs to be called on the thread this instance runs on.
   *
   * @param inbound_id ID of the subscription to test against.
   * @param namespace_id The namespace of the topic to test.
   * @param topic The topic name of the topic to test.
   * @param epoch The epoch of the starting point of the test.
   * @param seqno The sequence number of the starting point of the test.
   * @return Back pressure status. Use BackPressure::None() if message was
   *         processed successfully, otherwise specify type of back pressure.
   */
  virtual BackPressure TryHandleHasMessageSince(
      InboundID inbound_id, NamespaceID namespace_id, Topic topic, Epoch epoch,
      SequenceNumber seqno);

  /**
   * Notifies that a stream has disconnected.
   *
   * If the RocketeerServer is running with terminate_on_disconnect then
   * the rocketeer will automatically receive HandleTermination calls for each
   * subscription on the stream, before calling HandleDisconnect.
   *
   * If the RocketeerServer is not running with terminate_on_disconnect then
   * it is the responsibility of the implementation to manually terminate
   * any inbound subscription with the specified stream ID.
   *
   * Implementations should provide either TryHandleDisconnect or
   * HandleDisconnect, but not both. If both are provided, this version is
   * ignored.
   *
   * @param flow Flow control handle for exerting back-pressure.
   * @param stream_id The ID of the stream that has been disconnected.
   */
  virtual void HandleDisconnect(Flow* flow, StreamID stream_id);

  /**
   * Same as HandleDisconnect, but using return value for flow control.
   */
  virtual BackPressure TryHandleDisconnect(StreamID stream_id);

  /**
   * Notifies that a stream has connected.
   *
   * HandleConnect for a stream is guaranteed to be called before
   * any other callbacks for that stream.
   *
   * Implementations should provide either TryHandleConnect or
   * HandleConnect, but not both. If both are provided, this version is
   * ignored.
   *
   * @param flow Flow control handle for exerting back-pressure.
   * @param stream_id The ID of the stream that has been disconnected.
   * @param params Introduction parameters for the stream
   */
  virtual void HandleConnect(Flow* flow,
                             StreamID stream_id,
                             IntroParameters params);

  /**
   * Same as HandleConnect, but using return value for flow control.
   */
  virtual BackPressure TryHandleConnect(StreamID stream_id,
                                        IntroParameters params);

  /**
   * Sends a message on given subscription.
   * This method needs to be called on the thread this instance runs on.
   *
   * @param inbound_id ID of the subscription to send message on.
   * @param namespace_id Namespace to deliver on.
   * @param topic Topic to deliver on.
   * @param seqno Sequence number of the message.
   * @param payload Payload of the message.
   * @param msg_id The ID of the message.
   */
  virtual void Deliver(Flow* flow,
                       InboundID inbound_id,
                       NamespaceID namespace_id,
                       Topic topic,
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
   * @param namespace_id Namespace of the subscription.
   * @param topic Topic name of the subscription.
   * @param seqno The subscription will be advanced, so that it expects the
   *              next sequence number.
   * @return true iff operation was successfully scheduled.
   */
  virtual void Advance(Flow* flow,
                       InboundID inbound_id,
                       NamespaceID namespace_id,
                       Topic topic,
                       SequenceNumber seqno);

  /**
   * Same as Advance but triggers DataLoss callback on the client's observer.
   *
   * This method needs to be called on the thread this instance runs on.
   */
  virtual void NotifyDataLoss(Flow* flow,
                              InboundID inbound_id,
                              NamespaceID namespace_id,
                              Topic topic,
                              SequenceNumber seqno);

  /**
   * Unsubscribes given subscription.
   * This method needs to be called on the thread this instance runs on.
   *
   * @param inbound_id ID of the subscription to unsubscribe.
   * @param namespace_id Namespace of the subscription to unsubscribe.
   * @param topic Topic of the subscription to unsubscribe.
   * @param reason A reason why this subscription was unsubscribed.
   * @return true iff operation was successfully scheduled.
   */
  enum class UnsubscribeReason {
    /** The unsubscribe was requested by the subscriber. */
    Requested,

    /** The subscription parameters are invalid and cannot be served. */
    Invalid,
  };
  virtual void Unsubscribe(Flow* flow,
                           InboundID inbound_id,
                           NamespaceID namespace_id,
                           Topic topic,
                           UnsubscribeReason reason);

  /**
   * Response to TryHandleHasMessageSince. Must be called at least once after
   * each TryHandleHasMessageSince invocation. The inbound_id, namespace_id,
   * epoch, and seqno must be the same as that in the request.
   *
   * @param flow Handle for flow control.
   * @param inbound_id ID of the subscription that was tested.
   * @param namespace_id The namespace of the topic tested.
   * @param topic The topic name of the topic tested.
   * @param epoch The epoch of the starting point of the test.
   * @param seqno The sequence number of the starting point of the test.
   * @param response The result of the query. See HasMessageSinceResult.
   * @param info Extra info to supply about the result. Ignored by RocketSpeed.
   */
  virtual void HasMessageSinceResponse(
      Flow* flow, InboundID inbound_id, NamespaceID namespace_id, Topic topic,
      Epoch epoch, SequenceNumber seqno, HasMessageSinceResult response,
      std::string info);

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
  std::unique_ptr<RetryLaterSink<RocketeerMetadataMessage>> metadata_sink_;
};

}  // namespace rocketspeed

namespace std {
template <>
struct hash<rocketspeed::InboundID> {
  size_t operator()(const rocketspeed::InboundID& x) const { return x.Hash(); }
};

}  // namespace std
