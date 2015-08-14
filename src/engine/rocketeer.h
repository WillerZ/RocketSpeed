// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>
#include <limits>
#include <string>
#include <unordered_map>
#include <vector>

#include "include/Types.h"
#include "src/messages/messages.h"
#include "src/messages/stream_socket.h"
#include "src/util/common/base_env.h"
#include "src/util/common/hash.h"
#include "src/util/common/statistics.h"
#include "src/util/common/thread_check.h"

namespace rocketspeed {

class MsgLoop;
class MsgLoopThread;
class Logger;
class Rocketeer;
class RocketeerServer;

/** Uniquely identifies subscription within RocketeerServer. */
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

class InboundSubscription {
 public:
  InboundSubscription(TenantID _tenant_id, SequenceNumber _prev_seqno)
      : tenant_id(_tenant_id), prev_seqno(_prev_seqno) {}

  TenantID tenant_id;
  SequenceNumber prev_seqno;
};

/** Options for creating RocketeerServer. */
struct RocketeerOptions {
  static constexpr uint16_t DEFAULT_PORT = 58200;

  /** Fills in default values for parameters. */
  RocketeerOptions();

  /** Environment, defaults to Env::Default(). */
  BaseEnv* env;

  /** Logger, defaults to NullLogger. */
  std::shared_ptr<Logger> info_log;

  /** Port to listen on, defaults to RocketeerServer::DEFAULT_PORT. */
  uint16_t port;

  /** Stats prefix, defaults to "rocketeer.". */
  std::string stats_prefix;
};

class Rocketeer {
 public:
  Rocketeer();
  virtual ~Rocketeer() = default;

  /**
   * Notifies about new inbound subscription.
   *
   * @param inbound_id Globally unique ID of this subscription.
   * @param params Parameters of the subscription provided by the subscriber.
   */
  virtual void HandleNewSubscription(InboundID inbound_id,
                                     SubscriptionParameters params) = 0;

  /**
   * Notifies that given subscription was terminated by the user.
   *
   * @param inbound_id ID of the subscription to be terminated.
   */
  virtual void HandleTermination(InboundID inbound_id) = 0;

  /**
   * Sends a message on given subscription.
   *
   * @param inbound_id ID of the subscription to send message on.
   * @param seqno Sequence number of the message.
   * @param payload Payload of the message.
   * @param msg_id The ID of the message.
   * @return true iff operation was successfully sheduled.
   */
  bool Deliver(InboundID inbound_id,
               SequenceNumber seqno,
               std::string payload,
               MsgId msg_id = MsgId());

  /**
   * Advances next expected sequence number on a subscription without sending
   * data on it. Client might be notified, so that the next time it
   * resubscribes, it will send advanced sequence number as a part of
   * subscription parameters.
   *
   * @param inbound_id ID of the subscription to advance.
   * @param seqno The subscription will be advanced, so that it expects the next
   *              sequence number.
   * @return true iff operation was successfully sheduled.
   */
  bool Advance(InboundID inbound_id, SequenceNumber seqno);

  /**
   * Terminates given subscription.
   *
   * @param inbound_id ID of the subscription to terminate.
   * @param reason A reason why this subscription was terminated.
   * @return true iff operation was successfully sheduled.
   */
  bool Terminate(InboundID inbound_id, MessageUnsubscribe::Reason reason);

  /** Returns the server that this Rocketeer is registered with. */
  RocketeerServer* GetServer() { return server_; }

  /** Returns a numeric ID of this Rocketeer within the server that runs it. */
  size_t GetID() const;

 private:
  friend class RocketeerServer;

  struct Stats {
    explicit Stats(const std::string& prefix);

    Counter* subscribes;
    Counter* unsubscribes;
    Counter* inbound_subscriptions;
    Counter* dropped_reordered;
    Statistics all;
  };

  ThreadCheck thread_check_;
  // RocketeerServer, which owns both the implementation and this object.
  RocketeerServer* server_;
  // An ID assigned by the server.
  size_t id_;
  std::unique_ptr<Stats> stats_;

  using SubscriptionsOnStream =
      std::unordered_map<SubscriptionID, InboundSubscription>;
  std::unordered_map<StreamID, SubscriptionsOnStream> inbound_subscriptions_;

  void Initialize(RocketeerServer* server, size_t id);

  const Statistics& GetStatisticsInternal();

  InboundSubscription* Find(const InboundID& inbound_id);

  void Receive(std::unique_ptr<MessageSubscribe> subscribe, StreamID origin);

  void Receive(std::unique_ptr<MessageUnsubscribe> unsubscribe,
               StreamID origin);

  void Receive(std::unique_ptr<MessageGoodbye> goodbye, StreamID origin);
};

class RocketeerServer {
 public:
  explicit RocketeerServer(RocketeerOptions options);

  ~RocketeerServer();

  /**
   * Registers provided Rocketeer in the server and returns it's ID. Provided
   * pointer will be used until Stop() method is called, therefor it's life time
   * must be extended until that time.
   *
   * @param rocketeer A Rocketeer implementation to be registered.
   * @return An ID assigned sequentially starting from 0.
   */
  size_t Register(Rocketeer* rocketeer);

  /**
   * Launches Rocketeer threads and starts the loop.
   *
   * @return Status::OK() iff successfully started.
   */
  Status Start();

  /** Stops the server, no thread is running after this method returns. */
  void Stop();

  /** Returns server-wide statistics. */
  Statistics GetStatisticsSync();

  MsgLoop* GetMsgLoop() { return msg_loop_.get(); }

 private:
  friend class Rocketeer;

  RocketeerOptions options_;
  std::unique_ptr<MsgLoop> msg_loop_;
  std::unique_ptr<MsgLoopThread> msg_loop_thread_;
  std::vector<Rocketeer*> rocketeers_;

  template <typename M>
  std::function<void(std::unique_ptr<Message>, StreamID)> CreateCallback();
};

}  // namespace rocketspeed

namespace std {
template <>
struct hash<rocketspeed::InboundID> {
  size_t operator()(const rocketspeed::InboundID& x) const { return x.Hash(); }
};
}  // namespace std
