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

#include "Rocketeer.h"
#include "Types.h"

namespace rocketspeed {

class BaseEnv;
class CommunicationRocketeer;
class Flow;
class HostId;
class Message;
class MsgLoop;
class MsgLoopThread;
class Logger;
class RocketeerServer;
class Statistics;

/** Options for creating RocketeerServer. */
struct RocketeerOptions {
  static constexpr uint16_t DEFAULT_PORT = 58200;

  /** Fills in default values for parameters. */
  RocketeerOptions();

  /** Environment, defaults to Env::Default(). */
  BaseEnv* env;

  /** Logger, defaults to stderr. */
  std::shared_ptr<Logger> info_log;

  /** Port to listen on, defaults to RocketeerServer::DEFAULT_PORT. */
  uint16_t port;

  /** Stats prefix, defaults to "rocketeer.". */
  std::string stats_prefix;

  /** Send heartbeats every X milliseconds. Default is once a minute.
   * Set to zero to disable. */
  std::chrono::milliseconds heartbeat_period = std::chrono::seconds(60);

  /** Set to true to use heartbeat delta encoding. */
  bool use_heartbeat_deltas = false;

  /** Queue size for event loop. */
  size_t queue_size = 1000;

  /** If socket is unwriteable for this amount of time, it will be closed. */
  std::chrono::milliseconds socket_timeout{10000};

  /**
   * Throttle deliveries to the client at the rate specified by the policy
   * The rate is defined as rate_limit / rate_duration.
   * The rate limit is per stream. In the future these paramaters would be
   * provided by the streams when they introduce themselves.
   */
  bool enable_throttling = false;
  size_t rate_limit = 10000;
  std::chrono::milliseconds rate_duration{1000};

  /**
   * Batch deliveries to the client specified by the policy
   * The server will batch as many messages together with maximum being the
   * batch limit until the batching duration times out.
   * The batching is done per stream. In the future these paramaters would be
   * provided by the streams when they introduce themselves.
   */
  bool enable_batching = false;
  size_t batch_max_limit = 100;
  std::chrono::milliseconds batch_max_duration{10};

  /**
   * If true, Rocketeers will automatically receive HandleTerminate calls for
   * all affected subscriptions when a stream disconnects. This makes Rocketeer
   * implementation easier, but requires that the RocketeerServer keep track
   * of subscriptions, which consumes a lot of memory if a large number of
   * subscriptions are expected.
   *
   * If the implementation is able to handle clean up themselves, then this
   * flag can be set to false. It is then the responsibility of the
   * implementation to terminate all subscriptions when HandleDisconnect is
   * invoked.
   */
  bool terminate_on_disconnect = true;
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

  /**
   * A thread-safe version of Rocketeer::Deliver.
   *
   * @return true iff operation was successfully scheduled.
   */
  bool Deliver(InboundID inbound_id,
               NamespaceID namespace_id,
               Topic topic,
               SequenceNumber seqno,
               std::string payload,
               MsgId msg_id = MsgId());

  /**
   * A thread-safe version of Rocketeer::DeliverBatch.
   *
   * @return true iff operation was successfully scheduled.
   */
  bool DeliverBatch(StreamID stream_id,
                    int worker_id,
                    std::vector<RocketeerMessage> messages);

  /**
   * A thread-safe version of Rocketeer::Advance.
   *
   * @return true iff operation was successfully scheduled.
   */
  bool Advance(InboundID inbound_id,
               NamespaceID namespace_id,
               Topic topic,
               SequenceNumber seqno);

  /**
   * A thread-safe version of Rocketeer::NotifyDataLoss.
   *
   * @return true iff operation was successfully scheduled.
   */
  bool NotifyDataLoss(InboundID inbound_id,
                      NamespaceID namespace_id,
                      Topic topic,
                      SequenceNumber seqno);

  /**
   * A thread-safe version of Rocketeer::Unsubscribe.
   *
   * @return true iff operation was successfully scheduled.
   */
  bool Unsubscribe(InboundID inbound_id,
                   NamespaceID namespace_id,
                   Topic topic,
                   Rocketeer::UnsubscribeReason reason);

  /**
   * A thread-safe version of Rocketeer::HasMessageSinceResponse.
   *
   * @return true iff operation was successfully scheduled.
   */
  bool HasMessageSinceResponse(
      InboundID inbound_id, NamespaceID namespace_id, Topic topic, Epoch epoch,
      SequenceNumber seqno, HasMessageSinceResult response, std::string info);

  /** Returns server-wide statistics. */
  // DEPRECATED
  Statistics GetStatisticsSync() const;

  /**
   * Walks over all statistics using the provided StatisticsVisitor.
   *
   * @param visitor Used to visit all statistics maintained by the client.
   */
  void ExportStatistics(StatisticsVisitor* visitor) const;

  int GetWorkerID(const InboundID& inbound_id) const;

  MsgLoop* GetMsgLoop() { return msg_loop_.get(); }

  /** Return host ID of this Rocketeer server */
  const HostId& GetHostId() const;

 private:
  friend class CommunicationRocketeer;

  RocketeerOptions options_;
  std::unique_ptr<MsgLoop> msg_loop_;
  std::unique_ptr<MsgLoopThread> msg_loop_thread_;
  std::vector<std::unique_ptr<CommunicationRocketeer>> rocketeers_;

  template <typename M>
  std::function<void(Flow*, std::unique_ptr<Message>, StreamID)>
  CreateCallback();
};

}  // namespace rocketspeed
