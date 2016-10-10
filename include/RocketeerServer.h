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

class Env;
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
  Env* env;

  /** Logger, defaults to stderr. */
  std::shared_ptr<Logger> info_log;

  /** Port to listen on, defaults to RocketeerServer::DEFAULT_PORT. */
  uint16_t port;

  /** Stats prefix, defaults to "rocketeer.". */
  std::string stats_prefix;

  /** Send heartbeats every X milliseconds. Default is once a minute.
   * Set to zero to disable. */
  std::chrono::milliseconds heartbeat_period = std::chrono::seconds(60);
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
  bool Advance(InboundID inbound_id, SequenceNumber seqno);

  /** A thread-safe version of Rocketeer::.
   *
   * @return true iff operation was successfully scheduled.
   */
  bool Terminate(InboundID inbound_id, Rocketeer::UnsubscribeReason reason);

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
