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
#include "src/engine/rocketeer.h"
#include "src/messages/messages.h"
#include "src/messages/stream_socket.h"
#include "src/util/common/base_env.h"
#include "src/util/common/hash.h"
#include "src/util/common/statistics.h"
#include "src/util/common/thread_check.h"

namespace rocketspeed {

class CommunicationRocketeer;
class Flow;
class MsgLoop;
class MsgLoopThread;
class Logger;
class Rocketeer;
class RocketeerServer;

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
   * @return true iff operation was successfully sheduled.
   */
  bool Deliver(InboundID inbound_id,
               SequenceNumber seqno,
               std::string payload,
               MsgId msg_id = MsgId());

  /**
   * A thread-safe version of Rocketeer::Advance.
   *
   * @return true iff operation was successfully sheduled.
   */
  bool Advance(InboundID inbound_id, SequenceNumber seqno);

  /** A thread-safe version of Rocketeer::.
   *
   * @return true iff operation was successfully sheduled.
   */
  bool Terminate(InboundID inbound_id, MessageUnsubscribe::Reason reason);

  /** Returns server-wide statistics. */
  Statistics GetStatisticsSync();

  MsgLoop* GetMsgLoop() { return msg_loop_.get(); }

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
