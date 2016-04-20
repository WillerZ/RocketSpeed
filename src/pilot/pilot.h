// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <random>
#include <vector>
#include "src/messages/messages.h"
#include "src/messages/stream_socket.h"
#include "src/util/common/statistics.h"
#include "src/port/port.h"
#include "src/pilot/options.h"

namespace rocketspeed {

class AppendClosure;
class AppendResponse;
class Logger;
class LogStorage;
class Message;
class MsgLoop;
class Pilot;
template <typename> class SharedPooledObjectList;
template <typename> class ThreadLocalQueues;

class Pilot {
 public:
  static const int DEFAULT_PORT = 58600;

  // A new instance of a Pilot
  static Status CreateNewInstance(PilotOptions options,
                                  Pilot** pilot);
  ~Pilot();

  void Stop();

  // Returns the sanitized options used by the pilot
  PilotOptions& GetOptions() { return options_; }

  // Get HostID
  const HostId& GetHostId() const;

  Statistics GetStatisticsSync() const;

  // Gets information about the running service.
  std::string GetInfoSync(std::vector<std::string> args);

  void AppendCallback(Status append_status,
                      SequenceNumber seqno,
                      std::unique_ptr<MessageData> msg,
                      LogID logid,
                      StreamID origin);

  MsgLoop* GetMsgLoop() {
    return options_.msg_loop;
  }

 private:
  friend class AppendClosure;

  struct Stats {
    Stats() {
      append_latency = all.AddLatency("pilot.append_latency_us");
      append_requests = all.AddCounter("pilot.append_requests");
      failed_appends = all.AddCounter("pilot.failed_appends");

      FAULT_corrupt_writes = all.AddCounter("pilot.FAULT_corrupt_writes");
    }

    Statistics all;

    // Latency of append request -> response.
    Histogram* append_latency;

    // Number of append requests received.
    Counter* append_requests;

    // Number of append failures.
    Counter* failed_appends;

    // Number of written corrupt records through fault injection.
    Counter* FAULT_corrupt_writes;
  };

  // Send an ack message to the host for the msgid.
  void SendAck(MessageData* msg,
               SequenceNumber seqno,
               MessageDataAck::AckStatus status,
               StreamID origin);

  // The options used by the Pilot
  PilotOptions options_;

  // Interface with LogDevice
  std::shared_ptr<LogStorage> log_storage_;

  struct alignas(CACHE_LINE_SIZE) WorkerData {
    WorkerData(MsgLoop* msg_loop, int worker_id, Pilot* pilot);

    // AppendClosure object pool and lock.
    std::unique_ptr<SharedPooledObjectList<AppendClosure>> append_closure_pool_;
    Stats stats_;
    std::mt19937_64& prng_;
    std::shared_ptr<ThreadLocalQueues<AppendResponse>> append_response_queues_;
  };

  // Per-thread data.
  std::vector<std::unique_ptr<WorkerData>> worker_data_;

  // private Constructor
  explicit Pilot(PilotOptions options);

  // Sanitize input options if necessary
  PilotOptions SanitizeOptions(PilotOptions options);

  // callbacks to process incoming messages
  void ProcessPublish(std::unique_ptr<Message> msg, StreamID origin);
};

}  // namespace rocketspeed
