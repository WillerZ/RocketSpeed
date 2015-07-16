// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <map>
#include <random>
#include <thread>
#include <vector>
#include "src/messages/serializer.h"
#include "src/messages/commands.h"
#include "src/messages/messages.h"
#include "src/messages/msg_loop.h"
#include "src/util/auto_roll_logger.h"
#include "src/util/logging.h"
#include "src/util/log_buffer.h"
#include "src/util/storage.h"
#include "src/util/common/object_pool.h"
#include "src/pilot/options.h"

namespace rocketspeed {

class Pilot;

// Storage for captured objects in the append callback.
struct AppendClosure : public PooledObject<AppendClosure> {
 public:
  AppendClosure(Pilot* pilot,
                std::unique_ptr<MessageData> msg,
                LogID logid,
                uint64_t now,
                int worker_id,
                StreamID origin)
  : pilot_(pilot)
  , msg_(std::move(msg))
  , logid_(logid)
  , append_time_(now)
  , worker_id_(worker_id)
  , origin_(origin) {
  }

  void operator()(Status append_status, SequenceNumber seqno);

  void Invoke(Status append_status, SequenceNumber seqno);

 private:
  Pilot* pilot_;
  std::unique_ptr<MessageData> msg_;
  LogID logid_;
  uint64_t append_time_;
  int worker_id_;
  StreamID origin_;
};

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
  const HostId& GetHostId() const {
    return options_.msg_loop->GetHostId();
  }

  Statistics GetStatisticsSync() const;

  // Gets information about the running service.
  std::string GetInfoSync(std::vector<std::string> args);

  void AppendCallback(Status append_status,
                      SequenceNumber seqno,
                      std::unique_ptr<MessageData> msg,
                      LogID logid,
                      uint64_t append_time,
                      int worker_id,
                      StreamID origin);

  MsgLoop* GetMsgLoop() {
    return options_.msg_loop;
  }

 private:
  friend struct AppendClosure;

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
               int worker_id,
               StreamID origin);

  // The options used by the Pilot
  PilotOptions options_;

  // Interface with LogDevice
  std::shared_ptr<LogStorage> log_storage_;

  struct alignas(CACHE_LINE_SIZE) WorkerData {
    WorkerData(unsigned int seed)
    : append_closure_pool_(new SharedPooledObjectList<AppendClosure>())
    , prng_(seed) {
    }

    // AppendClosure object pool and lock.
    std::unique_ptr<SharedPooledObjectList<AppendClosure>> append_closure_pool_;
    Stats stats_;
    std::mt19937 prng_;
  };

  // Per-thread data.
  std::vector<WorkerData> worker_data_;

  // private Constructor
  explicit Pilot(PilotOptions options);

  // Sanitize input options if necessary
  PilotOptions SanitizeOptions(PilotOptions options);

  // callbacks to process incoming messages
  void ProcessPublish(std::unique_ptr<Message> msg, StreamID origin);

  std::map<MessageType, MsgCallbackType> InitializeCallbacks();
};

}  // namespace rocketspeed
