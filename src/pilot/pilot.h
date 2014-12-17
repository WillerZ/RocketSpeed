// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <map>
#include <thread>
#include <vector>
#include "src/messages/serializer.h"
#include "src/messages/commands.h"
#include "src/messages/messages.h"
#include "src/messages/msg_loop.h"
#include "src/util/auto_roll_logger.h"
#include "src/util/logging.h"
#include "src/util/log_buffer.h"
#include "src/util/log_router.h"
#include "src/util/storage.h"
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
                int worker_id)
  : pilot_(pilot)
  , msg_(std::move(msg))
  , logid_(logid)
  , append_time_(now)
  , worker_id_(worker_id) {
  }

  void operator()(Status append_status, SequenceNumber seqno);

  void Invoke(Status append_status, SequenceNumber seqno);

 private:
  Pilot* pilot_;
  std::unique_ptr<MessageData> msg_;
  LogID logid_;
  uint64_t append_time_;
  int worker_id_;
};

class Pilot {
 public:
  static const int DEFAULT_PORT = 58600;

  // A new instance of a Pilot
  static Status CreateNewInstance(PilotOptions options,
                                  Pilot** pilot);
  virtual ~Pilot();

  // Returns the sanitized options used by the pilot
  PilotOptions& GetOptions() { return options_; }

  // Get HostID
  const HostId& GetHostId() const {
    return options_.msg_loop->GetHostId();
  }

  // Get the client id of a worker thread on this pilot
  const ClientID& GetClientId(int worker_id) const {
    return options_.msg_loop->GetClientId(worker_id);
  }

  Statistics GetStatistics() const;

  void AppendCallback(Status append_status,
                      SequenceNumber seqno,
                      std::unique_ptr<MessageData> msg,
                      LogID logid,
                      uint64_t append_time,
                      int worker_id);

 private:
  friend struct AppendClosure;

  struct Stats {
    Stats() {
      append_latency = all.AddLatency("rocketspeed.pilot.append_latency_us");
      append_requests = all.AddCounter("rocketspeed.pilot.append_requests");
      failed_appends = all.AddCounter("rocketspeed.pilot.failed_appends");
    }

    Statistics all;

    // Latency of append request -> response.
    Histogram* append_latency;

    // Number of append requests received.
    Counter* append_requests;

    // Number of append failures.
    Counter* failed_appends;
  };

  // Send an ack message to the host for the msgid.
  void SendAck(MessageData* msg,
               SequenceNumber seqno,
               MessageDataAck::AckStatus status,
               int worker_id);

  // The options used by the Pilot
  PilotOptions options_;

  // Interface with LogDevice
  std::shared_ptr<LogStorage> log_storage_;

  // Log router for mapping topic names to logs.
  LogRouter log_router_;

  struct alignas(CACHE_LINE_SIZE) WorkerData {
    WorkerData()
    : append_closure_pool_(new SharedPooledObjectList<AppendClosure>()) {
    }

    // AppendClosure object pool and lock.
    std::unique_ptr<SharedPooledObjectList<AppendClosure>> append_closure_pool_;
    Stats stats_;
  };

  // Per-thread data.
  std::vector<WorkerData> worker_data_;

  // private Constructor
  explicit Pilot(PilotOptions options);

  // Sanitize input options if necessary
  PilotOptions SanitizeOptions(PilotOptions options);

  // callbacks to process incoming messages
  void ProcessPublish(std::unique_ptr<Message> msg);

  std::map<MessageType, MsgCallbackType> InitializeCallbacks();
};

}  // namespace rocketspeed
