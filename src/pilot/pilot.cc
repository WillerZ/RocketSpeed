//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "src/pilot/pilot.h"
#include <map>
#include <string>
#include <thread>
#include <vector>
#include "src/messages/msg_loop.h"
#include "src/messages/queues.h"
#include "src/util/common/flow_control.h"
#include "src/util/common/object_pool.h"
#include "src/util/common/processor.h"
#include "src/util/common/random.h"
#include "src/util/auto_roll_logger.h"
#include "src/util/storage.h"
#include "src/util/memory.h"

namespace rocketspeed {

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

struct AppendResponse {
  Status status;
  SequenceNumber seqno;
  std::unique_ptr<MessageData> msg;
  LogID log_id;
  uint64_t latency;
  StreamID origin;
  AppendClosure* closure;
};

void AppendClosure::operator()(Status append_status, SequenceNumber seqno) {
  // IMPORTANT: This may be called after Stop(). Must not use the log storage
  // or log router after this point.
  LOG_INFO(pilot_->options_.info_log,
      "Append response %d",
      worker_id_);

  AppendResponse response;
  response.status = append_status;
  response.seqno = seqno;
  response.msg = std::move(msg_);
  response.log_id = logid_;
  response.latency = pilot_->options_.env->NowMicros() - append_time_;
  response.origin = origin_;
  response.closure = this;

  // Send response back to relevant worker.
  Pilot* pilot = pilot_;
  int worker_id = worker_id_;
  auto queue =
    pilot->worker_data_[worker_id]->append_response_queues_->GetThreadLocal();
  if (!queue->Write(response)) {
    // Note: the command has still been sent in this case, but memory usage
    // is unbounded if the flow control is not working.
    LOG_ERROR(pilot->options_.info_log,
      "Append response queue for worker %d is overflowing",
      worker_id);

    // Flow control mechanism should ensure that this queue never overflows.
    assert(false);
  } else {
    LOG_INFO(pilot->options_.info_log,
      "Wrote response to queue %d",
      worker_id);
  }
}

/**
 * Sanitize user-specified options
 */
PilotOptions Pilot::SanitizeOptions(PilotOptions options) {
  if (options.info_log == nullptr) {
    Status s = CreateLoggerFromOptions(options.env,
                                       options.log_dir,
                                       "LOG.pilot",
                                       options.log_file_time_to_roll,
                                       options.max_log_file_size,
                                       options.info_log_level,
                                       &options.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      options.info_log = std::make_shared<NullLogger>();
    }
  }

  return std::move(options);
}

/**
 * Private constructor for a Pilot
 */
Pilot::Pilot(PilotOptions options):
  options_(SanitizeOptions(std::move(options))) {

  for (int i = 0; i < options_.msg_loop->GetNumWorkers(); ++i) {
    worker_data_.emplace_back(new WorkerData(options_.msg_loop, i, this));
  }
  log_storage_ = options_.storage;

  std::map<MessageType, MsgCallbackType> cb {
    { MessageType::mPublish, [this] (std::unique_ptr<Message> msg,
                                     StreamID origin) {
        ProcessPublish(std::move(msg), origin);
    }}};
  options_.msg_loop->RegisterCallbacks(std::move(cb));

  LOG_INFO(options_.info_log, "Created a new Pilot");
  options_.info_log->Flush();
}

Pilot::~Pilot() {
  options_.info_log->Flush();
}

void Pilot::Stop() {
  assert(!options_.msg_loop->IsRunning());  // must stop message loop first
  log_storage_.reset();
  options_.storage.reset();
  options_.log_router.reset();
}

/**
 * This is a static method to create a Pilot
 */
Status Pilot::CreateNewInstance(PilotOptions options,
                                Pilot** pilot) {
  if (!options.msg_loop) {
    assert(false);
    return Status::InvalidArgument("Message loop must be provided");
  }

  if (!options.storage) {
    assert(false);
    return Status::InvalidArgument("Log storage must be provided");
  }

  if (!options.log_router) {
    assert(false);
    return Status::InvalidArgument("Log router must be provided");
  }

  *pilot = new Pilot(std::move(options));

  // Ensure we managed to connect to the log storage.
  if ((*pilot)->log_storage_ == nullptr) {
    delete *pilot;
    *pilot = nullptr;
    return Status::NotInitialized();
  }

  return Status::OK();
}

// A callback method to process MessageData
void Pilot::ProcessPublish(std::unique_ptr<Message> msg, StreamID origin) {
  // Sanity checks.
  assert(msg);
  assert(msg->GetMessageType() == MessageType::mPublish);

  int worker_id = options_.msg_loop->GetThreadWorkerIndex();
  WorkerData& worker_data = *worker_data_[worker_id];

  // Route topic to log ID.
  MessageData* msg_data = static_cast<MessageData*>(msg.release());
  LogID logid;
  if (!options_.log_router->GetLogID(msg_data->GetNamespaceId(),
                                     msg_data->GetTopicName(),
                                     &logid).ok()) {
    assert(false);  // GetLogID should never fail.
    return;
  }

  LOG_INFO(options_.info_log,
      "Received data (%.16s) for Topic(%s,%s) in Log(%" PRIu64 ")",
      msg_data->GetPayload().ToString().c_str(),
      msg_data->GetNamespaceId().ToString().c_str(),
      msg_data->GetTopicName().ToString().c_str(),
      logid);

  // Setup AppendCallback
  uint64_t now = options_.env->NowMicros();
  AppendClosure* closure;
  std::unique_ptr<MessageData> msg_owned(msg_data);
  closure = worker_data.append_closure_pool_->Allocate(
    this,
    std::move(msg_owned),
    logid,
    now,
    worker_id,
    origin);

  // Asynchronously append to log storage.
  auto append_callback = std::ref(*closure);
  auto status = log_storage_->AppendAsync(logid,
                                          msg_data->GetStorageSlice(),
                                          std::move(append_callback));

  // Fault injection: insert corrupt data into the logs.
  if (options_.FAULT_corrupt_extra_probability != 0.0) {
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    if (dist(worker_data.prng_) < options_.FAULT_corrupt_extra_probability) {
      LOG_INFO(options_.info_log,
        "Fault-injection: appending corrupt record into Log(%" PRIu64 ")",
        logid);
      worker_data.stats_.FAULT_corrupt_writes->Add(1);
      log_storage_->AppendAsync(logid, "invalid",
        [] (Status, SequenceNumber) {});
    }
  }

  if (!status.ok()) {
    // Append call failed, log and send failure ack.
    worker_data.stats_.failed_appends->Add(1);
    LOG_ERROR(options_.info_log,
      "Failed to append to Topic(%s,%s) in Log(%" PRIu64 ") (%s)",
      msg_data->GetNamespaceId().ToString().c_str(),
      msg_data->GetTopicName().ToString().c_str(),
      logid,
      status.ToString().c_str());
    options_.info_log->Flush();

    SendAck(msg_data, 0, MessageDataAck::AckStatus::Failure, origin);

    // If AppendAsync, the closure will never be invoked, so delete now.
    worker_data.append_closure_pool_->Deallocate(closure);
  }
}

void Pilot::AppendCallback(Status append_status,
                           SequenceNumber seqno,
                           std::unique_ptr<MessageData> msg,
                           LogID logid,
                           StreamID origin) {
  if (append_status.ok()) {
    // Append successful, send success ack.
    SendAck(msg.get(),
            seqno,
            MessageDataAck::AckStatus::Success,
            origin);
    LOG_INFO(options_.info_log,
        "Appended (%.16s) successfully to Topic(%s,%s) in Log(%" PRIu64
        ")@%" PRIu64,
        msg->GetPayload().ToString().c_str(),
        msg->GetNamespaceId().ToString().c_str(),
        msg->GetTopicName().ToString().c_str(),
        logid,
        seqno);
  } else {
    // Append failed, send failure ack.
    LOG_ERROR(options_.info_log,
        "AppendAsync failed for Topic(%s,%s) in Log(%" PRIu64 ") (%s)",
        msg->GetNamespaceId().ToString().c_str(),
        msg->GetTopicName().ToString().c_str(),
        logid,
        append_status.ToString().c_str());

    // TODO: retry depending on error type.
    SendAck(msg.get(),
            0,
            MessageDataAck::AckStatus::Failure,
            origin);
  }
}

void Pilot::SendAck(MessageData* msg,
                    SequenceNumber seqno,
                    MessageDataAck::AckStatus status,
                    StreamID origin) {
  MessageDataAck::Ack ack;
  ack.status = status;
  ack.msgid = msg->GetMessageId();
  ack.seqno = seqno;

  // create new message
  MessageDataAck newmsg(msg->GetTenantID(), {ack});
  auto cmd = options_.msg_loop->ResponseCommand(newmsg, origin);
  options_.msg_loop->SendCommandToSelf(std::move(cmd));
}

Statistics Pilot::GetStatisticsSync() const {
  auto stats = options_.msg_loop->AggregateStatsSync(
      [this](int i) { return worker_data_[i]->stats_.all; });
  stats.Aggregate(options_.storage->GetStatistics());
  return stats;
}

const HostId& Pilot::GetHostId() const {
  return options_.msg_loop->GetHostId();
}

std::string Pilot::GetInfoSync(std::vector<std::string> args) {
  return "Unknown info for pilot";
}

Pilot::WorkerData::WorkerData(MsgLoop* msg_loop, int worker_id, Pilot* pilot)
: append_closure_pool_(new SharedPooledObjectList<AppendClosure>())
, prng_(ThreadLocalPRNG())
, flow_control_(new FlowControl("pilot", msg_loop->GetEventLoop(worker_id))) {
  // Register processors.
  EventLoop* event_loop = msg_loop->GetEventLoop(worker_id);
  append_response_queues_.reset(
    new ThreadLocalQueues<AppendResponse>(
      [this, pilot, event_loop] () {
        return InstallQueue<AppendResponse>(
          pilot->options_.info_log,
          event_loop->GetQueueStats(),
          1000,
          flow_control_.get(),
          [this, pilot] (Flow* flow, AppendResponse response) {
            // Update statistics.
            stats_.append_latency->Record(response.latency);
            stats_.append_requests->Add(1);
            if (!response.status.ok()) {
              stats_.failed_appends->Add(1);
            }

            pilot->AppendCallback(response.status,
                                  response.seqno,
                                  std::move(response.msg),
                                  response.log_id,
                                  response.origin);
            append_closure_pool_->Deallocate(response.closure);
          });
      }));
}

}  // namespace rocketspeed
