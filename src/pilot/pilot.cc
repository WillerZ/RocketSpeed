//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/pilot/pilot.h"
#include <map>
#include <string>
#include <thread>
#include <vector>
#include "src/util/storage.h"
#include "src/util/memory.h"

namespace rocketspeed {

void AppendClosure::operator()(Status append_status, SequenceNumber seqno) {
  // Record latency
  uint64_t latency = pilot_->options_.env->NowMicros() - append_time_;
  pilot_->worker_data_[worker_id_].stats_.append_latency->Record(latency);
  pilot_->worker_data_[worker_id_].stats_.append_requests->Add(1);
  pilot_->AppendCallback(append_status,
                         seqno,
                         std::move(msg_),
                         logid_,
                         append_time_,
                         worker_id_);
  pilot_->worker_data_[worker_id_].append_closure_pool_->Deallocate(this);
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
      options.info_log = nullptr;
    }
  }

  return std::move(options);
}

/**
 * Private constructor for a Pilot
 */
Pilot::Pilot(PilotOptions options):
  options_(SanitizeOptions(std::move(options))),
  log_router_(options_.log_range.first, options_.log_range.second) {

  worker_data_.resize(options_.msg_loop->GetNumWorkers());
  log_storage_ = options_.storage;
  options_.msg_loop->RegisterCallbacks(InitializeCallbacks());

  LOG_INFO(options_.info_log, "Created a new Pilot");
  options_.info_log->Flush();
}

Pilot::~Pilot() {
  options_.info_log->Flush();
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
void Pilot::ProcessPublish(std::unique_ptr<Message> msg) {
  // Sanity checks.
  assert(msg);
  assert(msg->GetMessageType() == MessageType::mPublish);

  int worker_id = options_.msg_loop->GetThreadWorkerIndex();

  // Route topic to log ID.
  MessageData* msg_data = static_cast<MessageData*>(msg.release());
  LogID logid;
  Slice topic_name = msg_data->GetTopicName();
  if (!log_router_.GetLogID(topic_name, &logid).ok()) {
    assert(false);  // GetLogID should never fail.
    return;
  }

  LOG_INFO(options_.info_log,
      "Received data (%.16s) for Topic(%s)",
      msg_data->GetPayload().ToString().c_str(),
      msg_data->GetTopicName().ToString().c_str());

  // Setup AppendCallback
  uint64_t now = options_.env->NowMicros();
  AppendClosure* closure;
  std::unique_ptr<MessageData> msg_owned(msg_data);
  closure = worker_data_[worker_id].append_closure_pool_->Allocate(
    this,
    std::move(msg_owned),
    logid,
    now,
    worker_id);

  // Asynchronously append to log storage.
  auto append_callback = std::ref(*closure);
  auto status = log_storage_->AppendAsync(logid,
                                          msg_data->GetStorageSlice(),
                                          std::move(append_callback));

  if (!status.ok()) {
    // Append call failed, log and send failure ack.
    worker_data_[worker_id].stats_.failed_appends->Add(1);
    LOG_WARN(options_.info_log,
      "Failed to append to Log(%lu) (%s)",
      static_cast<uint64_t>(logid), status.ToString().c_str());
    options_.info_log->Flush();

    SendAck(msg_data, 0, MessageDataAck::AckStatus::Failure, worker_id);

    // If AppendAsync, the closure will never be invoked, so delete now.
    worker_data_[worker_id].append_closure_pool_->Deallocate(closure);
  }
}

void Pilot::AppendCallback(Status append_status,
                           SequenceNumber seqno,
                           std::unique_ptr<MessageData> msg,
                           LogID logid,
                           uint64_t append_time,
                           int worker_id) {
  if (append_status.ok()) {
    // Append successful, send success ack.
    SendAck(msg.get(), seqno, MessageDataAck::AckStatus::Success, worker_id);
    LOG_INFO(options_.info_log,
        "Appended (%.16s) successfully to Topic(%s) in Log(%lu)@%lu",
        msg->GetPayload().ToString().c_str(),
        msg->GetTopicName().ToString().c_str(),
        logid,
        seqno);
  } else {
    // Append failed, send failure ack.
    worker_data_[worker_id].stats_.failed_appends->Add(1);
    LOG_WARN(options_.info_log,
        "AppendAsync failed (%s)",
        append_status.ToString().c_str());
    options_.info_log->Flush();
    SendAck(msg.get(), 0, MessageDataAck::AckStatus::Failure, worker_id);
  }
}

void Pilot::SendAck(MessageData* msg,
                    SequenceNumber seqno,
                    MessageDataAck::AckStatus status,
                    int worker_id) {
  const bool is_new_request = false;
  MessageDataAck::Ack ack;
  ack.status = status;
  ack.msgid = msg->GetMessageId();
  ack.seqno = seqno;

  // create new message
  const ClientID& client = msg->GetOrigin();
  MessageDataAck newmsg(msg->GetTenantID(), client, { ack });
  // serialize message
  std::string serial;
  newmsg.SerializeToString(&serial);
  // send message
  std::unique_ptr<Command> cmd(
    new SerializedSendCommand(std::move(serial),
                              client,
                              options_.env->NowMicros(),
                              is_new_request));
  Status st = options_.msg_loop->SendCommand(std::move(cmd), worker_id);
  if (!st.ok()) {
    // This is entirely possible, other end may have disconnected by the time
    // we get round to sending an ack. This shouldn't be a rare occurrence.
    LOG_INFO(options_.info_log,
      "Failed to send ack to %s",
      client.c_str());
  }
}

// A static method to initialize the callback map
std::map<MessageType, MsgCallbackType> Pilot::InitializeCallbacks() {
  // create a temporary map and initialize it
  std::map<MessageType, MsgCallbackType> cb;
  cb[MessageType::mPublish] = [this] (std::unique_ptr<Message> msg) {
    ProcessPublish(std::move(msg));
  };

  // return the updated map
  return cb;
}

Statistics Pilot::GetStatistics() const {
  Statistics aggr;
  for (const WorkerData& data : worker_data_) {
    aggr.Aggregate(data.stats_.all);
  }
  return aggr;
}

}  // namespace rocketspeed
