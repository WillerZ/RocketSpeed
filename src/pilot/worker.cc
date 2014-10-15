// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/pilot/worker.h"
#include <vector>
#include "include/Status.h"
#include "include/Types.h"
#include "src/messages/msg_client.h"
#include "src/util/storage.h"

namespace rocketspeed {

PilotWorker::PilotWorker(const PilotOptions& options,
                         LogStorage* storage,
                         MsgClient* client)
: worker_loop_(options.worker_queue_size)
, storage_(storage)
, options_(options)
, msg_client_(client) {
  LOG_INFO(options_.info_log, "Created a new PilotWorker");
  options_.info_log->Flush();
}

void PilotWorker::Run() {
  LOG_INFO(options_.info_log, "Starting worker loop");
  worker_loop_.Run([this](PilotWorkerCommand command) {
    CommandCallback(std::move(command));
  });
}

bool PilotWorker::Forward(LogID logid, std::unique_ptr<MessageData> msg) {
  return worker_loop_.Send(logid, std::move(msg));
}

void PilotWorker::CommandCallback(PilotWorkerCommand command) {
  // Process PilotWorkerCommand
  MessageData* msg_raw = command.ReleaseMessage();
  assert(msg_raw);
  LogID logid = command.GetLogID();

  // Setup AppendCallback
  auto append_callback = [this, msg_raw, logid] (Status append_status) {
    std::unique_ptr<MessageData> msg(msg_raw);
    if (append_status.ok()) {
      // Append successful, send success ack.
      SendAck(msg->GetTenantID(),
              msg->GetOrigin(),
              msg->GetMessageId(),
              MessageDataAck::AckStatus::Success);
      LOG_INFO(options_.info_log,
          "Appended (%.16s) successfully to Topic(%s) in log %lu",
          msg_raw->GetPayload().ToString().c_str(),
          msg_raw->GetTopicName().ToString().c_str(),
          logid);
    } else {
      // Append failed, send failure ack.
      LOG_WARN(options_.info_log,
          "AppendAsync failed (%s)",
          append_status.ToString().c_str());
      options_.info_log->Flush();
      SendAck(msg->GetTenantID(),
              msg->GetOrigin(),
              msg->GetMessageId(),
              MessageDataAck::AckStatus::Failure);
    }
  };

  // Asynchronously append to log storage.
  auto status = storage_->AppendAsync(logid,
                                      msg_raw->SerializeStorage(),
                                      append_callback);
  // TODO(pja) 1: Technically there is no need to re-serialize the message.
  // If we keep the wire-serialized form that we received the message in then
  // the SerializeLogStorage slice is just a sub-slice of that format.
  // This is an optimization though, so it can wait.

  if (!status.ok()) {
    // Append call failed, log and send failure ack.
    LOG_WARN(options_.info_log,
      "Failed to append to log ID %lu",
      static_cast<uint64_t>(logid));
    options_.info_log->Flush();

    // Subtle note: msg_raw is actually owned by append_callback at this point,
    // but because the append failed, it will never be called, so we own msg_raw
    // here, and it is our responsibility to delete it.
    std::unique_ptr<MessageData> msg(msg_raw);
    SendAck(msg->GetTenantID(),
            msg->GetOrigin(),
            msg->GetMessageId(),
            MessageDataAck::AckStatus::Failure);
  }
}

void PilotWorker::SendAck(const TenantID tenantid,
                          const HostId& host,
                          const MsgId& msgid,
                          MessageDataAck::AckStatus status) {
  MessageDataAck::Ack ack;
  ack.status = status;
  ack.msgid = msgid;

  std::vector<MessageDataAck::Ack> acks = { ack };
  MessageDataAck msg(tenantid, host, acks);

  Status st = msg_client_->Send(host, &msg);
  if (!st.ok()) {
    // This is entirely possible, other end may have disconnected by the time
    // we get round to sending an ack. This shouldn't be a rare occurrence.
    LOG_INFO(options_.info_log,
      "Failed to send ack to %s",
      host.hostname.c_str());
  }
}

}  // namespace rocketspeed
