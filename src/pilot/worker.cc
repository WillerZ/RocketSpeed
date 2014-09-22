// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/pilot/worker.h"
#include <vector>
#include "include/Status.h"
#include "include/Types.h"
#include "src/messages/commands.h"

namespace rocketspeed {

// This command instructs a pilot worker to append to the log storage, and
// then send an ack on completion.
class PilotWorkerCommand : public Command {
 public:
  PilotWorkerCommand(LogID logid, std::unique_ptr<MessageData> msg)
  : logid_(logid)
  , msg_(std::move(msg)) {
  }

  // Get the log ID to append to.
  LogID GetLogID() const {
    return logid_;
  }

  // Releases ownership of the message and returns it.
  MessageData* ReleaseMessage() {
    return msg_.release();
  }

 private:
  LogID logid_;
  std::unique_ptr<MessageData> msg_;
};

PilotWorker::PilotWorker(const PilotOptions& options,
                         LogStorage* storage)
: storage_(storage)
, options_(options) {
  // Setup command callback.
  auto command_callback = [this] (std::unique_ptr<Command> command) {
    CommandCallback(std::move(command));
  };

  // Create message loop (not listening for connections).
  msg_loop_.reset(new MsgLoop(options.env,
                              options.env_options,
                              HostId("", -1),
                              options.info_log,
                              static_cast<ApplicationCallbackContext>(this),
                              callbacks_,
                              command_callback));

  Log(InfoLogLevel::INFO_LEVEL, options_.info_log,
    "Created a new PilotWorker");
  options_.info_log->Flush();
}

void PilotWorker::Forward(LogID logid, std::unique_ptr<MessageData> msg) {
  std::unique_ptr<Command> cmd(new PilotWorkerCommand(logid, std::move(msg)));
  msg_loop_->SendCommand(std::move(cmd));
}

void PilotWorker::CommandCallback(std::unique_ptr<Command> command) {
  // Process PilotWorkerCommand
  assert(command);
  PilotWorkerCommand* cmd = static_cast<PilotWorkerCommand*>(command.get());
  MessageData* msg_raw = cmd->ReleaseMessage();
  assert(msg_raw);

  // Setup AppendCallback
  auto append_callback = [this, msg_raw] (Status append_status) {
    std::unique_ptr<MessageData> msg(msg_raw);
    if (append_status.ok()) {
      // Append successful, send success ack.
      SendAck(msg->GetOrigin(),
              msg->GetMessageId(),
              MessageDataAck::AckStatus::Success);

    } else {
      // Append failed, send failure ack.
      SendAck(msg->GetOrigin(),
              msg->GetMessageId(),
              MessageDataAck::AckStatus::Failure);
    }
  };

  // Asynchronously append to log storage.
  LogID logid = cmd->GetLogID();
  auto status = storage_->AppendAsync(logid,
                                      msg_raw->SerializeStorage(),
                                      append_callback);
  // TODO(pja) 1: Technically there is no need to re-serialize the message.
  // If we keep the wire-serialized form that we received the message in then
  // the SerializeLogStorage slice is just a sub-slice of that format.
  // This is an optimization though, so it can wait.

  if (!status.ok()) {
    // Append call failed, log and send failure ack.
    Log(InfoLogLevel::WARN_LEVEL, options_.info_log,
      "Failed to append to log ID %lu",
      static_cast<uint64_t>(logid));

    // Subtle note: msg_raw is actually owned by append_callback at this point,
    // but because the append failed, it will never be called, so we own msg_raw
    // here, and it is our responsibility to delete it.
    std::unique_ptr<MessageData> msg(msg_raw);
    SendAck(msg->GetOrigin(),
            msg->GetMessageId(),
            MessageDataAck::AckStatus::Failure);
  }
}

void PilotWorker::SendAck(const HostId& host,
                          const MsgId& msgid,
                          MessageDataAck::AckStatus status) {
  MessageDataAck::Ack ack;
  ack.status = status;
  ack.msgid = msgid;

  std::vector<MessageDataAck::Ack> acks = { ack };
  MessageDataAck msg(acks);

  Status st = msg_loop_->GetClient().Send(host, &msg);
  if (!st.ok()) {
    // This is entirely possible, other end may have disconnected by the time
    // we get round to sending an ack. This shouldn't be a rare occurrence.
    Log(InfoLogLevel::INFO_LEVEL, options_.info_log,
      "Failed to send ack to %s",
      host.hostname.c_str());
  }
}

}  // namespace rocketspeed
