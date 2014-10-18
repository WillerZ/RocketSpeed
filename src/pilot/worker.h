// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <map>
#include <memory>
#include "src/messages/commands.h"
#include "src/messages/messages.h"
#include "src/pilot/options.h"
#include "src/util/worker_loop.h"

namespace rocketspeed {

class LogStorage;
class Pilot;

// This command instructs a pilot worker to append to the log storage, and
// then send an ack on completion.
class PilotWorkerCommand {
 public:
  PilotWorkerCommand() = default;

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

// These Commands sent from the Worker to the Pilot
class PilotCommand : public Command {
 public:
  PilotCommand(std::string message, const HostId& host):
    message_(std::move(message)) {
    recipient_.push_back(host);
  }
  void GetMessage(std::string* out) {
    out->assign(std::move(message_));
  }
  // return the Destination HostId, otherwise returns null.
  const std::vector<HostId>& GetDestination() const {
    return recipient_;
  }
  bool IsSendCommand() const  {
    return true;
  }
 private:
  std::vector<HostId> recipient_;
  std::string message_;
};

/**
 * Pilot worker object. The pilot will allocate several of these, ideally one
 * per hardware thread. The workers take load off of the main thread by handling
 * the log appends and ack sending, and allows us to scale to multiple cores.
 */
class PilotWorker {
 public:
  // Constructs a new PilotWorker (does not start a thread).
  PilotWorker(const PilotOptions& options,
              LogStorage* storage,
              Pilot* pilot);

  // Forward a message to this worker for processing.
  // This will asynchronously append the message into the log storage,
  // and then send an ack back to the to the message origin.
  bool Forward(LogID logid, std::unique_ptr<MessageData> msg);

  // Start the worker loop on this thread.
  // Blocks until the worker loop ends.
  void Run();

  // Stop the worker loop.
  void Stop() {
    worker_loop_.Stop();
  }

  // Check if the worker loop is running.
  bool IsRunning() const {
    return worker_loop_.IsRunning();
  }

 private:
  // Callback for worker loop commands.
  void CommandCallback(PilotWorkerCommand command);

  // Send an ack message to the host for the msgid.
  void SendAck(const TenantID tenantid,
               const HostId& host,
               const MsgId& msgid,
               MessageDataAck::AckStatus status);

  WorkerLoop<PilotWorkerCommand> worker_loop_;
  LogStorage* storage_;
  const PilotOptions& options_;
  Pilot* pilot_;
};

}  // namespace rocketspeed
