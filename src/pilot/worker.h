// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <map>
#include <memory>
#include "src/messages/messages.h"
#include "src/pilot/options.h"
#include "src/util/worker_loop.h"

namespace rocketspeed {

class LogStorage;
class MsgClient;

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
              MsgClient* client);

  // Forward a message to this worker for processing.
  // This will asynchronously append the message into the log storage,
  // and then send an ack back to the to the message origin.
  bool Forward(LogID logid, std::unique_ptr<MessageData> msg);

  // Start the worker loop on this thread.
  // Blocks until the worker loop ends.
  void Run() {
    worker_loop_.Run([this](PilotWorkerCommand command) {
      CommandCallback(std::move(command));
    });
  }

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
  void SendAck(const HostId& host,
               const MsgId& msgid,
               MessageDataAck::AckStatus status);

  WorkerLoop<PilotWorkerCommand> worker_loop_;
  LogStorage* storage_;
  const PilotOptions& options_;
  MsgClient* msg_client_;
};

}  // namespace rocketspeed
