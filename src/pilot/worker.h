// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <map>
#include <memory>
#include "src/messages/messages.h"
#include "src/messages/msg_loop.h"
#include "src/pilot/options.h"
#include "src/util/storage.h"

namespace rocketspeed {

/**
 * Pilot worker object. The pilot will allocate several of these, ideally one
 * per hardware thread. The workers take load off of the main thread by handling
 * the log appends and ack sending, and allows us to scale to multiple cores.
 */
class PilotWorker {
 public:
  // Constructs a new PilotWorker (does not start a thread).
  PilotWorker(const PilotOptions& options,
              LogStorage* storage);

  // Forward a message to this worker for processing.
  // This will asynchronously append the message into the log storage,
  // and then send an ack back to the to the message origin.
  void Forward(LogID logid, std::unique_ptr<MessageData> msg);

  // Start the message loop on this thread.
  // Blocks until the message loop ends.
  void Run() {
    msg_loop_->Run();
  }

  // Stop the message loop.
  void Stop() {
    msg_loop_->Stop();
  }

  // Check if the message loop is running.
  bool IsRunning() const {
    return msg_loop_->IsRunning();
  }

 private:
  // Callback for message loop commands.
  void CommandCallback(std::unique_ptr<Command> command);

  // Send an ack message to the host for the msgid.
  void SendAck(const HostId& host,
               const MsgId& msgid,
               MessageDataAck::AckStatus status);

  std::unique_ptr<MsgLoop> msg_loop_;
  LogStorage* storage_;
  const PilotOptions& options_;
  const std::map<MessageType, MsgCallbackType> callbacks_;
};

}  // namespace rocketspeed
