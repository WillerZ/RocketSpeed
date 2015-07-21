// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <functional>
#include <memory>
#include <string>

#include "include/Types.h"
#include "src/messages/messages.h"
#include "src/messages/stream_socket.h"
#include "src/util/common/autovector.h"
#include "src/util/common/host_id.h"

namespace rocketspeed {

enum CommandType : uint8_t {
  kNotInitialized = 0,
  kSendCommand = 0x01,
  kAcceptCommand = 0x02,
  kExecuteCommand = 0x03,
};

/**
 * Interface class for sending messages from any thread to the event loop for
 * processing on the event loop thread.
 */
class Command {
 public:
  explicit Command() {}

  virtual ~Command() {}

  /** Get type of the command. */
  virtual CommandType GetCommandType() const = 0;
};

/**
 * Command for sending a message to remote recipients.
 * The SendCommand is special because the event loop processes it inline
 * instead of invoking the application callback. The message associated with
 * this command is sent to the streams specified via a call to GetDestination().
 */
class SendCommand : public Command {
 public:
  /**
   * Denotes stream and, if this is a first message on the stream, destination
   * client ID.
   */
  struct StreamSpec {
    StreamSpec(StreamID _stream, HostId _destination)
        : stream(_stream), destination(std::move(_destination)) {
    }
    StreamID stream;
    HostId destination;
  };
  /** Allocate one stream spec in-place for the common case. */
  typedef autovector<StreamSpec, 1> Recipients;
  /** Denotes sockets that this message shall be sent to. */
  typedef autovector<StreamSocket*, 1> SocketList;
  /** Denotes streams that this message shall be sent to. */
  typedef autovector<StreamID, 1> StreamList;

  explicit SendCommand(Recipients recipients)
      : recipients_(std::move(recipients)) {}

  virtual ~SendCommand() {}

  CommandType GetCommandType() const { return kSendCommand; }

  /**
   * Writes a serialised form of a message to provided string.
   * Depending on implementation, this call might perform a move of message
   * content into provided string.
   */
  virtual void GetMessage(std::string* out) = 0;

  /**
   * If this is a command to send a mesage to remote hosts, then returns the
   * list of destination stream specs.
   */
  const Recipients& GetDestinations() const { return recipients_; }

 private:
  Recipients recipients_;
};

/** SendCommand where message is passed in a serialized form. */
class SerializedSendCommand : public SendCommand {
 public:
  static std::unique_ptr<SerializedSendCommand> Request(
      std::string serialized,
      const SocketList& sockets) {
    Recipients recipients;
    for (const auto& socket : sockets) {
      recipients.emplace_back(
          socket->GetStreamID(),
          socket->IsOpen() ? HostId() : socket->GetDestination());
    }
    return std::unique_ptr<SerializedSendCommand>(new SerializedSendCommand(
        std::move(serialized), std::move(recipients)));
  }

  static std::unique_ptr<SerializedSendCommand> Response(
      std::string serialized,
      const StreamList& streams) {
    Recipients recipients;
    for (auto stream : streams) {
      recipients.emplace_back(stream, HostId());
    }
    return std::unique_ptr<SerializedSendCommand>(new SerializedSendCommand(
        std::move(serialized), std::move(recipients)));
  }

  void GetMessage(std::string* out) {
    out->assign(std::move(message_));
  }

 private:
  // Hiding, as it's not super convenient to work with this class without
  // std::make_unique.
  SerializedSendCommand(std::string message, Recipients recipients)
      : SendCommand(std::move(recipients)), message_(std::move(message)) {
    assert(message_.size() > 0);
  }
  // Buffer with the message. It's content is moved away on first attempt to get
  // serialized message.
  std::string message_;
};

/**
 * Command that executes a function from within the event loop.
 */
class ExecuteCommand : public Command {
 public:
  /**
   * Executes a function within the event loop thread.
   */
  explicit ExecuteCommand() {}

  virtual ~ExecuteCommand() {}

  virtual CommandType GetCommandType() const {
    return kExecuteCommand;
  }

  virtual void Execute() = 0;
};

template <typename Function>
class ExecuteCommandImpl : public ExecuteCommand {
 public:
  explicit ExecuteCommandImpl(Function func)
  : func_(std::move(func)) {}

  void Execute() override {
    func_();
  }

 private:
  Function func_;
};

template <typename Function>
ExecuteCommandImpl<Function>* MakeExecuteCommand(Function func) {
  return new ExecuteCommandImpl<Function>(std::move(func));
}

}  // namespace rocketspeed
