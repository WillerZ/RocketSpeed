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
#include "src/messages/streams.h"
#include "src/util/common/autovector.h"

namespace rocketspeed {

enum CommandType : uint8_t {
  kNotInitialized = 0,
  kSendCommand = 0x01,
  kAcceptCommand = 0x02,
  kStorageUpdateCommand = 0x03,
  kStorageLoadCommand = 0x04,
  kStorageSnapshotCommand = 0x05,
  kExecuteCommand = 0x06,
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
 * this command is sent to the host specified via a call to GetDestination().
 */
class SendCommand : public Command {
 public:
  /** Allocate one ClientID in-place for the common case. */
  typedef autovector<ClientID, 1> Recipients;

  SendCommand(StreamID stream, bool new_request)
      : stream_(stream), new_request_(new_request) {}

  virtual ~SendCommand() {}

  CommandType GetCommandType() const { return kSendCommand; }

  /** Writes a serialised form of a message to provided string. */
  virtual void GetMessage(std::string* out) = 0;

  /**
   * If this is a command to send a mesage to remote hosts, then returns the
   * list of destination HostIds.
   */
  virtual const Recipients& GetDestination() const = 0;

  /** Returns identifier of the stream this message belongs to. */
  StreamID GetStream() const { return stream_; }

  /**
   * Returns true iff this is a new request. Responses shall not trigger
   * reestablishment of a stream if it broke.
   */
  bool IsNewRequest() const { return new_request_; }

 private:
  StreamID stream_;
  bool new_request_;
};

/**
 * SendCommand where message is serialized before sending.
 */
class SerializedSendCommand : public SendCommand {
 public:
  static std::unique_ptr<SerializedSendCommand> CreateRequest(
      std::string serialized,
      StreamSocket* socket) {
    return std::unique_ptr<SerializedSendCommand>(
        new SerializedSendCommand(std::move(serialized),
                                  socket->GetDestination(),
                                  socket->GetStreamID(),
                                  true));
  }

  SerializedSendCommand() = default;

  SerializedSendCommand(std::string message,
                        ClientID recipient,
                        StreamID stream,
                        bool new_request)
      : SendCommand(stream, new_request)
      , message_(std::move(message)) {
    recipient_.push_back(std::move(recipient));
    assert(message_.size() > 0);
  }

  void GetMessage(std::string* out) {
    out->assign(std::move(message_));
  }

  const Recipients& GetDestination() const {
    return recipient_;
  }

 private:
  Recipients recipient_;      // the list of destinations
  std::string message_;       // the message itself
};

/**
 * Command that executes a function from within the event loop.
 */
class ExecuteCommand : public Command {
 public:
  /**
   * Executes a function within the event loop thread.
   *
   * @param func The function to execute.
   */
  explicit ExecuteCommand(std::function<void()> func)
  : func_(std::move(func)) {}

  virtual ~ExecuteCommand() {}

  virtual CommandType GetCommandType() const {
    return kExecuteCommand;
  }

  void Execute() {
    func_();
  }

 private:
  std::function<void()> func_;
};

}  // namespace rocketspeed
