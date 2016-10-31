// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "include/Status.h"
#include "src/port/port.h"

namespace rocketspeed {

Status::Status() : code_(Code::kOk), state_() { }

// Return a success status.
Status Status::OK() { return Status(); }

// Return error status of an appropriate type.
Status Status::NotFound(std::string msg) {
  return Status(Code::kNotFound, std::move(msg));
}

// Fast path for not found without malloc;
Status Status::NotFound() {
  return Status(Code::kNotFound);
}

Status Status::NotSupported(std::string msg) {
  return Status(Code::kNotSupported, std::move(msg));
}

Status Status::InvalidArgument(std::string msg) {
  return Status(Code::kInvalidArgument, std::move(msg));
}

Status Status::IOError(std::string msg) {
  return Status(Code::kIOError, std::move(msg));
}

Status Status::NotInitialized() {
  return Status(Code::kNotInitialized);
}

Status Status::InternalError(std::string msg) {
  return Status(Code::kInternal, std::move(msg));
}

Status Status::Unauthorized(std::string msg) {
  return Status(Code::kInternal, std::move(msg));
}

Status Status::TimedOut(std::string msg) {
  return Status(Code::kTimedOut, std::move(msg));
}

Status Status::NoBuffer() {
  return Status(Code::kNoBuffer);
}

Status Status::ShardUnhealthy() {
  return Status(Code::kShardUnhealthy);
}

// Returns true iff the status indicates success.
bool Status::ok() const { return code_ == Code::kOk; }

// Returns true iff the status indicates a NotFound error.
bool Status::IsNotFound() const { return code_ == Code::kNotFound; }

// Returns true iff the status indicates a NotSupported error.
bool Status::IsNotSupported() const { return code_ == Code::kNotSupported; }

// Returns true iff the status indicates an InvalidArgument error.
bool Status::IsInvalidArgument() const {
  return code_ == Code::kInvalidArgument;
}

// Returns true iff the status indicates an IOError error.
bool Status::IsIOError() const { return code_ == Code::kIOError; }

// Returns true iff the status indicates Not initialized
bool Status::IsNotInitialized() const {
  return code_ == Code::kNotInitialized;
}

// Returns true iff the status indicates an internal error.
bool Status::IsInternal() const { return code_ == Code::kInternal; }

// Returns true iff the status indicates an unauthorized access.
bool Status::IsUnauthorized() const { return code_ == Code::kUnauthorized; }

// Returns true iff the status indicates a time out.
bool Status::IsTimedOut() const { return code_ == Code::kTimedOut; }

// Returns true iff the status indicates buffer full.
bool Status::IsNoBuffer() const { return code_ == Code::kNoBuffer; }

// Returns true iff the status indicates shard unhealthy.
bool Status::IsShardUnhealthy() const { return code_ == Code::kShardUnhealthy; }

// Return a string representation of this status suitable for printing.
// Returns the string "OK" for success.
std::string Status::ToString() const {
  int code = static_cast<int>(code_);
  switch (code_) {
    case Code::kOk:
      return "OK";
    case Code::kNotFound:
      return "NotFound: " + state_;
    case Code::kNotSupported:
      return "Not implemented: " + state_;
    case Code::kInvalidArgument:
      return "Invalid argument: " + state_;
    case Code::kIOError:
      return "IO error: " + state_;
    case Code::kNotInitialized:
      return "Not initialized: " + state_;
    case Code::kInternal:
      return "Internal error: " + state_;
    case Code::kUnauthorized:
      return "Unauthorized: " + state_;
    case Code::kTimedOut:
      return "Timed out: " + state_;
    case Code::kNoBuffer:
      return "No buffer: " + state_;
    case Code::kShardUnhealthy:
      return "Shard marked unhealthy: " + state_;
    default:
      return "Unknown code " + std::to_string(code);
  }
}

} // namespace
