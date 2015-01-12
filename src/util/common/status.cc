// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "include/Status.h"
#include "src/port/port.h"

namespace rocketspeed {

Status::Status() : code_(Code::kOk), state_("") { }

// Return a success status.
Status Status::OK() { return Status(); }

// Return error status of an appropriate type.
Status Status::NotFound(const std::string& msg, const std::string& msg2) {
  return Status(Code::kNotFound, msg, msg2);
}

// Fast path for not found without malloc;
Status Status::NotFound() {
  return Status(Code::kNotFound);
}

Status Status::NotSupported(const std::string msg,
                            const std::string msg2) {
  return Status(Code::kNotSupported, msg, msg2);
}

Status Status::InvalidArgument(const std::string msg,
                              const std::string msg2) {
  return Status(Code::kInvalidArgument, msg, msg2);
}

Status Status::IOError(const std::string msg,
                       const std::string msg2) {
  return Status(Code::kIOError, msg, msg2);
}

Status Status::NotInitialized() {
  return Status(Code::kNotInitialized);
}

Status Status::InternalError(const std::string msg,
                            const std::string msg2) {
  return Status(Code::kInternal, msg, msg2);
}

Status Status::Unauthorized(const std::string msg,
                            const std::string msg2) {
  return Status(Code::kInternal, msg, msg2);
}

Status Status::TimedOut() {
  return Status(Code::kTimedOut);
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
      default:
        return "Unknown code " + std::to_string(code);
    }
  }
} // namespace
