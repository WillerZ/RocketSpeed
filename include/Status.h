// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <string>

namespace rocketspeed {

class Status {
 public:
  /// Create a success status.
  Status() : code_(Code::kOk), state_("") { }

  /// Return a success status.
  static Status OK() { return Status(); }

  /// Return error status of an appropriate type.
  static Status NotFound(const std::string& msg, const std::string& msg2 = "") {
    return Status(Code::kNotFound, msg, msg2);
  }
  /// Fast path for not found without malloc;
  static Status NotFound() {
    return Status(Code::kNotFound);
  }
  static Status NotSupported(const std::string msg,
                             const std::string msg2 = "") {
    return Status(Code::kNotSupported, msg, msg2);
  }
  static Status InvalidArgument(const std::string msg,
                                const std::string msg2 = "") {
    return Status(Code::kInvalidArgument, msg, msg2);
  }
  static Status IOError(const std::string msg,
                        const std::string msg2 = "") {
    return Status(Code::kIOError, msg, msg2);
  }
  static Status NotInitialized() {
    return Status(Code::kNotInitialized);
  }
  static Status InternalError(const std::string msg,
                              const std::string msg2 = "") {
    return Status(Code::kInternal, msg, msg2);
  }
  static Status Unauthorized(const std::string msg,
                             const std::string msg2 = "") {
    return Status(Code::kInternal, msg, msg2);
  }
  static Status TimedOut() {
    return Status(Code::kTimedOut);
  }

  /// Returns true iff the status indicates success.
  bool ok() const { return code_ == Code::kOk; }

  /// Returns true iff the status indicates a NotFound error.
  bool IsNotFound() const { return code_ == Code::kNotFound; }

  /// Returns true iff the status indicates a NotSupported error.
  bool IsNotSupported() const { return code_ == Code::kNotSupported; }

  /// Returns true iff the status indicates an InvalidArgument error.
  bool IsInvalidArgument() const { return code_ == Code::kInvalidArgument; }

  /// Returns true iff the status indicates an IOError error.
  bool IsIOError() const { return code_ == Code::kIOError; }

  /// Returns true iff the status indicates Not initialized
  bool IsNotInitialized() const { return code_ == Code::kNotInitialized; }

  /// Returns true iff the status indicates an internal error.
  bool IsInternal() const { return code_ == Code::kInternal; }

  /// Returns true iff the status indicates an unauthorized access.
  bool IsUnauthorized() const { return code_ == Code::kUnauthorized; }

  /// Returns true iff the status indicates a time out.
  bool IsTimedOut() const { return code_ == Code::kTimedOut; }

  /// Return a string representation of this status suitable for printing.
  /// Returns the string "OK" for success.
  std::string ToString() const {
    int code = static_cast<int>(code_);
    switch (code_) {
      case Code::kOk:
        return "OK";
      case Code::kNotFound:
        return "NotFound: " + std::to_string(code);
      case Code::kNotSupported:
        return "Not implemented: " + std::to_string(code);
      case Code::kInvalidArgument:
        return "Invalid argument: " + std::to_string(code);
      case Code::kIOError:
        return "IO error: " + std::to_string(code);
      case Code::kNotInitialized:
        return "Not initialized: " + std::to_string(code);
      case Code::kInternal:
        return "Internal error: " + std::to_string(code);
      case Code::kUnauthorized:
        return "Unauthorized: " + std::to_string(code);
      case Code::kTimedOut:
        return "Timed out: " + std::to_string(code);
      default:
        return "Unknown code " + std::to_string(code);
    }
  }

 private:
  enum class Code : char {
    kOk = 0,
    kNotFound = 1,
    kNotSupported = 2,
    kInvalidArgument = 3,
    kIOError = 4,
    kNotInitialized = 5,
    kInternal = 6,
    kUnauthorized = 7,
    kTimedOut = 8,
  };

  Code code_;
  std::string state_;

  explicit Status(Code code) : code_(code), state_("") { }
  Status(Code code, const std::string msg, const std::string msg2) :
    code_(code), state_(msg + msg2) {
  }
};

} // namespace
