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
  Status();

  /// Return a success status.
  static Status OK();

  /// Return error status of an appropriate type.
  static Status NotFound(const std::string& msg, const std::string& msg2 = "");

  /// Fast path for not found without malloc;
  static Status NotFound();

  static Status NotSupported(const std::string msg,
                             const std::string msg2 = "");

  static Status InvalidArgument(const std::string msg,
                                const std::string msg2 = "");

  static Status IOError(const std::string msg,
                        const std::string msg2 = "");

  static Status NotInitialized();

  static Status InternalError(const std::string msg,
                              const std::string msg2 = "");

  static Status Unauthorized(const std::string msg,
                             const std::string msg2 = "");

  static Status TimedOut();

  /// Returns true iff the status indicates success.
  bool ok() const;

  /// Returns true iff the status indicates a NotFound error.
  bool IsNotFound() const;

  /// Returns true iff the status indicates a NotSupported error.
  bool IsNotSupported() const;

  /// Returns true iff the status indicates an InvalidArgument error.
  bool IsInvalidArgument() const;

  /// Returns true iff the status indicates an IOError error.
  bool IsIOError() const;

  /// Returns true iff the status indicates Not initialized
  bool IsNotInitialized() const;

  /// Returns true iff the status indicates an internal error.
  bool IsInternal() const;

  /// Returns true iff the status indicates an unauthorized access.
  bool IsUnauthorized() const;

  /// Returns true iff the status indicates a time out.
  bool IsTimedOut() const;

  /// Return a string representation of this status suitable for printing.
  /// Returns the string "OK" for success.
  std::string ToString() const;

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
