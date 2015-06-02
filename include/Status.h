// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <string>

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC visibility push(default)
#endif

namespace rocketspeed {

class Status {
 public:
  /// Create a success status.
  Status();

  /// Return a success status.
  static Status OK();

  /// Return error status of an appropriate type.
  static Status NotFound(std::string msg);

  /// Fast path for not found without malloc;
  static Status NotFound();

  static Status NotSupported(std::string msg);

  static Status InvalidArgument(std::string msg);

  static Status IOError(std::string msg);

  static Status NotInitialized();

  static Status InternalError(std::string msg);

  static Status Unauthorized(std::string msg);

  static Status TimedOut(std::string msg = "");

  static Status NoBuffer();

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

  /// Returns true iff the status indicates buffer full.
  bool IsNoBuffer() const;

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
    kNoBuffer = 9,
  };

  Code code_;
  std::string state_;

  explicit Status(Code code) : code_(code), state_("") { }

  Status(Code code, std::string msg) :
    code_(code), state_(std::move(msg)) {
  }
};

} // namespace
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC visibility pop
#endif
