/**
 * @file
 * @version 1.0
 *
 * @section LICENSE
 *
 * Copyright Facebook 2014
 *
 * @section DESCRIPTION
 *
 * This file defines the various return codes returned by methods
 * in the RocketSpeed library interface.
 */
#pragma once

#include <string>

namespace facebook { namespace rocketspeed {

class Status {
 public:
  /// Create a success status.
  Status() : code_(Code::kOk), state_("") { }

  /// Return a success status.
  static Status ok() { return Status(); }

  /// Return error status of an appropriate type.
  static Status notFound(const std::string& msg, const std::string& msg2 = "") {
    return Status(Code::kNotFound, msg, msg2);
  }
  /// Fast path for not found without malloc;
  static Status notFound() {
    return Status(Code::kNotFound);
  }
  static Status notSupported(const std::string msg,
                             const std::string msg2 = "") {
    return Status(Code::kNotSupported, msg, msg2);
  }
  static Status invalidArgument(const std::string msg,
                                const std::string msg2 = "") {
    return Status(Code::kInvalidArgument, msg, msg2);
  }
  static Status ioError(const std::string msg,
                        const std::string msg2 = "") {
    return Status(Code::kIOError, msg, msg2);
  }
  static Status notInitialized() {
    return Status(Code::kNotInitialized);
  }

  /// Returns true iff the status indicates success.
  bool isOk() const { return code_ == Code::kOk; }

  /// Returns true iff the status indicates a NotFound error.
  bool isNotFound() const { return code_ == Code::kNotFound; }

  /// Returns true iff the status indicates a NotSupported error.
  bool isNotSupported() const { return code_ == Code::kNotSupported; }

  /// Returns true iff the status indicates an InvalidArgument error.
  bool isInvalidArgument() const { return code_ == Code::kInvalidArgument; }

  /// Returns true iff the status indicates an IOError error.
  bool isIOError() const { return code_ == Code::kIOError; }

  /// Returns true iff the status indicates Not initialized
  bool isNotInitialized() const { return code_ == Code::kNotInitialized; }

  /// Return a string representation of this status suitable for printing.
  /// Returns the string "OK" for success.
  std::string toString() const {
    switch (code_) {
      case Code::kOk:
        return "OK";
      case Code::kNotFound:
        return "NotFound: " + std::to_string((int)code_);
      case Code::kNotSupported:
        return "Not implemented: " + std::to_string((int)code_);
      case Code::kInvalidArgument:
        return "Invalid argument: " + std::to_string((int)code_);
      case Code::kIOError:
        return "IO error: " + std::to_string((int)code_);
      case Code::kNotInitialized:
        return "Not initialized: " + std::to_string((int)code_);
      default:
        return "Unknown code " + std::to_string((int)code_);
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
  };

  Code code_;
  std::string state_;

  explicit Status(Code code) : code_(code), state_("") { }
  Status(Code code, const std::string msg, const std::string msg2) :
    code_(code), state_(msg + msg2) {
  }
};

}} // namespace
