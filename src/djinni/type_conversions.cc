//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "type_conversions.h"

#include <cstdint>
#include <memory>
#include <limits>
#include <string>
#include <vector>

#include "include/Logger.h"
#include "include/Status.h"
#include "include/Types.h"

#include "src-gen/djinni/cpp/MsgId.hpp"
#include "src-gen/djinni/cpp/LogLevel.hpp"
#include "src-gen/djinni/cpp/Status.hpp"
#include "src-gen/djinni/cpp/StatusCode.hpp"
#include "src-gen/djinni/cpp/StorageType.hpp"

namespace rocketspeed {
namespace djinni {

int64_t FromSequenceNumber(uint64_t uval) {
  return FromUint(uval);
}

uint64_t ToSequenceNumber(int64_t uval) {
  return ToUint(uval);
}

int64_t FromUint(uint64_t uval) {
  using Limits = std::numeric_limits<int64_t>;
  if (uval <= static_cast<uint64_t>(Limits::min()))
    return static_cast<int64_t>(uval);
  if (uval >= static_cast<uint64_t>(Limits::min()))
    return static_cast<int64_t>(uval - Limits::min()) + Limits::min();
  RS_ASSERT(false);
  return 0;
}

uint64_t ToUint(int64_t val) {
  return static_cast<uint64_t>(val);
}

rs::InfoLogLevel ToInfoLogLevel(jni::LogLevel log_level) {
  using rs::InfoLogLevel;
  static_assert(InfoLogLevel::DEBUG_LEVEL ==
                    static_cast<InfoLogLevel>(LogLevel::DEBUG_LEVEL),
                "Enum representations do not match.");
  static_assert(InfoLogLevel::NUM_INFO_LOG_LEVELS ==
                    static_cast<InfoLogLevel>(LogLevel::NUM_INFO_LOG_LEVELS),
                "Enum representations do not match.");
  return static_cast<InfoLogLevel>(log_level);
}

jni::Status FromStatus(rs::Status status) {
  StatusCode code = StatusCode::INTERNAL;
  if (status.ok()) {
    code = StatusCode::OK;
  } else if (status.IsNotFound()) {
    code = StatusCode::NOTFOUND;
  } else if (status.IsNotSupported()) {
    code = StatusCode::NOTSUPPORTED;
  } else if (status.IsInvalidArgument()) {
    code = StatusCode::INVALIDARGUMENT;
  } else if (status.IsIOError()) {
    code = StatusCode::IOERROR;
  } else if (status.IsNotInitialized()) {
    code = StatusCode::NOTINITIALIZED;
  } else if (status.IsUnauthorized()) {
    code = StatusCode::UNAUTHORIZED;
  } else if (status.IsTimedOut()) {
    code = StatusCode::TIMEDOUT;
  } else if (status.IsInternal()) {
    code = StatusCode::INTERNAL;
  } else {
    RS_ASSERT(false);
  }
  return Status(code, status.ToString());
}

rs::MsgId ToMsgId(const jni::MsgId& message_id) {
  union {
    char id[16];
    struct {
      int64_t hi, lo;
    };
  } value;
  value.hi = message_id.hi;
  value.lo = message_id.lo;
  return rs::MsgId(value.id);
}

jni::MsgId FromMsgId(const rs::MsgId& message_id) {
  union {
    char id[16];
    struct {
      int64_t hi, lo;
    };
  } value;
  memcpy(value.id, message_id.id, 16);
  return jni::MsgId(value.hi, value.lo);
}

rs::Slice ToSlice(const std::vector<uint8_t>& data) {
  auto first = reinterpret_cast<const char*>(data.data());
  return rs::Slice(first, data.size());
}

std::vector<uint8_t> FromSlice(rs::Slice slice) {
  auto first = reinterpret_cast<const uint8_t*>(slice.data());
  return std::vector<uint8_t>(first, first + slice.size());
}

}  // namespace djinni
}  // namespace rocketspeed
