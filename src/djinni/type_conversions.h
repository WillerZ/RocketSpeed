//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <cstdint>
#include <vector>

#include "include/Logger.h"
#include "include/Types.h"

#include "src-gen/djinni/cpp/LogLevel.hpp"

// Forward declare namespaces and setup short names for all the code that
// translates between RocketSpeed and Djinni types.
namespace rocketspeed {
namespace djinni {}  // djinni
}  // rocketspeed

namespace rs = ::rocketspeed;
namespace jni = ::rocketspeed::djinni;

namespace rocketspeed {

class Slice;
class Status;

namespace djinni {

class MsgId;
class Status;

int64_t FromSequenceNumber(uint64_t uval);
uint64_t ToSequenceNumber(int64_t uval);

int64_t FromUint(uint64_t uval);
uint64_t ToUint(int64_t val);

rs::InfoLogLevel ToInfoLogLevel(jni::LogLevel log_level);

jni::Status FromStatus(rs::Status status);

rs::MsgId ToMsgId(const jni::MsgId& message_id);
jni::MsgId FromMsgId(const rs::MsgId& message_id);

rs::Slice ToSlice(const std::vector<uint8_t>& data);
std::vector<uint8_t> FromSlice(rs::Slice slice);

}  // namespace djinni
}  // namespace rocketspeed
