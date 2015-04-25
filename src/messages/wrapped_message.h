//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "include/Status.h"
#include "include/Slice.h"
#include "src/messages/stream_socket.h"

namespace rocketspeed {

class Message;
class SendCommand;

typedef int32_t MessageSequenceNumber;

std::string WrapMessage(std::string raw_message,
                        StreamID stream,
                        MessageSequenceNumber seqno);

Status UnwrapMessage(std::string wrapped_message,
                     std::string* raw_message,
                     StreamID* stream,
                     MessageSequenceNumber* seqno);

Status UnwrapMessage(std::string wrapped_message,
                     std::unique_ptr<Message>* message,
                     StreamID* stream,
                     MessageSequenceNumber* seqno);

}  // namespace rocketspeed
