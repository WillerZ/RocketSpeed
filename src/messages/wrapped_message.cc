//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "wrapped_message.h"

#include "src/messages/commands.h"
#include "src/messages/messages.h"
#include "src/util/common/coding.h"

namespace rocketspeed {

namespace {

const size_t kFooterSize = sizeof(StreamID) + sizeof(MessageSequenceNumber);

}  // namespace

std::string WrapMessage(std::string message,
                        StreamID origin,
                        MessageSequenceNumber seqno) {
  message.reserve(message.size() + kFooterSize);
  // Append origin stream.
  EncodeOrigin(&message, origin);
  // Append message sequence number
  PutFixed32(&message, seqno);
  return message;
}

Status UnwrapMessage(std::string wrapped_message,
                     std::string* raw_message,
                     StreamID* stream,
                     MessageSequenceNumber* seqno) {
  if (wrapped_message.size() < kFooterSize) {
    return Status::IOError("Message too short to contain footer.");
  }

  // Read the footer.
  Slice in(wrapped_message.data() + (wrapped_message.size() - kFooterSize),
           kFooterSize);
  // Deserialize origin stream.
  if (!DecodeOrigin(&in, stream)) {
    return Status::IOError("Failed deserializing origin stream.");
  }
  // Deserialize message sequence number.
  uint32_t seqno_unsigned;
  if (!GetFixed32(&in, &seqno_unsigned)) {
    return Status::IOError("Failed deserializing message sequence number.");
  }
  *seqno = static_cast<MessageSequenceNumber>(seqno_unsigned);

  // Strip header and footer and leave a serialized message only.
  wrapped_message.resize(wrapped_message.size() - kFooterSize);
  *raw_message = std::move(wrapped_message);
  return Status::OK();
}

Status UnwrapMessage(std::string wrapped_message,
                     std::unique_ptr<Message>* message,
                     StreamID* stream,
                     MessageSequenceNumber* seqno) {
  std::string raw_message;
  Status st =
      UnwrapMessage(std::move(wrapped_message), &raw_message, stream, seqno);
  if (!st.ok()) {
    return st;
  }
  *message = Message::CreateNewInstance(Slice(raw_message));
  if (!*message) {
    return Status::IOError("Failed to decode the message.");
  }
  return Status::OK();
}

}  // namespace rocketspeed
