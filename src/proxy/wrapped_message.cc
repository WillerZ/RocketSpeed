//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "wrapped_message.h"

#include <climits>

#include "src/messages/commands.h"
#include "src/messages/messages.h"
#include "src/util/common/coding.h"

namespace rocketspeed {

std::string WrapMessage(std::string raw_message,
                        StreamID origin,
                        MessageSequenceNumber seqno) {
  // Append origin stream.
  raw_message.append(EncodeOrigin(origin));
  // Append message sequence number
  PutFixed32(&raw_message, seqno);
  return raw_message;
}

Status UnwrapMessage(std::string wrapped_message,
                     std::string* raw_message,
                     StreamID* stream,
                     MessageSequenceNumber* seqno) {
  const size_t kSeqnoSize = sizeof(MessageSequenceNumber),
               kStreamSize = sizeof(StreamID),
               kMetadataSize = kStreamSize + kSeqnoSize;
  // Verify that message contains stream ID and sequence number.
  if (wrapped_message.size() < kMetadataSize) {
    return Status::IOError("Message too short, dropping.");
  }
  Slice metadata_slice(
      wrapped_message.data() + wrapped_message.size() - kMetadataSize,
      kMetadataSize);
  // Deserialize origin stream.
  Status st = DecodeOrigin(&metadata_slice, stream);
  if (!st.ok()) {
    return st;
  }
  // Deserialize message sequence number.
  uint32_t seqno_unsigned;
  if (!GetFixed32(&metadata_slice, &seqno_unsigned)) {
    return Status::IOError("Failed to deserialise sequence number");
  }
  *seqno = static_cast<MessageSequenceNumber>(seqno_unsigned);
  // Strip metdata and leave a serialized message only.
  wrapped_message.resize(wrapped_message.size() - kMetadataSize);
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
  Slice message_slice(raw_message.data(), raw_message.size());
  std::unique_ptr<char[]> message_buf(message_slice.ToUniqueChars());
  *message =
      Message::CreateNewInstance(std::move(message_buf), message_slice.size());
  if (!*message) {
    return Status::IOError("Failed to decode the message.");
  }
  return Status::OK();
}

}  // namespace rocketspeed
