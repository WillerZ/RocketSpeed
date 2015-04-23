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

namespace {

const uint32_t kHeaderMagic = 0xD00DBEEF, kFooterMagic = 0xFEE1DEAD;

const size_t kHeaderSize = sizeof(kHeaderMagic),
             kFooterSize = sizeof(StreamID) + sizeof(MessageSequenceNumber) +
                           sizeof(kFooterMagic);

std::string MagicToHexString(const uint32_t magic) {
  return Slice(reinterpret_cast<const char*>(&magic), sizeof(magic))
      .ToString(true);
}

}  // namespace

std::string WrapMessage(std::string raw_message,
                        StreamID origin,
                        MessageSequenceNumber seqno) {
  std::string wrapped_message;
  wrapped_message.reserve(raw_message.size() + kHeaderSize + kFooterSize);
  // Prefix message with header magic bytes.
  PutFixed32(&wrapped_message, kHeaderMagic);
  // Put the RocketSpeed message.
  wrapped_message.append(raw_message);
  // Append origin stream.
  EncodeOrigin(&wrapped_message, origin);
  // Append message sequence number
  PutFixed32(&wrapped_message, seqno);
  // Suffix message with magic bytes.
  PutFixed32(&wrapped_message, kFooterMagic);
  return wrapped_message;
}

Status UnwrapMessage(std::string wrapped_message,
                     std::string* raw_message,
                     StreamID* stream,
                     MessageSequenceNumber* seqno) {
  if (wrapped_message.size() < kHeaderSize + kFooterSize) {
    return Status::IOError("Message too short to contain header and footer.");
  }

  // We'll accumulate description of all encountered errors and try to
  // opportunistically deserialise as much of a message as possible.
  std::string errors;

  {  // Read the header.
    Slice in(wrapped_message.data(), kHeaderSize);
    uint32_t header_magic;
    if (!GetFixed32(&in, &header_magic)) {
      errors += "failed reding header magic, ";
    } else if (header_magic != kHeaderMagic) {
      errors += MagicToHexString(kHeaderMagic) + " != " +
                MagicToHexString(header_magic) + " in header, ";
    }
  }

  {  // Read the footer.
    Slice in(wrapped_message.data() + (wrapped_message.size() - kFooterSize),
             kFooterSize);
    // Deserialize origin stream.
    if (!DecodeOrigin(&in, stream)) {
      errors += "failed reading origin, ";
    }
    // Deserialize message sequence number.
    uint32_t seqno_unsigned;
    if (!GetFixed32(&in, &seqno_unsigned)) {
      errors += "failed reading seqno, ";
    } else {
      *seqno = static_cast<MessageSequenceNumber>(seqno_unsigned);
    }
    // Deserialize footer magic.
    uint32_t footer_magic;
    if (!GetFixed32(&in, &footer_magic)) {
      errors += "failed reading footer magic, ";
    } else if (footer_magic != kFooterMagic) {
      errors += MagicToHexString(kFooterMagic) + " != " +
                MagicToHexString(footer_magic) + " in footer, ";
    }
  }

  // Strip header and footer and leave a serialized message only.
  wrapped_message.erase(0, kHeaderSize);
  wrapped_message.resize(wrapped_message.size() - kFooterSize);
  *raw_message = std::move(wrapped_message);
  return errors.empty() ? Status::OK() : Status::IOError(std::move(errors));
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
