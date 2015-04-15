// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/messages/stream_socket.h"

#include "include/Slice.h"
#include "src/util/common/coding.h"

namespace rocketspeed {

/**
 * Encodes stream ID onto wire.
 *
 * @param origin Origin stream ID.
 * @return Encoded origin.
 */
std::string EncodeOrigin(const StreamID origin) {
  std::string encoded;
  PutFixed64(&encoded, static_cast<uint64_t>(origin));
  return encoded;
}

/**
 * Decodes wire format of stream origin.
 *
 * @param in Input slice of encoded stream spec. Will be advanced beyond spec.
 * @param origin Output parameter for decoded stream.
 * @return ok() if successfully decoded, otherwise error.
 */
Status DecodeOrigin(Slice* in, StreamID* origin) {
  uint64_t origin_fixed;
  if (!GetFixed64(in, &origin_fixed)) {
    return Status::InvalidArgument("Bad stream ID");
  }
  *origin = static_cast<StreamID>(origin_fixed);
  return Status::OK();
}

}  // namespace rocketspeed
