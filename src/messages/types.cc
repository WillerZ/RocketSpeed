//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/messages/types.h"

#include "include/Slice.h"
#include "src/util/common/coding.h"

namespace rocketspeed {

void EncodeOrigin(std::string* out, const StreamID origin) {
  PutFixed64(out, static_cast<uint64_t>(origin));
}

bool DecodeOrigin(Slice* in, StreamID* origin) {
  uint64_t origin_fixed;
  if (!GetFixed64(in, &origin_fixed)) {
    return false;
  }
  *origin = static_cast<StreamID>(origin_fixed);
  return true;
}

}  // namespace rocketspeed
