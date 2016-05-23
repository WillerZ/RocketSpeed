/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#include "src/client/subscription_id.h"

#include "include/Slice.h"
#include "src/util/common/coding.h"

namespace rocketspeed {

void EncodeSubscriptionID(std::string* out, SubscriptionID in) {
  PutVarint64(out, static_cast<uint64_t>(in));
}

bool DecodeSubscriptionID(Slice* in, SubscriptionID* out) {
  uint64_t value;
  if (!GetVarint64(in, &value)) {
    return false;
  }
  *out = SubscriptionID(value);
  return true;
}

}  // namespace rocketspeed
