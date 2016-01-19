// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "rocketeer.h"

#include <functional>
#include <memory>
#include <sstream>
#include <string>

#include "external/folly/move_wrapper.h"

#include "include/Logger.h"
#include "include/RocketSpeed.h"
#include "include/Slice.h"
#include "include/Status.h"
#include "include/Types.h"
#include "include/Env.h"
#include "src/util/common/hash.h"

namespace rocketspeed {

////////////////////////////////////////////////////////////////////////////////
size_t InboundID::Hash() const {
  return MurmurHash2<StreamID, SubscriptionID>()(stream_id, sub_id);
}

std::string InboundID::ToString() const {
  std::ostringstream ss;
  ss << "InboundID(" << stream_id << ", " << sub_id << ")";
  return ss.str();
}

////////////////////////////////////////////////////////////////////////////////
void Rocketeer::Deliver(InboundID inbound_id,
                        SequenceNumber seqno,
                        std::string payload,
                        MsgId msg_id) {
  GetBelowRocketeer()->Deliver(inbound_id, seqno, std::move(payload), msg_id);
}

void Rocketeer::Advance(InboundID inbound_id, SequenceNumber seqno) {
  GetBelowRocketeer()->Advance(inbound_id, seqno);
}

void Rocketeer::Terminate(InboundID inbound_id, UnsubscribeReason reason) {
  GetBelowRocketeer()->Terminate(inbound_id, reason);
}

}  // namespace rocketspeed
