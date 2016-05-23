/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#define __STDC_FORMAT_MACROS
#include "subscriptions_map.h"

#include <cinttypes>

#include "include/Assert.h"
#include "include/Logger.h"
#include "include/Types.h"

namespace rocketspeed {

////////////////////////////////////////////////////////////////////////////////
bool SubscriptionBase::ProcessUpdate(Logger* info_log,
                                     const SequenceNumber previous,
                                     const SequenceNumber current) {
  RS_ASSERT(current >= previous);
  RS_ASSERT(current != 0);

  if ((expected_seqno_ == 0 &&
       previous != 0) /* must receive a snapshot if expected */ ||
      (expected_seqno_ > current) /* must not go back in time */ ||
      (expected_seqno_ < previous) /* must not skip an update */) {
    LOG_WARN(info_log,
             "SubscriptionBase(%llu, %s, %s)::ProcessUpdate(%" PRIu64
             ", %" PRIu64 ") expected %" PRIu64 ", dropped",
             GetIDWhichMayChange().ForLogging(),
             tenant_and_namespace_.Get().namespace_id.c_str(),
             topic_name_.c_str(),
             previous,
             current,
             expected_seqno_);
    return false;
  } else {
    LOG_DEBUG(info_log,
              "SubscriptionBase(%llu, %s, %s)::ProcessUpdate(%" PRIu64
              ", %" PRIu64 ") expected %" PRIu64 ", accepted",
              GetIDWhichMayChange().ForLogging(),
              tenant_and_namespace_.Get().namespace_id.c_str(),
              topic_name_.c_str(),
              previous,
              current,
              expected_seqno_);
    // We now expect the next sequence number.
    expected_seqno_ = current + 1;
    return true;
  }
}

}  // namespace rocketspeed
