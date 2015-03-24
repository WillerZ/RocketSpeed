// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/storage.h"
#include "src/util/topic_uuid.h"

namespace rocketspeed {

Status LogRouter::GetLogID(Slice namespace_id,
                           Slice topic_name,
                           LogID* out) const {
  return RouteToLog(TopicUUID::RoutingHash(namespace_id, topic_name), out);
}

Status LogRouter::GetLogID(const TopicUUID& topic,
                           LogID* out) const {
  return RouteToLog(topic.RoutingHash(), out);
}

}
