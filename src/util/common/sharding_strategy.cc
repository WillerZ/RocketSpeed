// Copyright (c) 2017, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "include/HostId.h"
#include "include/Types.h"

namespace rocketspeed {

HostId ShardingStrategy::GetHost(size_t) {
  RS_ASSERT(false) << "GetHost is deprecated, call GetReplica";
  return HostId();
}

HostId ShardingStrategy::GetReplica(size_t shard, size_t replica) {
  RS_ASSERT(replica == 0);
  return GetHost(shard);
}

}
