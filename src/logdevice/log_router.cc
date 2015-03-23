// Copyright (c) 2014, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "src/logdevice/log_router.h"
#include <string>

namespace rocketspeed {

LogDeviceLogRouter::LogDeviceLogRouter(LogID first, LogID last)
: first_(first)
, count_(last - first + 1) {
  assert(last >= first);
}

Status LogDeviceLogRouter::GetLogID(Slice namespace_id,
                                    Slice topic_name,
                                    LogID* out) const {
  // Find the Log ID for this topic hash key.
  const size_t key = TopicHash(namespace_id, topic_name);
  *out = JumpConsistentHash(key, count_) + first_;
  return Status::OK();
}

uint64_t
LogDeviceLogRouter::JumpConsistentHash(uint64_t key, uint64_t buckets) {
  // John Lamping, Eric Veach.
  // A Fast, Minimal Memory, Consistent Hash Algorithm.
  // http://arxiv.org/ftp/arxiv/papers/1406/1406.2294.pdf
  uint64_t b = 0, j = 0;
  do {
    b = j;
    key = key * 2862933555777941757ULL + 1;
    j = static_cast<uint64_t>(
          static_cast<double>(b + 1) *
          (static_cast<double>(1LL << 31) /
          (static_cast<double>((key >> 33) + 1))));
  } while (j < buckets);
  return b;
}

}  // namespace rocketspeed
