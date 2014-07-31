// Copyright (c) 2014, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "src/util/log_router.h"
#include <string>
#include "src/util/hash.h"

namespace rocketspeed {

LogRouter::LogRouter(uint64_t numLogs)
: _numLogs(numLogs - numLogs % Retention::Total) {
  assert(numLogs > 0);
}

Status LogRouter::GetLogID(const Topic& topic,
                           Retention retention,
                           LogID* out) const {
  // Hash the topic name
  // Using MurmurHash instead of std::hash because std::hash is implementation
  // defined, meaning we cannot rely on it to have a good distribution.
  MurmurHash2<std::string> hasher;
  size_t hash = hasher(topic);

  // Find an initial Log ID for this topic hash key.
  uint64_t result = JumpConsistentHash(hash, _numLogs);

  // set result % Retention::Total to the retention bucket of this topic.
  *out = result - (result % Retention::Total) + RetentionBucket(retention);

  return Status::OK();
}

uint64_t LogRouter::JumpConsistentHash(uint64_t key, uint64_t buckets) {
  // John Lamping, Eric Veach.
  // A Fast, Minimal Memory, Consistent Hash Algorithm.
  // http://arxiv.org/ftp/arxiv/papers/1406/1406.2294.pdf
  uint64_t b = 0, j = 0;
  do {
    b = j;
    key = key * 2862933555777941757ULL + 1;
    j = (b + 1) * (static_cast<double>(1LL << 31) /
                   static_cast<double>((key >> 33) + 1));
  } while (j < buckets);
  return b;
}

}  // namespace rocketspeed
