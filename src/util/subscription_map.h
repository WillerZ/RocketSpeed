// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <unordered_map>
#include "include/Types.h"
#include "src/messages/stream_socket.h"
#include "src/util/common/thread_check.h"

namespace rocketspeed {

/**
 * Maps a stream ID and subscription ID to any type.
 * Optimized to allow fast removal of all subscription IDs for a stream.
 * Not thread safe.
 */
template <typename T>
class SubscriptionMap {
 public:
  SubscriptionMap() {}

  T* Find(StreamID stream_id, SubscriptionID sub_id) {
    thread_check_.Check();
    auto it = map_.find(stream_id);
    if (it == map_.end()) {
      return nullptr;
    }
    auto it2 = it->second.find(sub_id);
    if (it2 == it->second.end()) {
      return nullptr;
    }
    return &it2->second;
  }

  bool MoveOut(StreamID stream_id, SubscriptionID sub_id, T* out) {
    assert(out);
    thread_check_.Check();
    auto it = map_.find(stream_id);
    if (it == map_.end()) {
      return false;
    }
    auto it2 = it->second.find(sub_id);
    if (it2 == it->second.end()) {
      return false;
    }
    *out = std::move(it2->second);
    it->second.erase(it2);
    return true;
  }

  void Insert(StreamID stream_id, SubscriptionID sub_id, T value) {
    thread_check_.Check();
    map_[stream_id].emplace(sub_id, std::move(value));
  }

  void Remove(StreamID stream_id) {
    thread_check_.Check();
    map_.erase(stream_id);
  }

  void Remove(StreamID stream_id, SubscriptionID sub_id) {
    thread_check_.Check();
    auto it = map_.find(stream_id);
    if (it != map_.end()) {
      it->second.erase(sub_id);
    }
  }

  template <typename Visitor>
  void VisitSubscriptions(StreamID stream_id, Visitor&& visitor) {
    auto it = map_.find(stream_id);
    if (it != map_.end()) {
      for (auto& entry : it->second) {
        visitor(entry.first, entry.second);
      }
    }
  }

 private:
  std::unordered_map<StreamID, std::unordered_map<SubscriptionID, T>> map_;
  ThreadCheck thread_check_;
};

}  // namespace rocketspeed
