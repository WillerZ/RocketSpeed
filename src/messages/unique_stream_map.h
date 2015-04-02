// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <unordered_map>

#include "src/messages/stream_allocator.h"
#include "src/util/common/thread_check.h"

namespace rocketspeed {

/**
 * An utility to remap stream IDs, which are unique in some context (local) into
 * ones which are unique to the instance of this class (global). Context should
 * be light, as it will be copied, hashed, compared and stored in many copies.
 */
template <typename Context>
class UniqueStreamMap {
 public:
  typedef std::unordered_map<StreamID, StreamID> LocalMap;

  /**
   * Creates an empty mapping, where remapped stream IDs are allocated using
   * provided allocator.
   */
  explicit UniqueStreamMap(StreamAllocator allocator)
      : allocator_(std::move(allocator)) {
  }

  bool FindLocalAndContext(StreamID global,
                           Context* out_context,
                           StreamID* out_local) const {
    thread_check_.Check();

    // Try to find global -> (ctx, local).
    const auto it_g = global_to_local_.find(global);
    if (it_g == global_to_local_.end()) {
      return false;
    }
    *out_context = it_g->second.first;
    *out_local = it_g->second.second;
    return true;
  }

  void InsertGlobal(StreamID global, const Context& context) {
    thread_check_.Check();

    // Insert (ctx, local = global) -> global.
    context_to_map_[context][global] = global;
    // Insert global -> (ctx, local = global).
    global_to_local_[global] = std::make_pair(std::move(context), global);
  }

  std::pair<bool, StreamID> GetGlobal(const Context& context, StreamID local) {
    thread_check_.Check();

    {  // Try to find (ctx, local) -> global.
      const auto it_c = context_to_map_.find(context);
      if (it_c != context_to_map_.end()) {
        const auto& local_map = it_c->second;
        const auto it_l = local_map.find(local);
        if (it_l != local_map.end()) {
          return std::make_pair(false, it_l->second);
        }
      }
    }
    // Allocate new global stream ID if we don't have the mapping.
    StreamID global = allocator_.Next();
    // Insert ctx -> (local -> global).
    context_to_map_[context][local] = global;
    // Insert global -> (ctx, local).
    assert(global_to_local_.find(global) == global_to_local_.end());
    global_to_local_.emplace(global, std::make_pair(std::move(context), local));
    // Return global stream ID.
    return std::make_pair(true, global);
  }

  enum RemovalStatus {
    kNotFound,
    kRemoved,
    kRemovedLast,
  };

  RemovalStatus RemoveGlobal(StreamID global,
                             Context* out_context,
                             StreamID* out_local) {
    thread_check_.Check();

    Context context;
    StreamID local;
    {  // Remove global -> (ctx, local)
      const auto it_g = global_to_local_.find(global);
      if (it_g == global_to_local_.end()) {
        return RemovalStatus::kNotFound;
      }
      context = it_g->second.first;
      *out_context = context;
      local = it_g->second.second;
      *out_local = local;
      global_to_local_.erase(it_g);
    }
    // Remove ctx -> (local -> global).
    const auto it_c = context_to_map_.find(context);
    if (it_c == context_to_map_.end()) {
      // This should never happen, as this is the inverse relation of stream
      // -> context mapping and we've found element of he latter.
      assert(false);
      return RemovalStatus::kRemoved;
    }
    // This is the mapping local -> global in the context that we've found.
    auto& local_map = it_c->second;
    {  // Remove the entry for this stream (local -> global).
      const auto it_l = local_map.find(local);
      if (it_l != local_map.end()) {
        // Thie mapping has to be consistent with whatever happened to be in
        // global -> (ctx, local) map.
        assert(it_l->second == global);
        local_map.erase(it_l);
      }
    }
    if (local_map.empty()) {
      // We've removed the last entry in the map for this context.
      // Remove the map as well.
      context_to_map_.erase(it_c);
      return RemovalStatus::kRemovedLast;
    }
    return RemovalStatus::kRemoved;
  }

  LocalMap RemoveContext(const Context& context) {
    thread_check_.Check();

    LocalMap local_map;
    {  // Remove ctx -> (local -> global).
      const auto it_c = context_to_map_.find(context);
      if (it_c != context_to_map_.end()) {
        local_map = std::move(it_c->second);
        context_to_map_.erase(it_c);
      }
    }
    // For each local -> global entry, remove corresponding entry from
    // global -> (ctx, local).
    for (const auto& entry : local_map) {
      StreamID global = entry.second;
      global_to_local_.erase(global);
    }
    return local_map;
  }

  size_t GetNumStreams() const {
    thread_check_.Check();
    return global_to_local_.size();
  }

  void Clear() {
    thread_check_.Reset();
    context_to_map_.clear();
    global_to_local_.clear();
  }

 private:
  ThreadCheck thread_check_;
  /** Allocator used when building mapping (ctx, local) -> global. */
  StreamAllocator allocator_;
  /** Maps context to a map from local (within this context) to global IDs. */
  std::unordered_map<Context, LocalMap> context_to_map_;
  /** Maps global back local IDs (and corresponding context). */
  std::unordered_map<StreamID, std::pair<Context, StreamID>> global_to_local_;
};

}  // namespace rocketspeed
