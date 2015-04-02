// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <limits>
#include <string>
#include <unordered_map>

#include "include/Types.h"
#include "src/util/common/thread_check.h"

namespace rocketspeed {

class MsgLoop;

/**
 * Identifies a stream, which is a pair of unidirectional channels, one in each
 * direction. Messages flowing in one direction within given stream are linearly
 * ordered. Two messages flowing in opposite directions have no ordering
 * guarantees.
 * The ID uniquely identifies a stream within a single physical connection only,
 * that means if streams are multiplexed on the same connection and have the
 * same IDs, the IDs need to be remapped. The IDs do not need to be unique
 * system-wide.
 */
typedef std::string StreamID;

/** An allocator for stream IDs. Wraps around if runs out of unique IDs. */
class StreamAllocator {
  // TODO(stupaq) remove once we migrate to nueric stream IDs
  typedef uint64_t StreamIDNumeric;

 public:
  // Noncopyable
  StreamAllocator(const StreamAllocator&) = delete;
  StreamAllocator& operator=(const StreamAllocator&) = delete;
  // Movable
  // TODO(stupaq) actually we can add an assertion here, so that no one uses
  // allocator after it's been moved away from lvalue
  StreamAllocator(StreamAllocator&&) = default;
  StreamAllocator& operator=(StreamAllocator&&) = default;

  StreamID Next() {
    CheckValid();
    StreamIDNumeric res = next_;
    next_ = IsLast() ? first_ : next_ + 1;
    return std::to_string(res);
  }

  bool IsLast() const {
    CheckValid();
    return next_ + 1 == end_;
  }

  bool IsEmpty() const {
    CheckValid();
    return first_ == end_;
  }

  /**
   * Breaks allocator's set of remaining available stream IDs in two (as equal
   * as possible) pieces.
   */
  StreamAllocator Split() {
    CheckValid();
    StreamIDNumeric mid = next_ + (end_ - next_) / 2;
    StreamAllocator upper(mid, end_);
    upper.Next();
    end_ = mid;
    return upper;
  }

  /**
   * Divides allocator's remaining range into (nearly) equally sized pieces.
   * This allocator remains unchanged after the operation, and it's set of
   * available stream IDs contains the union of sets of available IDs of
   * returned allocators. The converse is not necessarily true.
   */
  std::vector<StreamAllocator> Divide(size_t num_pieces) const {
    CheckValid();
    // Whatever implementation we go with, we need the reverse mapping from
    // stream ID to the index in returned vector (given the original, divided
    // allocator) to be quickly computable
    std::vector<StreamAllocator> allocs;
    allocs.reserve(num_pieces);
    const StreamIDNumeric min_size = (end_ - first_) / num_pieces;
    StreamIDNumeric first = first_, end;
    for (size_t piece = 0; piece < num_pieces; ++piece) {
      end = first + min_size;
      assert(first_ <= first);
      assert(first <= end);
      assert(end <= end_);
      allocs.push_back(StreamAllocator(first, end));
      first = end;
    }
    return allocs;
  }

 private:
  friend class MsgLoop;

  static const StreamIDNumeric kGlobalFirst =
      std::numeric_limits<StreamIDNumeric>::min();
  static const StreamIDNumeric kGlobalEnd =
      std::numeric_limits<StreamIDNumeric>::max();

  /** Creates an allocator for entire stream ID space. */
  StreamAllocator() : StreamAllocator(kGlobalFirst, kGlobalEnd) {
  }

  /** Creates an allocator for given range of stream IDs. */
  StreamAllocator(StreamIDNumeric first, StreamIDNumeric end)
      : first_(first), end_(end), next_(first_) {
    CheckValid();
  }

  void CheckValid() const {
    assert(first_ <= next_);
    assert(next_ <= end_);
  }

  /** First stream ID available for this allocator. */
  StreamIDNumeric first_;
  /** The first stream ID not available for this allocator after first_. */
  StreamIDNumeric end_;
  /** Next stream ID that can be allocated. */
  StreamIDNumeric next_;
};

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

/** Keeps state of the stream as seen by its creator. */
class StreamSocket {
 public:
  /**
   * A unary predicate which returns true iff destination of its argument equals
   * given host. Note that it does not copy or take ownership of provided host,
   * so caller must ensure that the referent has longer lifetime than returned
   * predicate.
   */
  struct Equals {
    const ClientID& host;
    explicit Equals(const ClientID& _host) : host(_host) {
    }
    bool operator()(const StreamSocket& socket) const {
      return socket.GetDestination() == host;
    }
  };

  /** Creates socket which doesn't point to any stream. */
  StreamSocket() : is_open_(false) {
  }

  /**
   * Creates a socket representing a stream.
   * @param destination The destination client ID.
   * @param stream_id ID of the stream.
   */
  // TODO(stupaq) remove once everyone can allocate stream IDs
  StreamSocket(ClientID destination, StreamID stream_id)
      : destination_(std::move(destination))
      , stream_id_(stream_id)
      , is_open_(false) {
  }

  /**
   * Creates a socket representing a stream.
   * @param destination The destination client ID.
   * @param alloc Allocator, which will provide stream ID.
   */
  StreamSocket(ClientID destination, StreamAllocator* alloc)
      : destination_(std::move(destination))
      , stream_id_(alloc->Next())
      , is_open_(false) {
  }

  // Noncopyable
  StreamSocket(const StreamSocket&) = delete;
  StreamSocket& operator=(const StreamSocket&) = delete;
  // Movable
  StreamSocket(StreamSocket&&) = default;
  StreamSocket& operator=(StreamSocket&&) = default;

  bool IsValid() const {
    return !stream_id_.empty();
  }

  const ClientID& GetDestination() const {
    assert(IsValid());
    return destination_;
  }

  void Open() {
    assert(IsValid());
    is_open_ = true;
  }

  bool IsOpen() const {
    assert(IsValid());
    return is_open_;
  }

  StreamID GetStreamID() const {
    assert(IsValid());
    return stream_id_;
  }

 private:
  ClientID destination_;
  StreamID stream_id_;
  bool is_open_;
};

}  // namespace rocketspeed
