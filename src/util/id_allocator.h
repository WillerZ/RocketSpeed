//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <cassert>
#include <cstdio>
#include <memory>
#include <limits>
#include <vector>

namespace rocketspeed {

/**
 * An allocator for arbitrary type of numeric IDs.
 *
 * Each allocator has a set of allowed IDs and will only return IDs from it.
 * Current implementation always represents a range of IDs, allocates them one
 * by one and wraps around if runs out of unique IDs.
 */
template <typename IDType, typename AllocatorType>
class IDAllocator {
 public:
  // Noncopyable
  IDAllocator(const IDAllocator&) = delete;
  IDAllocator& operator=(const IDAllocator&) = delete;
  // Movable
  IDAllocator(IDAllocator&& other) noexcept { *this = std::move(other); }
  IDAllocator& operator=(IDAllocator&& other) {
    first_ = other.first_;
    end_ = other.end_;
    next_ = other.next_;
    // Invalidate other.
    other.first_ = kGlobalEnd;
    other.end_ = kGlobalFirst;
    return *this;
  }

  /**
   * Creates an allocator for entire ID space for given ID type.
   * Do not use this one if you want to reserve some special values that should
   * never be allocated.
   */
  IDAllocator() : IDAllocator(kGlobalFirst, kGlobalEnd) { CheckValid(); }

  /** Creates an allocator for given range of IDs. */
  IDAllocator(IDType first, IDType end)
      : first_(first), end_(end), next_(first_) {
    CheckValid();
  }

  IDType Next() {
    CheckValid();
    IDType res = next_;
    if (IsLast()) {
      next_ = first_;
    } else {
      ++next_;
    }
    return res;
  }

  bool IsLast() const {
    CheckValid();
    return next_ + 1 == end_;
  }

  bool IsEmpty() const {
    CheckValid();
    return first_ == end_;
  }

  bool IsSourceOf(IDType id) const {
    CheckValid();
    return first_ <= id && id < end_;
  }

  size_t TotalSize() const {
    CheckValid();
    return end_ - first_;
  }

  /**
   * Breaks allocator's set of remaining available IDs in two (as equal as
   * possible) subsets. This retains the lower part of the set.
   *
   * @return An allocator representing the upper part.
   */
  AllocatorType Split() {
    CheckValid();
    IDType mid = next_ + (end_ - next_) / 2;
    AllocatorType upper(mid, end_);
    upper.Next();
    end_ = mid;
    return upper;
  }

  /**
   * A mappign returned by Divide operation.
   *
   * Maps ID that belongs to allowed set for the divided allocator into and
   * index of one of the resulting allocators in the list. Provided ID belongs
   * to the set of allowed IDs on the allocator, whose index was returned.
   * Attempting to obtain mapping for ID which doesn't belong to the allowed set
   * of the divided allocator yields undefined behavior.
   */
  class DivisionMapping {
   public:
    DivisionMapping() = default;

    size_t operator()(IDType id) const {
      assert(id >= first_);
      size_t index = (id - first_) / min_size_;
      if (index >= num_pieces_) {
        index = num_pieces_ - 1;
      }
      return index;
    }

   private:
    friend class IDAllocator<IDType, AllocatorType>;

    /** First allowed ID of the whole range. */
    IDType first_;
    /** Total number of pieces resulting from the division. */
    size_t num_pieces_;
    /** Size of the smallest piece. */
    IDType min_size_;

    DivisionMapping(IDType first, size_t num_pieces, IDType min_size)
        : first_(first), num_pieces_(num_pieces), min_size_(min_size) {}
  };

  /**
   * Divides allocator's remaining range into (nearly) equally sized pieces.
   * This allocator remains unchanged after the operation, and it's set of
   * available IDs contains the union of sets of available IDs of
   * returned allocators. The converse is not necessarily true.
   *
   * @param mapping A mapping, from ID to index in returned list, see above.
   *                If null, no mapping will be returned.
   * @return A list of disjoint allocators, their union is included in this.
   */
  std::vector<AllocatorType> Divide(size_t num_pieces,
                                    DivisionMapping* mapping = nullptr) const {
    CheckValid();
    assert(num_pieces < TotalSize());
    // Whatever implementation we go with, we need the reverse mapping from
    // ID to the index in returned vector (given the original, divided
    // allocator) to be quickly computable
    std::vector<AllocatorType> allocs;
    allocs.reserve(num_pieces);
    const IDType min_size = static_cast<IDType>((end_ - first_) / num_pieces);
    IDType first = first_;
    IDType end;
    for (size_t piece = 0; piece < num_pieces; ++piece) {
      end = piece == num_pieces - 1 ? end_
                                    : static_cast<IDType>(first + min_size);
      assert(first_ <= first);
      assert(first <= end);
      assert(end <= end_);
      allocs.push_back(AllocatorType(first, end));
      first = end;
    }
    // Set the mapping appropriately.
    if (mapping) {
      *mapping = DivisionMapping(first_, num_pieces, min_size);
    }
    return allocs;
  }

 private:
  static constexpr IDType kGlobalFirst = std::numeric_limits<IDType>::min();
  static constexpr IDType kGlobalEnd = std::numeric_limits<IDType>::max();

  /** First ID available for this allocator. */
  IDType first_;
  /** The first ID not available for this allocator after first_. */
  IDType end_;
  /** Next ID that can be allocated. */
  IDType next_;

  void CheckValid() const {
    assert(first_ <= next_);
    assert(next_ <= end_);
  }
};

}  // namespace rocketspeed
