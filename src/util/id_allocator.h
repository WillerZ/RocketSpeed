//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
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
 * Each allocator identifies a set of allowed IDs and will only return IDs from
 * this set. Since the set is finite, every allocator will eventually return the
 * same ID twice. The allocator specialised for any numeric type has a period
 * equal to the size of the set of allowed IDs.
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
    step_ = other.step_;
    next_ = other.next_;
    // Invalidate other.
    other.step_ = 0;
    return *this;
  }

  /**
   * Creates an allocator for entire ID space for given ID type.
   * Do not use this one if you want to reserve some special values that should
   * never be allocated.
   */
  IDAllocator() : step_(1), next_(0) { CheckValid(); }

  IDAllocator(IDType step, IDType next) : step_(step), next_(next) {}

  /**
   * Returns next allowed ID, advances allocator.
   */
  IDType Next() {
    CheckValid();
    IDType curr = next_;
    next_ = static_cast<IDType>(next_ + step_);
    return curr;
  }

  bool IsSourceOf(IDType id) const {
    CheckValid();
    IDType step_mask = step_;
    --step_mask;
    return (next_ & step_mask) == (id & step_mask);
  }

  /**
   * Breaks allocator's set of remaining available IDs in two (as equal as
   * possible) subsets. This retains one of the parts, the other one is
   * returned.
   *
   * @return An allocator representing one of the parts.
   */
  AllocatorType Split() {
    CheckValid();
    auto allocs = Divide(2);
    assert(allocs.size() == 2);
    *this = std::move(allocs.front());
    return std::move(allocs.back());
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
    DivisionMapping() : shift_(-1), mask_(0) {}

    size_t operator()(IDType id) const {
      assert(shift_ >= 0);
      return (id >> shift_) & mask_;
    }

   private:
    friend class IDAllocator<IDType, AllocatorType>;

    /** Step of an allocator before splitting. */
    int shift_;
    /** Multiplicative difference between steps before and after splitting. */
    IDType mask_;

    DivisionMapping(int shift, IDType mask) : shift_(shift), mask_(mask) {}
  };

  /**
   * Divides allocator's remaining range into (nearly) equally sized pieces.
   * This allocator remains unchanged after the operation, and it's set of
   * available IDs contains the union of sets of available IDs of
   * returned allocators. The converse is not necessarily true.
   *
   * @param min_pieces Minimal number of pieces to split allocator into.
   * @param mapping A mapping, from ID to index in returned list, see above.
   *                If null, no mapping will be returned.
   * @return A list of disjoint allocators, their union is included in this.
   */
  std::vector<AllocatorType> Divide(size_t min_pieces,
                                    DivisionMapping* mapping = nullptr) const {
    CheckValid();
    // Find the actual number of pieces, which is always a power of two.
    IDType num_pieces = 1;
    IDType new_step = step_;
    while (num_pieces < min_pieces) {
      assert(new_step <= (kMax << 1));
      num_pieces = static_cast<IDType>(num_pieces << 1);
      new_step = static_cast<IDType>(new_step << 1);
    }

    std::vector<AllocatorType> allocs;
    allocs.reserve(num_pieces);

    // Normalise next value, as if the allocator was never used.
    IDType first = next_ & static_cast<IDType>(step_ - 1);

    AllocatorType piece(step_, first);
    for (size_t i = 0; i < num_pieces; ++i) {
      allocs.emplace_back(new_step, piece.next_);
      piece.Next();
    }

    // Set the mapping appropriately.
    if (mapping) {
      int shift = -1;
      IDType step = step_;
      while (step > 0) {
        ++shift;
        step = static_cast<IDType>(step >> 1);
      }
      *mapping = DivisionMapping(shift, num_pieces - 1);
    }
    return allocs;
  }

 private:
  static_assert(std::numeric_limits<IDType>::is_integer,
                "ID type must be integer type");
  static_assert(!std::numeric_limits<IDType>::is_signed,
                "ID type must be unsigned type");

  static constexpr IDType kMax = std::numeric_limits<IDType>::max();

  /** Step of this ID allocator. */
  IDType step_;
  /** Next ID that can be allocated. */
  IDType next_;

  void CheckValid() const {
    assert(step_ > 0);
    assert((step_ & (step_ - 1)) == 0);
  }
};

}  // namespace rocketspeed
