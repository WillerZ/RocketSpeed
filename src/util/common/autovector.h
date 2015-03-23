//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <iterator>
#include <memory>
#include <utility>
#include <vector>

namespace rocketspeed {

#ifdef ROCKETSPEED_LITE
template <class T, size_t kCapacity = 8>
class autovector : public std::vector<T> {};
#else

// autovector<T,Int=1> is a sequence container that implements small buffer
// optimization. It behaves similarly to std::vector, except until a certain
// number of elements are reserved it does not use the heap.
//
// Like standard vector, it is guaranteed to use contiguous memory. (So, after
// it spills to the heap all the elements live in the heap buffer.)
//
// Simple usage example:
//
//   autovector<int,2> vec;
//   vec.push_back(0); // Stored in-place on stack
//   vec.push_back(1); // Still on the stack
//   vec.push_back(2); // Switches to heap buffer.
//
template <typename T, size_t kCapacity = 8>
class autovector;

namespace detail {

template <typename T, size_t kCapacity>
class AutoVectorAllocator {
 public:
  using value_type = T;

  template <typename U>
  struct rebind {
    typedef AutoVectorAllocator<U, kCapacity> other;
  };

  AutoVectorAllocator() = default;

  AutoVectorAllocator(const AutoVectorAllocator&) = delete;

  AutoVectorAllocator& operator= (const AutoVectorAllocator&) = delete;

  T* allocate(size_t n, void* hint = nullptr) {
    if (n <= kCapacity) {
      return buffer();
    } else {
      return std::allocator<T>().allocate(n, hint);
    }
  }

  void deallocate(T* ptr, size_t n) {
    if (ptr != buffer()) {
      std::allocator<T>().deallocate(ptr, n);
    }
  }

 private:
  T* buffer() {
    return static_cast<T*>(static_cast<void*>(buffer_));
  }

  using Placeholder =
      typename std::aligned_storage<sizeof(T), alignof(T)>::type;

  Placeholder buffer_[kCapacity];
};

} // namespace detail


template <typename T, size_t kCapacity>
class autovector :
       private std::vector<T, detail::AutoVectorAllocator<T, kCapacity>>
{
 public:
  using Base = std::vector<T, detail::AutoVectorAllocator<T, kCapacity>>;

  typedef typename Base::value_type value_type;
  typedef typename Base::iterator iterator;
  typedef typename Base::const_iterator const_iterator;
  typedef typename Base::reverse_iterator reverse_iterator;
  typedef typename Base::const_reverse_iterator const_reverse_iterator;

  autovector() {
    reserve(kCapacity);
  }

  explicit autovector(size_t count, const T& value = T()) {
    reserve(kCapacity);
    resize(count, value);
  }

  autovector(const autovector& other) {
    reserve(kCapacity);
    assign(other.begin(), other.end());
  }

  autovector(autovector&& other)
    noexcept(noexcept(T(std::move(other[0]))) &&
             noexcept(other[0] = std::move(other[0])))
  {
    if (other.capacity() > kCapacity) {
      other.swap(*this);
      other.reserve(kCapacity);
    } else {
      reserve(kCapacity);
      assign(std::make_move_iterator(other.begin()),
             std::make_move_iterator(other.end()));
    }
  }

  autovector(std::initializer_list<T> list) {
    reserve(kCapacity);
    assign(list.begin(), list.end());
  }

  autovector& operator= (const autovector& other) {
    if (this != &other) {
      assign(other.begin(), other.end());
    }
    return *this;
  }

  autovector& operator= (autovector&& other) {
    if (this != &other) {
      if (other.capacity() > kCapacity && capacity() > kCapacity) {
        other.swap(*this);
      } else {
        assign(std::make_move_iterator(other.begin()),
               std::make_move_iterator(other.end()));
      }
    }
    return *this;
  }

  autovector& operator= (std::initializer_list<T> list) {
    assign(list.begin(), list.end());
  }

  using Base::begin;
  using Base::end;
  using Base::rbegin;
  using Base::rend;
  using Base::cbegin;
  using Base::cend;
  using Base::crbegin;
  using Base::crend;

  using Base::size;
  using Base::resize;
  using Base::capacity;
  using Base::empty;
  using Base::reserve;
  //using Base::shrink_to_fit(); // Hard to implement.

  using Base::operator[];
  using Base::at;
  using Base::front;
  using Base::back;
  using Base::data;

  using Base::assign;
  using Base::push_back;
  using Base::pop_back;
  using Base::insert;
  using Base::erase;
  using Base::clear;
  using Base::emplace_back;
  using Base::emplace;
  //using Base::swap; // It can't fully comply with the standard.

  bool operator== (const autovector& other) const {
    return static_cast<const Base&>(*this) == static_cast<const Base&>(other);
  }

  bool operator!= (const autovector& other) const {
    return static_cast<const Base&>(*this) != static_cast<const Base&>(other);
  }

  bool operator< (const autovector& other) const {
    return static_cast<const Base&>(*this) < static_cast<const Base&>(other);
  }

  bool operator> (const autovector& other) const {
    return static_cast<const Base&>(*this) > static_cast<const Base&>(other);
  }

  bool operator<= (const autovector& other) const {
    return static_cast<const Base&>(*this) <= static_cast<const Base&>(other);
  }

  bool operator>= (const autovector& other) const {
    return static_cast<const Base&>(*this) >= static_cast<const Base&>(other);
  }
};

#endif // ROCKETSPEED_LITE

}  // namespace rocketspeed
