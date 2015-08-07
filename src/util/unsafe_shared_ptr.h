//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <cstdint>

namespace rocketspeed {

/**
 * Implements similar reference-counting behavior to std::shared_ptr, but does
 * not guarantee thread safety. Use only on a single thread where performance
 * is critical!
 */
template <class T>
class UnsafeSharedPtr {
 public:
  /**
   * The pointed-to type.
   */
  typedef T element_type;

  /**
   * Constructs a null pointer.
   */
  UnsafeSharedPtr() {}

  /**
   * Constructs a null pointer.
   */
  explicit UnsafeSharedPtr(std::nullptr_t ptr) : UnsafeSharedPtr() {}

  /**
   * Shares a pointer, only releasing it when each `UnsafeSharedPtr` using
   * it has been destroyed.
   */
  UnsafeSharedPtr(const UnsafeSharedPtr& other) : metadata_{other.metadata_} {
    retain();
  }

  /**
   * Transfers ownership from another shared pointer.
   */
  UnsafeSharedPtr(UnsafeSharedPtr&& other) noexcept
      : metadata_{other.metadata_} {
    other.metadata_ = Metadata();
  }

  /**
   * Releases the shared pointer if this is the last reference.
   */
  ~UnsafeSharedPtr() { release(); }

  /**
   * Releases the reference to the current pointer, then shares a reference
   * to the given pointer instead.
   */
  UnsafeSharedPtr& operator=(const UnsafeSharedPtr& other) {
    reset(other);
    return *this;
  }

  /**
   * Transfers ownership from the given shared pointer.
   */
  UnsafeSharedPtr& operator=(UnsafeSharedPtr&& other) {
    release();

    metadata_ = other.metadata_;
    other.metadata_ = Metadata();

    return *this;
  }

  /**
   * Wraps the given pointer, which must be implicitly convertible to the
   * element type.
   */
  template <class U>
  explicit UnsafeSharedPtr(U* ptr) {
    reset(ptr);
  }

  template <class U, class... Args>
  friend UnsafeSharedPtr<U> MakeUnsafeSharedPtr(Args&&... args);

  /**
   * Shares a pointer, only releasing it when each `UnsafeSharedPtr` using
   * it has been destroyed.
   */
  template <class U>
  explicit UnsafeSharedPtr(const UnsafeSharedPtr<U>& other)
  : metadata_{other.metadata_} {
    retain();
  }

  /**
   * Releases the reference to the current pointer, then shares a reference
   * to the given pointer instead.
   */
  template <class U>
  UnsafeSharedPtr& operator=(const UnsafeSharedPtr<U>& other) {
    reset(other);
    return *this;
  }

  /**
   * Releases the reference to the current pointer, replacing it with NULL.
   *
   * If this was the last reference to the pointer, it will be deleted.
   */
  void reset() {
    release();
    metadata_ = Metadata();
  }

  /**
   * Releases the reference to the current pointer, then takes the given
   * pointer instead.
   */
  template <class U>
  void reset(U* ptr) {
    release();

    metadata_.pointer = ptr;
    metadata_.refcount = new refcount_t(1);
    metadata_.shared_allocation = false;
  }

  /**
   * Releases the reference to the current pointer, then shares a reference
   * to the given pointer instead.
   */
  template <class U>
  void reset(const UnsafeSharedPtr<U>& other) {
    release();

    metadata_ = other.metadata_;
    retain();
  }

  /**
   * Returns the currently-shared pointer.
   */
  T* get() const { return metadata_.pointer; }

  /**
   * Dereferences the current pointer. Behavior is undefined if the pointer
   * is null.
   */
  T& operator*() const { return *get(); }

  /**
   * Dereferences through the current pointer. Behavior is undefined if the
   * pointer is null.
   */
  T* operator->() const { return get(); }

  /**
   * Returns whether this is the only reference to the shared pointer.
   *
   * This will always be false for a null pointer.
   */
  bool unique() const {
    return metadata_.refcount && *(metadata_.refcount) == 1;
  }

 private:
  typedef uint64_t refcount_t;

  struct Metadata {
    T* pointer;
    refcount_t* refcount;

    // Whether `pointer` and `refcount` were allocated together in a
    // `SharedAllocation`. If this is false, the two pointers need to be deleted
    // separately.
    bool shared_allocation;

    Metadata()
    : pointer{nullptr}, refcount{nullptr}, shared_allocation{true} {}
  } metadata_;

  // Used to allocate metadata and storage for the shared object at the same
  // time.
  struct SharedAllocation {
    // This field MUST come first, so that deleting a pointer to the
    // SharedAllocation is equivalent to deleting a pointer to this field.
    T value;
    refcount_t refcount;

    SharedAllocation() = delete;

    template <class... Args>
    explicit SharedAllocation(Args&&... args)
    : refcount(1) {
      value = T(std::forward<Args>(args)...);
    }
  };

  /**
   * Increments the reference count. If none is being maintained, nothing
   * happens.
   */
  void retain() {
    if (metadata_.refcount) {
      (*metadata_.refcount)++;
    }
  }

  /**
   * Decrements the reference count, destroying the pointer and refcount
   * memory if there are no other references remaining.
   *
   * If no reference count is being maintained, nothing happens.
   */
  void release() {
    if (metadata_.refcount && --(*metadata_.refcount) == 0) {
      if (metadata_.shared_allocation) {
        delete reinterpret_cast<SharedAllocation*>(metadata_.pointer);
      } else {
        delete metadata_.pointer;
        delete metadata_.refcount;
      }
    }
  }
};

/**
 * Constructs an object of type T, using the given arguments, then returns a
 * shared pointer wrapping it.
 *
 * Just like std::make_shared_ptr(), this involves one less allocation than
 * constructing a T with `new` and passing it in.
 */
template <class T, class... Args>
UnsafeSharedPtr<T> MakeUnsafeSharedPtr(Args&&... args) {
  // Allocate storage for the pointer and the metadata together.
  auto shared = new typename UnsafeSharedPtr<T>::SharedAllocation(
      std::forward<Args>(args)...);

  auto pointer = UnsafeSharedPtr<T>();
  pointer.metadata_.pointer = &shared->value;
  pointer.metadata_.refcount = &shared->refcount;
  pointer.metadata_.shared_allocation = true;

  return pointer;
}

}  // namespace rocketspeed
