// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <deque>
#include <mutex>
#include <type_traits>

namespace rocketspeed {

/**
 * A simple object pooling allocator, constructed from an intrusive linked list.
 *
 * The type T to be pooled should inherit PooledObject<T>.
 *
 * Then construct a PooledObjectList<T> to manage the pool.
 *
 * When the free list is empty, objects are allocated via new, however, when
 * objects are free'd they go into the pool instead of being deleted.
 * Subsequent object allocations will use the pool objects first instead of
 * using new.
 *
 * Objects are only finally freed when the PooledObjectList is destroyed.
 *
 * e.g.
 *
 * struct MyObject : public PooledObject<MyObject> {
 *   MyObject(int x, int y, int z) { ... }
 *   ...
 * };
 *
 * struct MyObjectManager {
 *   MyObject* AllocObject(int x, int y, int z) {
 *     return obj_pool_.Allocate(x, y, z);
 *   }
 *
 *   void FreeObject(MyObject* obj) {
 *     obj_pool_.Deallocate(obj);
 *   }
 * };
 *
 */

template <typename T>
class PooledObjectList;

/**
 * Objects that are to be pool allocated should inherit from PooledObject.
 */
template <typename T>
class PooledObject {
  // Only the pool list can access the tail.
  friend struct PooledObjectList<T>;
  T* tail_ = nullptr;
};

/**
 * The actual pool of objects.
 * Not thread safe.
 */
template <typename T>
class PooledObjectList {
  static_assert(std::is_base_of<PooledObject<T>, T>::value,
                "T should be a subclass of PooledObject<T>.");
 public:
  PooledObjectList() = default;

  template <typename... Args>
  T* Allocate(Args&&... args) {
    if (T* obj = head_) {
      head_ = head_->tail_;
      *obj = T(std::forward<Args>(args)...);
      return obj;
    } else {
      pool_.emplace_back(std::forward<Args>(args)...);
      return &pool_.back();
    }
  }

  void Deallocate(T* obj) {
    obj->tail_ = head_;
    head_ = obj;
  }

  // Noncopyable
  PooledObjectList(const PooledObjectList&) = delete;
  PooledObjectList operator= (const PooledObjectList&) = delete;

 private:
  T* head_ = nullptr;
  std::deque<T> pool_;
};

/**
 * Thread safe version of PooledObjectList.
 *
 * Currently uses a lock, but should be possible to write a lock-free version
 * of this if necessary.
 */
template <typename T>
struct SharedPooledObjectList {
 public:
  template <typename... Args>
  T* Allocate(Args&&... args) {
    std::lock_guard<std::mutex> lock(mutex_);
    return list_.Allocate(std::forward<Args>(args)...);
  }

  void Deallocate(T* obj) {
    std::lock_guard<std::mutex> lock(mutex_);
    list_.Deallocate(obj);
  }

 private:
  std::mutex mutex_;
  PooledObjectList<T> list_;
};

}  // namespace
