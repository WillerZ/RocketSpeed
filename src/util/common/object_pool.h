// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <atomic>
#include <memory>
#include <mutex>

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
struct PooledObjectList;

/**
 * Objects that are to be pool allocated should inherit from PooledObject.
 */
template <typename T>
struct PooledObject {
 private:
  // Only the pool list can access the tail.
  friend struct PooledObjectList<T>;

  PooledObject<T>* tail_ = nullptr;
};

/**
 * The actual pool of objects.
 * Not thread safe.
 */
template <typename T>
struct PooledObjectList {
 public:
  template <typename... Args>
  T* Allocate(Args&&... args) {
    if (T* obj = static_cast<T*>(head_.release())) {
      // List has an object, so use that memory.
      // Just construct a new object with it.
      head_.reset(obj->tail_);
      obj->tail_ = nullptr;
      *obj = T(std::forward<Args>(args)...);
      return obj;
    }
    // Free list is empty, so construct a new object.
    return new T(std::forward<Args>(args)...);
  }

  void Deallocate(T* obj) {
    obj->tail_ = head_.release();
    head_.reset(obj);
  }

  ~PooledObjectList() {
    // Delete in a loop.
    // Doing this recursively can cause stack overflow.
    PooledObject<T>* head = head_.release();
    while (head) {
      PooledObject<T>* next = head->tail_;
      delete head;
      head = next;
    }
  }

 private:
  std::unique_ptr<PooledObject<T>> head_;
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
  PooledObjectList<T> list_;
  std::mutex mutex_;
};

}  // namespace
