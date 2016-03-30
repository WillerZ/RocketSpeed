// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <map>
#include <memory>
#include "include/Assert.h"
#include "src/util/common/thread_check.h"
#include "src/util/common/noncopyable.h"
#include "src/util/common/nonmovable.h"

namespace rocketspeed {

template <class T>
class RefCountFlyweightFactory;

template <class T>
class RefCountFlyweight;

/**
 * Reference-Counted implementation of Flyweight design pattern.
 * Factory object.
 * Notes:
 * - Single-thread (or equivalent) usage for the factory and all its flyweights.
 *   Equivalent is usage from different threads with serialization guarantee.
 * - T must have a strict order operator< that doesn't throw exceptions.
 * - There is no restrictions on the flyweight vs. factory destruction order.
 */
template <typename T>
class RefCountFlyweightFactory : ThreadCheck, NonCopyable, NonMovable {
 public:
  RefCountFlyweightFactory();
  ~RefCountFlyweightFactory();

  /**
   * Returns Flyweight object for the corresponding T.
   * Creates new object if such T has never been requested from this factory or
   * all the references to it have been dropped.
   * Otherwise returns a reference to an existing object.
   */
  RefCountFlyweight<T> GetFlyweight(const T&);

 private:
  friend class RefCountFlyweight<T>;
  struct ObjectInfo;
  // don't use `unordered_map`! stable iterators are required
  typedef std::map<T, ObjectInfo> Map;
  std::shared_ptr<Map> map_;
};

/**
 * Reference-Counted implementation of Flyweight design pattern.
 * Flyweight object.
 * Notes:
 * - Guaranteed to be not greater than a raw pointer in a release build.
 */
template <class T>
class RefCountFlyweight : ThreadCheck {
 public:
  RefCountFlyweight();
  ~RefCountFlyweight();

  RefCountFlyweight(const RefCountFlyweight& rhs);
  RefCountFlyweight& operator=(const RefCountFlyweight& rhs);
  RefCountFlyweight(RefCountFlyweight&& rhs) noexcept;
  RefCountFlyweight& operator=(RefCountFlyweight&& rhs) noexcept;

  /**
   * Returns a reference to the object stored behind this flyweight.
   * Do not retain this reference longer than the original unchanged Flyweight.
   * Precondition: must be called on a non-empty Flyweight only.
   * An empty Flyweight can exist in the following cases:
   * - Default constructed and not assigned Flyweight;
   * - Flyweight that has been moved from;
   * - Assigned from other empty Flyweight.
   */
  const T& Get() const;

 private:
  friend class RefCountFlyweightFactory<T>;
  typedef typename RefCountFlyweightFactory<T>::ObjectInfo ObjectInfo;
  RefCountFlyweight(const ThreadCheck& thread_check, ObjectInfo* object_info);
  ObjectInfo* object_info_;
};

// implementation

// Note: ThreadCheck.Check is currently called in very few places because actual
//       users use this library from multiple threads with synchronisation.
//       It is a very fragile protection which may produce both false positives
//       and false negatives. But it is still better than nothing.

template <typename T>
struct RefCountFlyweightFactory<T>::ObjectInfo : NonCopyable, NonMovable {
  T data_;
  std::shared_ptr<Map> map_;
  std::size_t ref_count_;

  // Do always assign valid `iter_` right after construction!
  ObjectInfo(const T& data, std::shared_ptr<Map> map)
  : data_(data), map_(std::move(map)), ref_count_(0) {}

  void AddRef() {
    ++ref_count_;
    RS_ASSERT(ref_count_ != 0);
  }

  void Release() {
    RS_ASSERT(ref_count_ > 0);
    if (--ref_count_ == 0) {
      RS_ASSERT(!!map_);
      auto map = std::move(map_);
      // The following code deletes self. Do not access `this` during or after
      // the call.
      auto data = std::move(data_);
      map->erase(data);
    }
  }
};

template <typename T>
RefCountFlyweight<T> RefCountFlyweightFactory<T>::GetFlyweight(const T& data) {
  ThreadCheck::Check();
  auto i = map_->find(data);
  if (i == map_->end()) {
    i = map_->emplace(std::piecewise_construct,
                      std::forward_as_tuple(data),
                      std::forward_as_tuple(data, map_)).first;
  }
  return RefCountFlyweight<T>(*this, &i->second);
}

template <typename T>
RefCountFlyweightFactory<T>::RefCountFlyweightFactory()
: map_(std::make_shared<Map>()) {
}

template <typename T>
RefCountFlyweightFactory<T>::~RefCountFlyweightFactory() {
}

template <typename T>
RefCountFlyweight<T>::RefCountFlyweight()
: object_info_() {
}

template <typename T>
RefCountFlyweight<T>::~RefCountFlyweight() {
  if (object_info_) {
    object_info_->Release();
  }
}

template <typename T>
RefCountFlyweight<T>::RefCountFlyweight(const RefCountFlyweight& rhs)
: ThreadCheck(rhs), object_info_(rhs.object_info_) {
  if (object_info_) {
    object_info_->AddRef();
  }
}

template <typename T>
RefCountFlyweight<T>& RefCountFlyweight<T>::operator=(
    const RefCountFlyweight<T>& rhs) {
  ThreadCheck::operator=(rhs);
  if (object_info_) {
    object_info_->Release();
  }
  object_info_ = rhs.object_info_;
  if (object_info_) {
    object_info_->AddRef();
  }
  return *this;
}

template <typename T>
RefCountFlyweight<T>::RefCountFlyweight(RefCountFlyweight<T>&& rhs) noexcept
    : ThreadCheck(rhs),
      object_info_(rhs.object_info_) {
  rhs.object_info_ = nullptr;
}

template <typename T>
RefCountFlyweight<T>& RefCountFlyweight<T>::operator=(
    RefCountFlyweight<T>&& rhs) noexcept {
  ThreadCheck::operator=(rhs);
  if (object_info_) {
    object_info_->Release();
  }
  object_info_ = rhs.object_info_;
  rhs.object_info_ = nullptr;
  return *this;
}

template <typename T>
const T& RefCountFlyweight<T>::Get() const {
  ThreadCheck::Check();
  RS_ASSERT(!!object_info_);
  return object_info_->data_;
}

template <typename T>
RefCountFlyweight<T>::RefCountFlyweight(const ThreadCheck& thread_check,
                                        ObjectInfo* object_info)
: ThreadCheck(thread_check), object_info_(object_info) {
  RS_ASSERT(!!object_info_);
  object_info_->AddRef();
}

}  // namespace rocketspeed
