// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>

#include "Assert.h"
#include "TaggedPtr.h"

namespace rocketspeed {

/**
 * Compact vector like structure for keeping integrals.
 */
template <typename T>
class SmallIntArray {
 public:
  static const bool implemented = false;
};

/**
 * Compact vector like structure for keeping pointers.
 * It uses 8 bytes + 8 * Size() of memory.
 * For each insertion and deletion reallocation happens.
 * NOTE: mutating methods marked const to be able to use the class as a Key in
 * sets & maps.
 */

template <typename T>
class SmallIntArray<T*> {
 public:
  static const bool implemented = true;

  // value used by google::sparse_hash_set to internally mark
  // erased elements.
  static SmallIntArray<T*> GetDeletedValue() {
    SmallIntArray<T*> invalid;
    invalid.array_ = reinterpret_cast<TaggedPtr<T>*>(kInvalidValue);
    return invalid;
  }

  bool IsDeletedValue() const {
    return kInvalidValue == reinterpret_cast<uintptr_t>(array_);
  }

  // create empty array
  SmallIntArray() {}

  // create array with one element
  explicit SmallIntArray(T* ptr) { PushBack(ptr); }

  SmallIntArray(const SmallIntArray& other) { operator=(other); }

  // google::sparse_hash_set is pre C++11 and uses copy instead of move.
  // We own the array so we need to count references. We use 4 MSBs
  // of arrays first TaggedPtr, rest (12 bites) are used for array size.
  SmallIntArray& operator=(const SmallIntArray& other) {
    if (this != std::addressof(other)) {
      if (other.GetRefCount() == other.MaxRefs()) {
        throw std::runtime_error("SmallIntArray refcount overflow");
      }

      array_ = other.array_;
      if (!Empty()) {
        IncRefCount();
      }
    }
    return *this;
  }

  SmallIntArray(SmallIntArray&& other) { operator=(std::move(other)); }

  // move transfers ownership
  SmallIntArray& operator=(SmallIntArray&& other) {
    if (this != std::addressof(other)) {
      Clear();
      std::swap(array_, other.array_);
      RS_ASSERT(Empty() || GetRefCount() > 0) << GetRefCount();
    }
    return *this;
  }

  // removes all elements
  void Clear() {
    RS_ASSERT(Empty() || GetRefCount() > 0);
    if (!Empty()) {
      DecRefCount();
      if (GetRefCount() == 0) {
        delete[] array_;
      }
    }
    array_ = nullptr;
  }

  ~SmallIntArray() { Clear(); }

  static constexpr size_t MaxSize() {
    return std::numeric_limits<Tag>::max() >> kNumRefBits;
  }

  const size_t MaxRefs() const { return (1ULL << (kNumRefBits + 1)) - 1; }

  T* operator[](size_t idx) const {
    RS_ASSERT(idx < Size());
    return array_[idx].GetPtr();
  }

  /**
   * Tries to add element at the end of the array.
   * Fails iff max number of elements limit was reached.
   * Allocation takes place on addition of every element besides first.
   *
   * @param ptr pointer to be added
   * @return true iff successully added
   */
  bool PushBack(T* ptr) const {
    if (Size() == MaxSize()) {
      return false;
    }
    Reallocate(Size(), Size() + 1);
    array_[Size() - 1].SetPtr(ptr);
    if (Size() == 1) {
      IncRefCount();
    }
    return true;
  }

  /**
   * Tries to erase given element by linear search and compare.
   * Allocation takes place on deletion of every element besides first.
   * std::bad_alloc is populated and the array stays untouched in that case.
   *
   * @param ptr pointer to be erased.
   * @return false iff element wasn't found.
   */
  bool Erase(T* ptr) const {
    if (Empty()) {
      return false;
    }

    for (size_t i = 0; i < Size(); ++i) {
      if (array_[i].GetPtr() == ptr) {
        if (i == 0) {
          array_[Size() - 1].SetTag(array_[0].GetTag());
        }
        std::swap(array_[i], array_[Size() - 1]);
        Reallocate(Size() - 1, Size() - 1);
        return true;
      }
    }
    return false;
  }

  // It's important to have Size() == 0 for empty array (sic)
  // and kInvalidValue-array to make the code skipped all heap-array
  // specific things, like dereferencing, nicely.
  typename TaggedPtr<T>::Tag Size() const {
    return (array_ == nullptr ||
            reinterpret_cast<uintptr_t>(array_) == kInvalidValue)
               ? 0
               : array_[0].GetTag() & ~kRefMask;
  }

  bool Empty() const { return Size() == 0; }

 private:
  using Tag = typename TaggedPtr<T>::Tag;

  template <typename U>
  friend std::ostream& operator<<(std::ostream& out,
                                  const SmallIntArray<U*>& arr);

  /**
   * Allocates new array and copy elements from the current one.
   * Deallocates current array iff it's nonempty and new one was successully
   * allocated.
   * Assings new array to array_.
   * If the new array is bigger than to_copy elements the rest will be left
   * default contructed.
   * std::bad_alloc is populated and the array stays untouched in that case.
   *
   * @param  to_copy number of elements to copy from old array
   * @param  new_size of the array to be allocated
   */
  void Reallocate(Tag to_copy, Tag new_size) const {
    RS_ASSERT(to_copy <= Size());
    auto new_array = new_size ? new TaggedPtr<T>[new_size] : nullptr;
    if (to_copy > 0) {
      ::memcpy(new_array, array_, to_copy * sizeof(TaggedPtr<T>));
      delete[] array_;
    }
    array_ = new_array;
    SetSize(new_size);
  }

  Tag GetRefCount() const {
    if (Empty()) {
      return 0;
    }
    return (array_[0].GetTag() & kRefMask) >> kNumSizeBits;
  }

  void IncRefCount() const {
    RS_ASSERT(GetRefCount() < MaxRefs());
    RS_ASSERT(!Empty());
    array_[0].SetTag(Tag((GetRefCount() + 1) << kNumSizeBits) | Size());
  }

  void DecRefCount() const {
    RS_ASSERT(GetRefCount() > 0);
    RS_ASSERT(!Empty());
    array_[0].SetTag(Tag((GetRefCount() - 1) << kNumSizeBits) | Size());
  }

  void SetSize(Tag size) const {
    if (size == 0) {
      RS_ASSERT(array_ == nullptr);
      return;
    }
    RS_ASSERT(size <= MaxSize());
    RS_ASSERT(array_);
    array_[0].SetTag(Tag(GetRefCount() << kNumSizeBits) | size);
  }

  static constexpr Tag kNumSizeBits = 12;
  static constexpr Tag kNumRefBits = 4;
  static constexpr Tag kRefMask = 0xf000;
  // Note: conversion from uintptr_t to pointer and back
  // is implementation defined
  static constexpr uintptr_t kInvalidValue = 1ULL;

  mutable TaggedPtr<T>* array_ = nullptr;
};

template <typename T>
std::ostream& operator<<(std::ostream& out, const SmallIntArray<T>& arr) {
  if (arr.IsDeletedValue()) {
    return out << "{DEL}";
  }
  return out << "{s:" << arr.Size() << ",r:" << arr.GetRefCount() << ","
             << arr.array_ << "}";
}

template <typename Key>
class KeyStore {
 public:
  const Key* GetKey() const {
    RS_ASSERT(key_);
    return key_;
  }

  bool IsKeySet() const { return key_ != nullptr; }

  void SetKey(const Key* key) {
    RS_ASSERT(!key_);
    key_ = key;
  }

  void ResetKey() {
    RS_ASSERT(key_);
    key_ = nullptr;
  }

 private:
  const Key* key_ = nullptr;
};

template <typename Key, typename Value, typename Hash>
class HashUtils {
 public:
  HashUtils(const Hash& hasher, const KeyStore<Key>* store)
      : hasher_(hasher), store_(store) {}
  HashUtils(const HashUtils& other)
      : hasher_(other.hasher_), store_(other.store_) {}
  // move == copy
  HashUtils(HashUtils&& other) : hasher_(other.hasher_), store_(other.store_) {}
  HashUtils& operator=(HashUtils&& other) {
    hasher_ = other.hasher_;
    store_ = other.store_;
    return *this;
  }

  // hashing operator
  size_t operator()(const SmallIntArray<Value>& array) const {
    if (store_->IsKeySet()) {
      RS_ASSERT(array.Empty());
      return hasher_.Hash(*store_->GetKey());
    }
    RS_ASSERT(!array.Empty());
    auto ha = hasher_.Hash(hasher_.ExtractKey(array[0]));
    return ha;
  }

  // equality operator
  bool operator()(const SmallIntArray<Value>& lhs,
                  const SmallIntArray<Value>& rhs) const {
    const bool lhs_deleted = lhs.IsDeletedValue();
    const bool rhs_deleted = rhs.IsDeletedValue();

    if (lhs_deleted || rhs_deleted) {
      return lhs_deleted && rhs_deleted;
    }

    RS_ASSERT(!lhs.Empty() || !rhs.Empty());
    const Key& key1 =
        lhs.Empty() ? *store_->GetKey() : hasher_.ExtractKey(lhs[0]);
    const Key& key2 =
        rhs.Empty() ? *store_->GetKey() : hasher_.ExtractKey(rhs[0]);
    return hasher_.Equals(key1, key2);
  }

 private:
  Hash hasher_;
  const KeyStore<Key>* store_;
};

namespace detail {

template <typename ImplT>
struct HasSetter {
  template <typename T>
  static uint8_t check(decltype(&T::set_deleted_key));
  template <typename T>
  static uint16_t check(...);

  static constexpr bool value = (sizeof(check<ImplT>(0)) == sizeof(uint8_t));
};

template <typename Impl, typename Value, bool>
struct SetDeletedKey {
  void Set(Impl& impl) {}
};

template <typename Impl, typename Value>
struct SetDeletedKey<Impl, Value, true> {
  void Set(Impl& impl) {
    impl.set_deleted_key(SmallIntArray<Value>::GetDeletedValue());
  }
};
}  // detail

/**
 * Memory optimized hash map for keeping pointers.
 * Doesn't keep keys but instead use user specified mapping.
 * It can be used as
 * multi-map or regular map depending on SmallIntArray implementation.
 * Use google::sparse_hash_set as HashSetImpl to have smallest memory footprint.
 *
 * Hash struct must implement 3 functions:
 *    // return Key for Value*
 * 1) const Key& ExtractKey(const Value* ptr) const;
 *    // return hash of the Key
 * 2) size_t Hash(const Key& Key) const;
 *    // test keys for equality
 * 3) bool Equals(const Key& key1, const Key& key2) const;
 *
 * The mapping must not change during the map existence.
 */
template <typename Key, typename Value, typename Hash,
          template <typename...> class HashSetImpl>
class KeylessHashMMap {
  using Impl = HashSetImpl<SmallIntArray<Value>, HashUtils<Key, Value, Hash>,
                           HashUtils<Key, Value, Hash>>;

 public:
  class Iterator;
  // TODO(dyniusz): we might use num_buckets / num_elements
  KeylessHashMMap(const Hash& hasher = Hash())
      // HashUtils are copied
      : impl_(0, HashUtils<Key, Value, Hash>(hasher, &store_),
              HashUtils<Key, Value, Hash>(hasher, &store_)),
        size_(0) {
    static_assert(SmallIntArray<Value>::implemented == true,
                  "Type not yet supported");
    detail::SetDeletedKey<Impl, Value, detail::HasSetter<Impl>::value>().Set(
        impl_);
  }

  /**
   * Tries to inserts value in a map.
   * Fails iff max number of elements limit was reached.
   * std::bad_alloc might be thrown.
   *
   * @param value - value to be added
   * @return true iff successully added
   */
  bool Insert(const Value& value) {
    SmallIntArray<Value> to_insert(value);
    auto it = impl_.find(to_insert);
    if (it == impl_.end()) {
      const bool inserted = impl_.insert(to_insert).second;
      RS_ASSERT(inserted);
      ++size_;
      return true;
    } else {
      const bool inserted = it->PushBack(value);
      size_ += inserted;
      return inserted;
    }
  }

  /**
   * Tries to erase value from the map (if there's any).
   * TODO(dyniusz): Erase(Iterator) implementation
   * std::bad_alloc might be thrown.
   *
   * @param value value to be erased
   * @return true iff found (and erased)
   */
  bool Erase(const Value& value) {
    if (Empty()) {
      return false;
    }

    auto it = impl_.find(SmallIntArray<Value>(value));
    if (it == impl_.end()) {
      return false;
    }
    RS_ASSERT(!it->Empty());

    const bool erased = it->Erase(value);
    if (it->Empty()) {
      impl_.erase(it);
    }
    RS_ASSERT(size_);
    size_ -= erased;
    return erased;
  }

  /**
   * Search for a value with specified key.
   *
   * @param key to be used for searching
   * @return iterator pointing to the value
   *         or equal to End() iterator if not found
   */

  Iterator Find(const Key& key) const {
    if (Empty()) {
      return End();
    }

    ScopedKeyWarden warden(&store_, &key);
    auto it = impl_.find(SmallIntArray<Value>());
    if (it == impl_.end()) {
      return End();
    }

    RS_ASSERT(!it->Empty());
    return Iterator(&impl_, it);
  }

  Iterator Begin() const { return Iterator(&impl_, impl_.begin()); }

  Iterator End() const { return Iterator(&impl_, impl_.end()); }

  size_t Size() const { return size_; }

  bool Empty() const { return !Size(); }

  void Clear() {
    impl_.clear();
    size_ = 0;
  }

  static constexpr size_t MaxElementsPerKey() {
    return SmallIntArray<Value>::MaxSize();
  }

  // TODO(dyniusz): iterator interface to be polished
  class Iterator {
   public:
    Iterator(const Iterator&) = default;
    Iterator& operator=(const Iterator&) = default;

    Iterator(Iterator&& other) : owner_(other.owner_) {
      operator=(std::move(other));
    }

    Iterator& operator=(Iterator&& other) {
      RS_ASSERT(owner_ == other.owner_);
      if (this != std::addressof(other)) {
        it_ = other.it_;
        idx_ = other.idx_;
        other.idx_ = 0;
        other.it_ = owner_->end();
      }
      return *this;
    }

    Iterator& operator++() {
      CheckValid(*this);
      if (++idx_ == it_->Size()) {
        ++it_;
        idx_ = 0;
      }
      return *this;
    }

    Value operator*() const {
      CheckValid(*this);
      return (*it_)[idx_];
    }

    bool operator==(const Iterator& other) const {
      return it_ == other.it_ && idx_ == other.idx_;
    }

    bool operator!=(const Iterator& other) const { return !operator==(other); }

   private:
    friend class KeylessHashMMap;

    void CheckValid(const Iterator& iter) const {
      RS_ASSERT(it_ != owner_->end() && idx_ < it_->Size());
    }

    explicit Iterator(const Impl* owner, typename Impl::const_iterator it)
        : owner_(owner), it_(it), idx_(0) {
      RS_ASSERT(it_ == owner_->end() || (!it_->Empty() && idx_ < it_->Size()));
    }

    const Impl* owner_;
    typename Impl::const_iterator it_;
    size_t idx_;
  };

 private:
  class ScopedKeyWarden {
   public:
    explicit ScopedKeyWarden(KeyStore<Key>* store, const Key* key)
        : store_(store) {
      store_->SetKey(key);
    }
    ~ScopedKeyWarden() { store_->ResetKey(); }

   private:
    KeyStore<Key>* store_;
  };

  // mutable since used in const Find()
  // via ScopedKeyWarden
  mutable KeyStore<Key> store_;
  Impl impl_;
  size_t size_;
};

}  // namespace rocketspeed
