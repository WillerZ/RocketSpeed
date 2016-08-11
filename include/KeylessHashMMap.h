// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>

#include "Assert.h"
#include "TaggedPtr.h"

namespace rocketspeed {
template <typename T>
class SingleIntArray {
 public:
  static const bool implemented = false;
};

// "Container" for storing single pointer value
// with array interface required by KeylessHashMMap
template <typename T>
class SingleIntArray<T*> {
 public:
  static const bool implemented = true;

  // value used by google::sparse_hash_set to internally mark
  // erased elements.
  static SingleIntArray<T*> GetDeletedValue() {
    SingleIntArray<T*> invalid;
    invalid.ptr_ = kInvalidValue;
    return invalid;
  }

  bool IsDeletedValue() const { return kInvalidValue == ptr_; }

  // create empty object
  SingleIntArray() {}

  // create array with one element
  explicit SingleIntArray(T* ptr) { PushBack(ptr); }

  SingleIntArray(const SingleIntArray& other) { operator=(other); }

  SingleIntArray& operator=(const SingleIntArray& other) {
    ptr_ = other.ptr_;
    return *this;
  }

  void Clear() { ptr_ = reinterpret_cast<uintptr_t>(nullptr); }

  ~SingleIntArray() = default;

  static constexpr size_t MaxSize() { return 1; }

  T* operator[](size_t idx) const {
    RS_ASSERT(idx < Size());
    return reinterpret_cast<T*>(ptr_);
  }

  bool PushBack(T* ptr) const {
    RS_ASSERT(ptr != nullptr);
    if (Size() == MaxSize()) {
      return false;
    }
    ptr_ = reinterpret_cast<uintptr_t>(ptr);
    return true;
  }

  bool Erase(T* ptr) const {
    if (Empty()) {
      return false;
    }
    if (reinterpret_cast<uintptr_t>(ptr) == ptr_) {
      ptr_ = reinterpret_cast<uintptr_t>(nullptr);
      return true;
    }
    return false;
  }

  // It's important to have Size() == 0 for empty array
  uint16_t Size() const {
    return (ptr_ == reinterpret_cast<uintptr_t>(nullptr) ||
            ptr_ == kInvalidValue)
               ? 0
               : 1;
  }

  bool Empty() const { return Size() == 0; }

 private:
  static constexpr uintptr_t kInvalidValue = 1ULL;
  mutable uintptr_t ptr_ = reinterpret_cast<uintptr_t>(nullptr);
};

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
 * It uses 16 bytes + 8 * Size() bytes of memory.
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
    invalid.data_ = reinterpret_cast<Data*>(kInvalidValue);
    return invalid;
  }

  bool IsDeletedValue() const {
    return kInvalidValue == reinterpret_cast<uintptr_t>(data_);
  }

  // create empty array
  SmallIntArray() {}

  // create array with one element
  explicit SmallIntArray(T* ptr) { PushBack(ptr); }

  SmallIntArray(const SmallIntArray& other) { operator=(other); }

  // google::sparse_hash_set is pre C++11 and uses copy instead of move.
  // We own the array so we need to count references.
  SmallIntArray& operator=(const SmallIntArray& other) {
    if (this != std::addressof(other)) {
      data_ = other.data_;
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
      std::swap(data_, other.data_);
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
        delete data_;
      }
    }
    data_ = nullptr;
  }

  ~SmallIntArray() { Clear(); }

  static constexpr size_t MaxSize() {
    return std::numeric_limits<uint32_t>::max();
  }

  static constexpr size_t MaxRefs() {
    return std::numeric_limits<uint32_t>::max();
  }

  T* operator[](size_t idx) const {
    RS_ASSERT(idx < Size());
    return data_->arr[idx];
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
    data_->arr[Size() - 1] = ptr;
    if (Size() == 1) {
      IncRefCount();
      RS_ASSERT(GetRefCount() == 1) << GetRefCount();
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
      if (data_->arr[i] == ptr) {
        std::swap(data_->arr[i], data_->arr[Size() - 1]);
        Reallocate(Size() - 1, Size() - 1);
        return true;
      }
    }
    return false;
  }

  // It's important to have Size() == 0 for empty array (sic)
  // and kInvalidValue-array to make the code skipped all heap-array
  // specific things, like dereferencing, nicely.
  uint32_t Size() const {
    return (data_ == nullptr ||
            reinterpret_cast<uintptr_t>(data_) == kInvalidValue)
            ? 0
            : data_->size;
  }

  bool Empty() const { return Size() == 0; }

 private:

  template <typename U>
  friend std::ostream& operator<<(std::ostream& out,
                                  const SmallIntArray<U*>& arr);

  /**
   * Allocates new array and copy elements from the current one.
   * Deallocates current array iff it's nonempty and new one was successully
   * allocated.
   * Assings new array to data_->arr.
   * If the new array is bigger than to_copy elements the rest will be left
   * default contructed.
   * std::bad_alloc is populated and the array stays untouched in that case.
   *
   * @param  to_copy number of elements to copy from old array
   * @param  new_size of the array to be allocated
   */
  void Reallocate(uint32_t to_copy, uint32_t new_size) const {
    RS_ASSERT(to_copy <= Size());
    Data* new_data = nullptr;
    if (new_size) {
      new_data = (Data *) operator new (sizeof(Data) + sizeof(T*) * new_size);
      new_data->ref = new_data->size = 0;
    }
    if (to_copy > 0) {
      ::memcpy(new_data, data_, sizeof(Data) + to_copy * sizeof(T*));
    }
    delete data_; // there's nullptr in case there was no data
    data_ = new_data;
    SetSize(new_size);
  }

  uint32_t GetRefCount() const {
    if (Empty()) {
      return 0;
    }
    return data_->ref;
  }

  void IncRefCount() const {
    RS_ASSERT(GetRefCount() < MaxRefs());
    RS_ASSERT(!Empty());
    ++data_->ref;
  }

  void DecRefCount() const {
    RS_ASSERT(GetRefCount() > 0);
    RS_ASSERT(!Empty());
    --data_->ref;
  }

  void SetSize(uint32_t size) const {
    if (size == 0) {
      RS_ASSERT(data_ == nullptr);
      return;
    }
    RS_ASSERT(size <= MaxSize());
    RS_ASSERT(data_);
    data_->size = size;
  }

  // Note: conversion from uintptr_t to pointer and back
  // is implementation defined
  static constexpr uintptr_t kInvalidValue = 1ULL;

  struct Data {
    uint32_t ref;
    uint32_t size;
    T* arr[0];
  };
  mutable Data* data_ = nullptr;
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

template <typename Key, typename Value, typename Hash, typename ArrayImpl>
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
  size_t operator()(const ArrayImpl& array) const {
    if (store_->IsKeySet()) {
      RS_ASSERT(array.Empty());
      return hasher_.Hash(*store_->GetKey());
    }
    RS_ASSERT(!array.Empty());
    auto ha = hasher_.Hash(hasher_.ExtractKey(array[0]));
    return ha;
  }

  // equality operator
  bool operator()(const ArrayImpl& lhs, const ArrayImpl& rhs) const {
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

template <typename Impl, typename Array>
struct SetDeletedKey<Impl, Array, true> {
  void Set(Impl& impl) { impl.set_deleted_key(Array::GetDeletedValue()); }
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
          template <typename...> class HashSetImpl,
          typename ArrayImpl = SmallIntArray<Value>>
class KeylessHashMMap {
  using Impl = HashSetImpl<ArrayImpl, HashUtils<Key, Value, Hash, ArrayImpl>,
                           HashUtils<Key, Value, Hash, ArrayImpl>>;

 public:
  class Iterator;
  using value_type = Value;
  using key_type = Key;
  // TODO(dyniusz): we might use num_buckets / num_elements
  // NOTE(dyniusz): non-default hasher provided by the user
  //                doesn't work with copy semantics
  KeylessHashMMap()
      // HashUtils are copied
      : store_(new KeyStore<Key>),
        impl_(0, HashUtils<Key, Value, Hash, ArrayImpl>(Hash(), store_.get()),
              HashUtils<Key, Value, Hash, ArrayImpl>(Hash(), store_.get())),
        size_(0) {
    static_assert(ArrayImpl::implemented == true, "Type not yet supported");
    detail::SetDeletedKey<Impl, ArrayImpl, detail::HasSetter<Impl>::value>()
        .Set(impl_);
  }

  /**
   * Creates new map by copying all elements from the other.
   * std::bad_alloc might be thrown.
   * NOTE: After the copy maps aren't necessary the same in terms
   *       of memory layout and memory consumption. They are identical
   *       element-wise.
   *
   * @param other - copy-from map
   */
  KeylessHashMMap(const KeylessHashMMap& other) : KeylessHashMMap() {
    operator=(other);
  }

  /**
   * Copy-assignment operator.
   * std::bad_alloc might be thrown.
   * Note from copy ctor applies here as well.
   *
   * @param other - assign-from map
   * @return reference to this map
   */
  KeylessHashMMap& operator=(const KeylessHashMMap& other) {
    RS_ASSERT(!other.store_->IsKeySet());
    // I don't use sparse container operator because
    // there's no way to change its store_ pointer. After such copy
    // both maps would share the same store_. Inserting all elements instead.
    impl_.clear();
    for (auto it = other.impl_.begin(); it != other.impl_.end(); ++it) {
      impl_.insert(*it);
    }
    size_ = other.Size();
    return *this;
  }

  ~KeylessHashMMap() { Clear(); }

  /**
   * Tries to inserts value in a map.
   * Fails iff max number of elements limit was reached.
   * std::bad_alloc might be thrown.
   *
   * @param value - value to be added
   * @return true iff successully added
   */
  bool Insert(const Value& value) {
    ArrayImpl to_insert(value);
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
   * Removes entry from the map.
   * std::bad_alloc might be thrown.

   * @param value value to be erased
   */
  void Erase(Iterator it) {
    RS_ASSERT(!Empty());
    RS_ASSERT(&impl_ == it.owner_);
    RS_ASSERT(it.it_ != it.owner_->end());

    // TODO(dyniusz): this doesn't use the fact that we have the exact element
    // in hand already but searches the ArrayImpl. This might be inefficient
    // for large out of line arrays (multimaps). Unfortunately at the moment
    // ArrayImpl doesn't provide required interface to do it in a right way.
    const bool erased = it.it_->Erase(*it);
    RS_ASSERT(erased);
    if (it.it_->Empty()) {
      impl_.erase(it.it_);
    }
    --size_;
  }

  /**
   * Tries to erase value from the map (if there's any).
   * std::bad_alloc might be thrown.
   *
   * @param value value to be erased
   * @return true iff found (and erased)
   */
  bool Erase(const Value& value) {
    if (Empty()) {
      return false;
    }

    auto it = impl_.find(ArrayImpl(value));
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

    ScopedKeyWarden warden(store_.get(), &key);
    auto it = impl_.find(ArrayImpl());
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

  static constexpr size_t MaxElementsPerKey() { return ArrayImpl::MaxSize(); }

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

  /**
   * Swaps the content of two hash maps.
   *
   * @param other map to be swapped with.
   */
  void Swap(KeylessHashMMap& other) {
    RS_ASSERT(!store_->IsKeySet());
    RS_ASSERT(!other.store_->IsKeySet());
    std::swap(store_, other.store_);
    impl_.swap(other.impl_);
    std::swap(size_, other.size_);
  }

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

  std::unique_ptr<KeyStore<Key>> store_;
  Impl impl_;
  size_t size_;
};

template <typename KeyT>
struct StdEqualsAndHash {
  size_t Hash(const KeyT& k) const { return std::hash<KeyT>()(k); }
  bool Equals(const KeyT& k1, const KeyT& k2) const { return k1 == k2; }
};

}  // namespace rocketspeed
