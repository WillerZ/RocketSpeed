#pragma once
#include <functional>
#include <iterator>
#include <list>
#include <unordered_map>
#include <utility>
#include "include/Assert.h"

namespace rocketspeed {

// A hashmap that maintains a doubly-linked list running through all of its
// entries. This linked list defines the iteration ordering,
// which is normally the order in which entries were inserted into the map.
//
// Simple usage example:
//
//   LinkedMap<int, int> map;
//   map.emplace_back(1, 101);
//   map.emplace_back(2, 102);
//   map.emplace_back(3, 103);
//   map.move_to_front(map.find(3));
//   for (const auto& entry : map) {
//     std::cout << '(' << entry.first << ',' << entry.second << ") ";
//   }
//
// Output:
//
//   (3,103) (1,101) (2,102)
//
template <class Key, class Node, class KeyHash, class KeyEqual, class NodeToKey>
class LinkedBase {
 public:
  using List = std::list<Node>;

  using value_type = typename List::value_type;
  using iterator = typename List::iterator;
  using const_iterator = typename List::const_iterator;
  using reverse_iterator = typename List::reverse_iterator;
  using const_reverse_iterator = typename List::const_reverse_iterator;
  using insertion_result = typename std::pair<iterator, bool>;

  LinkedBase() {}
  LinkedBase(LinkedBase&&) = default;
  LinkedBase& operator= (LinkedBase&&) = default;

  LinkedBase(const LinkedBase&) = delete;
  LinkedBase& operator= (const LinkedBase&) = delete;

  // Iterators
  iterator begin() { return list_.begin(); }
  const_iterator begin() const { return list_.cbegin(); }
  const_iterator cbegin() const { return list_.cbegin(); }
  reverse_iterator rbegin() { return list_.rbegin(); }
  const_reverse_iterator rbegin() const { return list_.crbegin(); }
  const_reverse_iterator crbegin() const { return list_.crbegin(); }
  iterator end() { return list_.end(); }
  const_iterator end() const { return list_.cend(); }
  const_iterator cend() const { return list_.cend(); }
  reverse_iterator rend() { return list_.rend(); }
  const_reverse_iterator rend() const { return list_.crend(); }
  const_reverse_iterator crend() const { return list_.crend(); }

  // Capacity
  bool empty() const { return list_.empty(); }
  size_t size() const { return list_.size(); }

  // Element access
  value_type& front() { return list_.front(); }
  const value_type& front() const { return list_.front(); }
  value_type& back() { return list_.back(); }
  const value_type& back() const { return list_.back(); }

  // Element lookup
  iterator find(const Key& key) {
    const auto it = index_.find(&key);
    if (it == index_.end()) {
      return list_.end();
    }
    return it->second;
  }

  const_iterator find(const Key& key) const {
    const auto it = index_.find(&key);
    if (it == index_.end()) {
      return list_.end();
    }
    return it->second;
  }

  bool contains(const Key& key) const {
    return index_.count(&key) > 0;
  }

  // Modifiers
  void clear() {
    list_.clear();
    index_.clear();
  }

  template <class P>
  typename std::enable_if<
      std::is_constructible<value_type, P&&>::value,
      insertion_result
    >::type
  insert(iterator pos, P&& element) {
    return emplace(pos, std::forward<P>(element));
  }

  template <class... Args>
  typename std::enable_if<
      std::is_constructible<value_type, Args&&...>::value,
      insertion_result
    >::type
  emplace(iterator pos, Args&&... args) {
    const auto it = list_.emplace(pos, std::forward<Args>(args)...);
    try {
      const auto emplace_result = index_.emplace(&node_to_key(*it), it);
      if (emplace_result.second) {
        return { it, true };
      } else {
        list_.erase(it);
        return { emplace_result.first->second, false };
      }
    } catch (...) {
      list_.erase(it);
      throw;
    }
  }

  iterator erase(iterator pos) {
    index_.erase(&node_to_key(*pos));
    return list_.erase(pos);
  }

  bool erase(const Key& key) {
    const auto it = index_.find(&key);
    if (it == index_.end()) {
      return false;
    }
    list_.erase(it->second);
    index_.erase(it);
    return true;
  }

  template <class P>
  typename std::enable_if<
      std::is_constructible<value_type, P&&>::value,
      insertion_result
    >::type
  push_front(P&& element) {
    return emplace_front(std::forward<P>(element));
  }

  template <class P>
  typename std::enable_if<
      std::is_constructible<value_type, P&&>::value,
      insertion_result
    >::type
  push_back(P&& element) {
    return emplace_back(std::forward<P>(element));
  }

  template <class... Args>
  typename std::enable_if<
      std::is_constructible<value_type, Args&&...>::value,
      insertion_result
    >::type
  emplace_front(Args&&... args) {
    return emplace(begin(), std::forward<Args>(args)...);
  }

  template <class... Args>
  typename std::enable_if<
      std::is_constructible<value_type, Args&&...>::value,
      insertion_result
    >::type
  emplace_back(Args&&... args) {
    return emplace(end(), std::forward<Args>(args)...);
  }

  void pop_front() {
    RS_ASSERT(!empty());
    erase(begin());
  }

  void pop_back() {
    RS_ASSERT(!empty());
    erase(std::prev(end()));
  }

  void swap(LinkedBase& other) {
    list_.swap(other.list_);
    index_.swap(other.index_);
  }

  // Operations

  // Move the element pointed to by it to position before
  // the element pointed by pos.
  void move_to(iterator it, iterator pos) { list_.splice(pos, list_, it); }
  void move_to_front(iterator it) { move_to(it, begin()); }
  void move_to_back(iterator it) { move_to(it, end()); }

 private:
  struct KeyPtrHash {
    size_t operator() (const Key* key) const {
      return key_hash(*key);
    }
    KeyHash key_hash;
  };

  struct KeyPtrEqual {
    bool operator() (const Key* left, const Key* right) const {
      return key_equal(*left, *right);
    }
    KeyEqual key_equal;
  };

  using Index = std::unordered_map<
      const Key*, iterator, KeyPtrHash, KeyPtrEqual>;

  List list_;
  Index index_;
  NodeToKey node_to_key;
};

struct LinkedMapKey {
  template <class A, class B>
  const A& operator()(const std::pair<const A, B>& p) const {
    return p.first;
  }
};

struct LinkedSetKey {
  template <class T>
  const T& operator()(const T& p) const {
    return p;
  }
};

template <class Key,
          class Value,
          class KeyHash = std::hash<Key>,
          class KeyEqual = std::equal_to<Key>>
using LinkedMap =
  LinkedBase<Key, std::pair<const Key, Value>, KeyHash, KeyEqual, LinkedMapKey>;

template <class Key,
          class KeyHash = std::hash<Key>,
          class KeyEqual = std::equal_to<Key>>
using LinkedSet =
  LinkedBase<Key, Key, KeyHash, KeyEqual, LinkedSetKey>;

} // namespace rocketspeed
