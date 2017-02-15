// Copyright (c) 2017, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once  
  
#include <glog/logging.h> 
#include <ratio>  

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC visibility push(default)
#endif
  
namespace rocketspeed {
/**
 * A collection of STL containers wrappers managing removal of unused capacity.
 */
  
/**
 * For std::unordered_*-like (unordered associative) containers, if the 
 * container's load factor falls below than threshold * max_load_factor(), it 
 * shrinks the bucket size to be double of the current number of elements 
 * divided by max_load_factor().  
 */
template <typename Threshold> 
struct hash_default_shrink_policy { 
  constexpr static float threshold() {  
    return float(Threshold::num) / float(Threshold::den); 
  } 
  
  constexpr static bool checkPolicy() { 
    static_assert(  
        0.f <= threshold() && threshold() < 0.5f, 
        "threshold must be in [0, 1/2), preferably less than or equal to 1/4"); 
    return true;  
  } 
  
  template <class Container>  
  static void shrink(Container& container) {  
    if (container.load_factor() >= container.max_load_factor() * threshold()) { 
      return; 
    } 
    container.rehash(static_cast<typename Container::size_type>(  
        container.size() * 2.f / container.max_load_factor())); 
  } 
};  
  
/**
 * Default deallocation policy for std::vector-like (dynamic contiguous) containers.
 * If the container's occupancy (size / capacity) falls below than the threshold,
 * it shrinks the capacity to be twice of the current size.  
 */
template <typename Threshold> 
struct vector_default_shrink_policy { 
  static constexpr float threshold() {  
    return float(Threshold::num) / float(Threshold::den); 
  } 
  
  constexpr static bool checkPolicy() { 
    static_assert(  
        0.f <= threshold() && threshold() < 0.5f, 
        "threshold must be in [0, 1/2), preferably less than or equal to 1/4"); 
    return true;  
  } 
  
  template <class Container>  
  static void shrink(Container& container) {  
    if (container.size() >= container.capacity() * threshold()) { 
      return; 
    } 
    Container tmp;  
    tmp.reserve(container.size() * 2);  
    tmp.swap(container);  
  } 
};  
  
namespace detail {  
template <class Container, class ShrinkingPolicy> 
class auto_shrinkable_base : public Container { 
  static_assert(ShrinkingPolicy::checkPolicy(), "policy integrity check");  
  
 public:  
  constexpr float threshold() const { 
    return ShrinkingPolicy::threshold();  
  } 
  
  void clear() {  
    Container::clear(); 
    ShrinkingPolicy::shrink(*this); 
  } 
};  
} 
  
/**
 * std::unordered_*-like (unordered associative) containers wrapper.
 */
template <  
    class Container,  
    class ShrinkingPolicy = hash_default_shrink_policy<std::ratio<1, 4>>> 
class auto_shrinkable_hash  
    : public detail::auto_shrinkable_base<Container, ShrinkingPolicy> { 
 public:  
  using size_type = typename Container::size_type;  
  using iterator = typename Container::iterator;  
  using const_iterator = typename Container::const_iterator;  
  using key_type = typename Container::key_type;  
  
  // The return type is 'void', which is different from, e.g., "iterator  
  // unordered_map::erase()" that returns the iterator following the  
  // removed one. If shrinking occurs, iterators will be invalidated. 
  // As containers cannot always guarantee the ordering (e.g., unordered_map),  
  // we do not return the next iterator.  
  void erase(const_iterator pos) {  
    Container::erase(pos);  
    ShrinkingPolicy::shrink(*this); 
  } 
  
  void erase(const_iterator first, const_iterator last) { 
    // Also, void return here.  
    Container::erase(first, last);  
    ShrinkingPolicy::shrink(*this); 
  } 
  
  size_type erase(const key_type& k) {  
    size_type ret = Container::erase(k);  
    ShrinkingPolicy::shrink(*this); 
    return ret; 
  } 
};  
  
/**
 * std::vector-like (dynamic contiguous) containers wrapper.
 */
template <  
    class Container,  
    class ShrinkingPolicy = vector_default_shrink_policy<std::ratio<1, 4>>> 
class auto_shrinkable_vector  
    : public detail::auto_shrinkable_base<Container, ShrinkingPolicy> { 
 public:  
  using size_type = typename Container::size_type;  
  using iterator = typename Container::iterator;  
  using const_iterator = typename Container::const_iterator;  
  using value_type = typename Container::value_type;  
  
  void erase(const_iterator pos) {  
    Container::erase(pos);  
    ShrinkingPolicy::shrink(*this); 
  } 
  
  void erase(const_iterator first, const_iterator last) { 
    Container::erase(first, last);  
    ShrinkingPolicy::shrink(*this); 
  } 
  
  void pop_back() { 
    Container::pop_back();  
    ShrinkingPolicy::shrink(*this); 
  } 
  
  void resize(size_type count) {  
    Container::resize(count); 
    ShrinkingPolicy::shrink(*this); 
  } 
  
  void resize(size_type count, const value_type& value) { 
    Container::resize(count, value);  
    ShrinkingPolicy::shrink(*this); 
  } 
};  
  
} // namespace rocketspeed 

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC visibility pop
#endif
