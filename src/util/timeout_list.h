// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include "src/util/common/linked_map.h"
#include <chrono>

namespace rocketspeed {
// A list of items which internally contains a timestamp using a steady_clock.
// Adding a new entry adds it to the back of the list with now() timestamp.
// Adding an existing entry, updates the internal timestamp and moves the entry
// to the back of the list.
//
// An application can call GetExpired or ProcessExpired with a timeout
// to get a list of expired items
//
// Simple usage example:
//
//   TimeoutList<std::string> tlist;
//   tlist.Add("red");
//   tlist.Add("green");
//   tlist.Add("blue");
//   tlist.Add("yellow");
//   // 20 seconds later we add/update "red"
//   tlist.Add("red"); // this moves it to the back and updates the timestamp
//
//   std::vector<std::string> expired;
//   tlist.GetExpired(std::chrono::seconds(10), back_inserter(expired));
//   for (const auto& colour : expired) {
//      std::cout << '(' << colour << ") ";
//   }
//
// Output:
//
//   (green) (blue) (yellow)
//

template <class T> class TimeoutList {
 public:
  TimeoutList() {}
  TimeoutList(TimeoutList&&) = default;
  TimeoutList& operator= (TimeoutList&&) = default;

  TimeoutList(const TimeoutList&) = delete;
  TimeoutList& operator= (const TimeoutList&) = delete;

  // Capacity
  bool Empty() const { return lmap_.empty(); }
  size_t Size() const { return lmap_.size(); }

  /**
  * Adds a new item with a time_point = now()
  * or updates an existing item to now() and moves to back of the list
  */
  void Add(const T& t) {
    auto it = lmap_.find(t);
    if (it == lmap_.end()) {
      // add this item
      lmap_.emplace_back(t, std::chrono::steady_clock::now());
    } else {
      // update the item's time and move to the end
      it->second = std::chrono::steady_clock::now();
      lmap_.move_to_back(it);
    }
  }

  /**
  * Erases the entry if found
  */
  bool Erase(const T& t) {
    return lmap_.erase(t);
  }

  /**
  * gets a list of expired T items, if the elapsed time is > timeout.
  * This method also deletes the expired items from the internal list.
  *
  * @param timeout Elapsed time to evict an entry
  * @param out     OutputIterator to write the evicted/expired entries to
  */
  template <class OutputIterator, class Rep, class Period>
  void GetExpired(const std::chrono::duration<Rep, Period>& timeout,
                   OutputIterator out) {
    auto tm_now = std::chrono::steady_clock::now();
    auto it = lmap_.begin();
    while (it != lmap_.end()) {
      if ((tm_now - it->second) <= timeout) {
        // no more to process, as this is in ascending order
        return;
      }
      *out++ = std::move(it->first);
      it = lmap_.erase(it);
    }
  }

  /**
  * Invokes a callback for each expired item if elapsed time > timeout
  * This method also deletes the expired items from the internal list.
  *
  * @param timeout  Elapsed time to evict an entry
  * @param callback Callback invoked for each evicted/expired entry,
  *                 should take a T parameter e.g. [](T expired_item) {...}
  * @param batch_limit
  *                 limit the number of entries to be expired & processed.
  *                  -1 exipres all qualified entries.
  */
  template <class Rep, class Period, class ExpiryCallback>
  void ProcessExpired(const std::chrono::duration<Rep, Period>& timeout,
                      ExpiryCallback callback,
                      int batch_limit) {
    auto tm_now = std::chrono::steady_clock::now();
    auto it = lmap_.begin();
    while (it != lmap_.end() && (batch_limit < 0 || batch_limit-- > 0)) {
      if ((tm_now - it->second) <= timeout) {
        // no more to process, as this is in ascending order
        return;
      }
      callback(std::move(it->first));
      it = lmap_.erase(it);
    }
  }

 private:
  LinkedMap<T, std::chrono::steady_clock::time_point> lmap_;

};

} // namespace rocketspeed
