// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#include "src/client/topic_subscription_map.h"

#include <tuple>

#include "include/RocketSpeed.h"
#include "include/Types.h"
#include "src/client/single_shard_subscriber.h"
#include <xxhash.h>

namespace rocketspeed {

TopicToSubscriptionMap::TopicToSubscriptionMap(
    std::function<bool(SubscriptionID, NamespaceID*, Topic*)> get_topic)
: get_topic_(std::move(get_topic))
, sub_count_low_(0)
, sub_count_high_(0)
, sub_count_(0) {}

SubscriptionID TopicToSubscriptionMap::Find(
    Slice namespace_id, Slice topic_name) const {
  if (vector_.empty()) {
    return SubscriptionID();
  }

  const size_t optimal_position = FindOptimalPosition(namespace_id, topic_name);
  // Scan through the vector until we reach a gap or make a full circle.
  size_t position = optimal_position;
  do {
    SubscriptionID sub_id = vector_[position];
    if (!sub_id) {
      // Reached the gap.
      return SubscriptionID();
    }

    NamespaceID this_namespace_id;
    Topic this_topic_name;
    bool success = get_topic_(sub_id, &this_namespace_id, &this_topic_name);
    RS_ASSERT(success);
    if (this_topic_name == topic_name && this_namespace_id == namespace_id) {
      // Found the right subscription ID.
      return sub_id;
    }
    // Namespace or topic don't match, move on.
    position = (position + 1) % vector_.size();
  } while (position != optimal_position);

  // Went through entire vector and didn't find the subscription.
  return SubscriptionID();
}

void TopicToSubscriptionMap::Insert(Slice namespace_id,
                                    Slice topic_name,
                                    SubscriptionID sub_id) {
  Rehash();
  InsertInternal(namespace_id, topic_name, sub_id);
}

bool TopicToSubscriptionMap::Remove(Slice namespace_id,
                                    Slice topic_name,
                                    SubscriptionID sub_id) {
  RS_ASSERT(sub_id);

  size_t position;
  {  // Find position of an entry with this ID.
    const size_t optimal_position =
        FindOptimalPosition(namespace_id, topic_name);
    position = optimal_position;
    do {
      if (!vector_[position] || vector_[position] == sub_id) {
        break;
      }
      position = (position + 1) % vector_.size();
    } while (position != optimal_position);
    if (sub_id != vector_[position]) {
      return false;
    }
  }

  RS_ASSERT(sub_count_ > 0);
  --sub_count_;

  // Ensure that there exists no element which is separated by a gap from its
  // optimal position.
  // We only need to inspect elements on positions (cyclically) between the
  // position of removed element and the next gap. Ad absurdum: if an
  // element after the next gap had been separated from its optimal position
  // by a gap, it would have been sparated by the gap before removal.
  size_t current_position = position;
  do {
    vector_[position] = SubscriptionID();
    current_position = (current_position + 1) % vector_.size();

    SubscriptionID current_id = vector_[current_position];
    if (!current_id) {
      break;
    }

    NamespaceID this_namespace_id;
    Topic this_topic_name;
    bool success = get_topic_(current_id, &this_namespace_id, &this_topic_name);
    RS_ASSERT(success);
    const auto x = FindOptimalPosition(this_namespace_id, this_topic_name);
    if (position <= current_position
            ? /* regular range */ (position < x && x <= current_position)
            : /* wrapped range */ (position < x || x <= current_position)) {
      continue;
    }

    vector_[position] = current_id;
    position = current_position;
  } while (true);

  Rehash();
  return true;
}

void TopicToSubscriptionMap::InsertInternal(Slice namespace_id,
                                            Slice topic_name,
                                            SubscriptionID sub_id) {
  RS_ASSERT(sub_id);
  RS_ASSERT(sub_count_ < vector_.size());
  RS_ASSERT(sub_count_ < sub_count_high_);

  const size_t optimal_position = FindOptimalPosition(namespace_id, topic_name);
  size_t position = optimal_position;
  do {
    if (vector_[position] == sub_id) {
      // The subscription ID have already been inserted with this or "similar"
      // key. Note that in this loop we scan all IDs for given key (and possibly
      // more), hence we will be able to detect duplicates, as long as they hash
      // to the same "optimal position".
      RS_ASSERT(false);
      return;
    }

    if (!vector_[position]) {
      vector_[position] = sub_id;
      ++sub_count_;
      return;
    }
    position = (position + 1) % vector_.size();
  } while (position != optimal_position);

  // Failed to find a spot, this (together with preconditions) means we failed
  // to properly rehash the vector or it somehow got corrupted.
  RS_ASSERT(false);
}

size_t TopicToSubscriptionMap::FindOptimalPosition(Slice namespace_id,
                                                   Slice topic_name) const {
  RS_ASSERT(!vector_.empty());
  const uint64_t seed = 0x57933c4a28a735b0;
  XXH64_state_t state;
  XXH64_reset(&state, seed);
  XXH64_update(&state, namespace_id.data(), namespace_id.size());
  XXH64_update(&state, topic_name.data(), topic_name.size());
  size_t hash = XXH64_digest(&state);
  return hash % vector_.size();
}

void TopicToSubscriptionMap::Rehash() {
  if (!NeedsRehash()) {
    // We're within the "load limit", no need to rehash.
    return;
  }

  // Configuration parameters.
  constexpr size_t kMinSize = 16;
  constexpr double kLoadFLow = 0.25, kLoadFHigh = 0.5;
  constexpr double kLoadFOpt = (kLoadFLow + kLoadFHigh) / 2.0;

  // Figure out the new size and recompute cached range, these calculations
  // might be heavy, but happen only if we actually need to rehash the vector.
  // This is becasue we cache "load limits" rather than "range of good sizes".
  size_t new_size = (size_t)((double)sub_count_ / kLoadFOpt);
  sub_count_low_ = (size_t)((double)new_size * kLoadFLow);
  // Account for lower bound on the vector size.
  if (new_size <= kMinSize) {
    new_size = kMinSize;
    sub_count_low_ = 0;
  }
  sub_count_high_ = (size_t)((double)new_size * kLoadFHigh);

  // Resize the vector and clear it.
  std::vector<SubscriptionID> old_vector(new_size);
  std::swap(old_vector, vector_);
#ifndef NO_RS_ASSERT
  std::unordered_set<SubscriptionID> seen_ids;
  const size_t old_sub_count = sub_count_;
#endif  // NO_RS_ASSERT
  sub_count_ = 0;
  // Reinsert all elements we expect to find in the hash set.
  for (SubscriptionID sub_id : old_vector) {
    if (sub_id) {
#ifndef NO_RS_ASSERT
      // Check for any duplicated subscription IDs.
      RS_ASSERT(seen_ids.emplace(sub_id).second);
#endif  // NO_RS_ASSERT
      NamespaceID namespace_id;
      Topic topic_name;
      bool success = get_topic_(sub_id, &namespace_id, &topic_name);
      RS_ASSERT(success);
      InsertInternal(namespace_id, topic_name, sub_id);
    }
  }

#ifndef NO_RS_ASSERT
  // The cached number of subscription IDs must equal the actual number.
  RS_ASSERT(sub_count_ == old_sub_count);
  RS_ASSERT(seen_ids.size() == old_sub_count);
#endif  // NO_RS_ASSERT
  // The cached load range must make any sense.
  RS_ASSERT(sub_count_low_ <= sub_count_high_);
  RS_ASSERT(sub_count_high_ < vector_.size());
  // The open hashing data structure must not require another rehashing.
  RS_ASSERT(!NeedsRehash());
  // We must be able to accommodate one extra element.
  RS_ASSERT(sub_count_ < sub_count_high_);
  RS_ASSERT(sub_count_ < vector_.size());
}

bool TopicToSubscriptionMap::NeedsRehash() const {
  return sub_count_low_ > sub_count_ || sub_count_ >= sub_count_high_;
}

}  // namespace rocketspeed
