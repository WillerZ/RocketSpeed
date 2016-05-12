/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#define __STDC_FORMAT_MACROS

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "external/folly/Memory.h"

#include "include/ProxyServer.h"

namespace rocketspeed {

namespace {

class DefaultAccumulator : public UpdatesAccumulator {
 public:
  using Action = UpdatesAccumulator::Action;
  using ConsumerCb = UpdatesAccumulator::ConsumerCb;

  explicit DefaultAccumulator(size_t count_limit) : count_limit_(count_limit) {}

  Action ConsumeUpdate(Slice contents,
                       SequenceNumber prev_seqno,
                       SequenceNumber current_seqno) override {
    if (prev_seqno == 0) {
      // We can just throw away all updates upon receiving a snapshot.
      updates_.clear();
    }
    RS_ASSERT(updates_.size() < count_limit_);
    // Remember be update in either case.
    updates_.emplace_back(
        Update{contents.ToString(), prev_seqno, current_seqno});
    return updates_.size() >= count_limit_ ? Action::kResubscribeUpstream
                                           : Action::kNoOp;
  }

  SequenceNumber BootstrapSubscription(
      SequenceNumber initial_seqno,
      const UpdatesAccumulator::ConsumerCb& consumer) override {
    // TODO(stupaq): do not ignore the initial seqno (for efficiency)
    for (const auto& update : updates_) {
      consumer(update.contents, update.prev_seqno, update.current_seqno);
    }
    return updates_.empty() ? 0 : updates_.back().current_seqno + 1;
  }

 private:
  const size_t count_limit_;

  // TODO(stupaq): this is not the most compact way of storing updates
  struct Update {
    std::string contents;
    SequenceNumber prev_seqno;
    SequenceNumber current_seqno;
  };
  std::vector<Update> updates_;
};

}  // namespace

std::unique_ptr<UpdatesAccumulator> UpdatesAccumulator::CreateDefault(
    size_t count_limit) {
  return folly::make_unique<DefaultAccumulator>(count_limit);
}

}  // namespace rocketspeed
