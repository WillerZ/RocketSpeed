// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <atomic>
#include <functional>
#include <mutex>
#include <unordered_map>

#include "include/Logger.h"
#include "include/Types.h"
#include "include/SubscriptionStorage.h"
#include "src/client/topic_id.h"
#include "src/port/port.h"
#include "src/util/common/env_options.h"
#include "src/util/common/thread_check.h"

namespace rocketspeed {

class MsgLoopBase;
class DescriptorEvent;
class Command;
class BaseEnv;

/**
 * Local file based subscription metadate storage.
 */
class FileStorage final : public SubscriptionStorage {
 public:
  FileStorage(BaseEnv* env,
              std::string read_path,
              std::shared_ptr<Logger> info_log);

  virtual ~FileStorage();

  void Initialize(LoadCallback load_callback, MsgLoopBase* msg_loop);

  Status ReadSnapshot() override;

  Status Update(SubscriptionRequest message) override;

  Status Load(std::vector<SubscriptionRequest> requests) override;

  Status LoadAll() override;

  void WriteSnapshot(SnapshotCallback callback) override;

 private:
  // Client's environment.
  BaseEnv* env_;
  // Client environment options.
  const EnvOptions env_options_;
  // Logger for info messages.
  const std::shared_ptr<Logger> info_log_;

  // Path to the read file.
  const std::string read_path_;

  // Client's message loop, storage does not own it.
  MsgLoopBase* msg_loop_;
  // Callback to be invoked with loaded subscription data.
  LoadCallback load_callback_;

  class SubscriptionState {
   public:
    explicit SubscriptionState(SequenceNumber seqno) : seqno_(seqno) {
    }

    explicit SubscriptionState(const SubscriptionState& state)
        : seqno_(state.seqno_) {
    }

    void IncreaseSequenceNumber(SequenceNumber new_seqno) {
      thread_check_.Check();
      if (seqno_ < new_seqno) {
        seqno_ = new_seqno;
      }
    }

    SequenceNumber GetSequenceNumber() const {
      thread_check_.Check();
      return seqno_;
    }

   private:
    // Thread check, which asserts that only one thread is accessing this data.
    ThreadCheck thread_check_;
    // Last acknowledged sequence number.
    SequenceNumber seqno_;
  };

  struct alignas(CACHE_LINE_SIZE) WorkerData {
    typedef std::unordered_map<TopicID, SubscriptionState> SubscriptionStateMap;
    // Topics serviced by this worker.
    SubscriptionStateMap topics_subscribed;

    // Lookups subscription state for given request.
    // Provided request might be modified during the call, but will be
    // reverted to original state, without invalidating any pointers.
    SubscriptionStateMap::iterator Find(SubscriptionRequest* request) {
      SubscriptionStateMap::iterator iter;
      // We do this to avoid copying topic name when doing the lookup.
      TopicID topic_id(request->namespace_id, std::move(request->topic_name));
      iter = topics_subscribed.find(topic_id);
      request->topic_name = std::move(topic_id.topic_name);
      return iter;
    }
  };

  // Holds state of a worker responsible for handling a subset of subscriptions.
  std::unique_ptr<WorkerData[]> worker_data_;

  void HandleUpdateCommand(std::unique_ptr<Command> command);

  void HandleLoadCommand(std::unique_ptr<Command> command);

  void HandleSnapshotCommand(std::unique_ptr<Command> command);

  // Returns index of a MsgLoop worker, which handles updates for given topic.
  int GetWorkerForTopic(const Topic& topic_name) const;

  // Returns WorkerData for calling MsgLoop worker.
  WorkerData& GetCurrentWorkerData();
};

}  // namespace rocketspeed
