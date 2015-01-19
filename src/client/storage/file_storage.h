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

  void Initialize(LoadCallback load_callback,
                  UpdateCallback update_callback,
                  SnapshotCallback write_snapshot_callback,
                  MsgLoopBase* msg_loop);

  void Update(SubscriptionRequest message) override;

  void Load(std::vector<SubscriptionRequest> requests) override;

  void LoadAll() override;

  void WriteSnapshot() override;

  Status ReadSnapshot() override;

 private:
  // Client's environment.
  BaseEnv* env_;
  // Client environment options.
  const EnvOptions env_options_;
  // Logger for info messages.
  const std::shared_ptr<Logger> info_log_;

  // Path to the write file.
  const std::string write_path_;
  // Path to the read file.
  const std::string read_path_;

  // Client's message loop, storage does not own this.
  MsgLoopBase* msg_loop_;
  // Callback to be invoked with loaded subscription data.
  LoadCallback load_callback_;
  // Callback to be invoked with updated subscription data.
  UpdateCallback update_callback_;
  // Callback to be invoked when snapshot has been written or failed.
  SnapshotCallback write_snapshot_callback_;

  // Ensures that only one snapshot is taking place at the moment.
  std::atomic<bool> running_;

  class SubscriptionState {
   public:
    explicit SubscriptionState(SequenceNumber seqno) : seqno_(seqno) {}

    explicit SubscriptionState(const SubscriptionState& other)
        : seqno_(other.seqno_.load()) {}

    inline void IncreaseSequenceNumber(SequenceNumber new_seqno) {
      thread_check_.Check();
      // We can use relaxed memory order, since this thread is the only writer.
      SequenceNumber stored_seqno = seqno_.load(std::memory_order_relaxed);
      if (stored_seqno < new_seqno) {
        // Writer releases the value.
        seqno_.store(new_seqno, std::memory_order_release);
      }
    }

    inline SequenceNumber GetSequenceNumber() const {
      // Reader performs consume operation, since we do not want to order
      // operations on all memory locations, just this one.
      return seqno_.load(std::memory_order_consume);
    }

    inline SequenceNumber GetSequenceNumberRelaxed() const {
      thread_check_.Check();
      // The only modifications to the sequence number come from this thread.
      return seqno_.load(std::memory_order_relaxed);
    }

   private:
    // Thread check, which asserts that atomic operations with relaxed memory
    // ordering are correct.
    ThreadCheck thread_check_;
    // Last acknowledged sequence number.
    std::atomic<SequenceNumber> seqno_;
  };

  struct alignas(CACHE_LINE_SIZE) WorkerData {
    typedef std::unordered_map<TopicID, SubscriptionState> SubscriptionStateMap;
    // Topics serviced by this worker.
    SubscriptionStateMap topics_subscribed;
    // Mututal exclusion between writers and a threads doing scans.
    std::mutex topics_subscribed_mutex;

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

  std::unique_ptr<WorkerData[]> worker_data_;

  // Asserts no concurrent access to the following fields.
  ThreadCheck thread_check_;
  // Holds status of a snapshot if one is running.
  Status status_;
  // Number of writes to be completed before we can close a descriptor.
  int remaining_writes_;
  // File that we persist state to.
  std::unique_ptr<DescriptorEvent> descriptor_;

  void HandleUpdateCommand(std::unique_ptr<Command> command);

  void HandleLoadCommand(std::unique_ptr<Command> command);

  // Creates temp file to write snapshot, and starts writing.
  void InitiateSnapshot();

  // Finalizes disk snapshot after all data has been written to a temp file.
  void FinalizeSnapshot();

  // Returns index of a MsgLoop worker, which handles updates for given topic.
  int GetWorkerForTopic(const Topic& topic_name) const;

  // Returns WorkerData for calling MsgLoop worker.
  WorkerData& GetCurrentWorkerData();
};

}  // namespace rocketspeed
