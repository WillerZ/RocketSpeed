// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "multi_threaded_subscriber.h"

#include <chrono>
#include <cmath>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "external/folly/Memory.h"
#include "external/folly/move_wrapper.h"

#include "include/Logger.h"
#include "include/RocketSpeed.h"
#include "include/Slice.h"
#include "include/Status.h"
#include "include/SubscriptionStorage.h"
#include "include/Types.h"
#include "src/client/multi_shard_subscriber.h"
#include "src/client/subscriber_stats.h"
#include "src/util/common/subscription_id.h"
#include "src/messages/event_callback.h"
#include "src/messages/event_loop.h"
#include "src/messages/msg_loop.h"
#include "src/messages/stream.h"
#include "src/port/port.h"
#include "src/util/common/processor.h"
#include "src/util/common/random.h"
#include "src/util/common/rate_limiter_sink.h"
#include "src/util/common/statistics.h"
#include "src/util/timeout_list.h"
#include "src/util/xxhash.h"

namespace rocketspeed {

///////////////////////////////////////////////////////////////////////////////
MultiThreadedSubscriber::MultiThreadedSubscriber(
    const ClientOptions& options, std::shared_ptr<MsgLoop> msg_loop)
: options_(options), msg_loop_(std::move(msg_loop)), next_sub_id_(0) {
  for (int i = 0; i < msg_loop_->GetNumWorkers(); ++i) {
    statistics_.emplace_back(std::make_shared<SubscriberStats>("subscriber."));
    subscribers_.emplace_back(new MultiShardSubscriber(
        options_, msg_loop_->GetEventLoop(i), statistics_.back()));
    subscriber_queues_.emplace_back(msg_loop_->CreateThreadLocalQueues(i));
  }
}

void MultiThreadedSubscriber::Stop() {
  // If the subscriber's loop has not been started, we can perform cleanup the
  // calling thread.
  if (msg_loop_->IsRunning()) {
    // Ensure subscribers are destroyed in the event_loop thread
    int nworkers = msg_loop_->GetNumWorkers();
    RS_ASSERT(nworkers == static_cast<int>(subscribers_.size()));

    port::Semaphore destroy_sem;
    std::atomic<int> count(nworkers);
    for (int i = 0; i < nworkers; ++i) {
      std::unique_ptr<Command> cmd(
          MakeExecuteCommand([this, &destroy_sem, &count, i]() {
            // We replace the underlying subscriber with a one that'll ignore
            // all calls.
            class NullSubscriber : public SubscriberIf {
              void StartSubscription(
                  SubscriptionID sub_id,
                  SubscriptionParameters parameters,
                  std::unique_ptr<Observer> observer) override{};

              void Acknowledge(SubscriptionID sub_id,
                               SequenceNumber seqno) override{};

              void TerminateSubscription(SubscriptionID sub_id) override{};

              bool Empty() const override { return true; };

              Status SaveState(SubscriptionStorage::Snapshot* snapshot,
                               size_t worker_id) override {
                return Status::InternalError("Stopped");
              };
            };
            subscribers_[i].reset(new NullSubscriber());
            if (--count == 0) {
              destroy_sem.Post();
            }
          }));
      msg_loop_->SendControlCommand(std::move(cmd), i);
    }
    destroy_sem.Wait();  // Wait until subscribers are destroyed
  }
}

MultiThreadedSubscriber::~MultiThreadedSubscriber() {
  RS_ASSERT(!msg_loop_->IsRunning());
}

SubscriptionHandle MultiThreadedSubscriber::Subscribe(
    Flow* flow,
    SubscriptionParameters parameters,
    std::unique_ptr<Observer> observer) {
  // Choose worker for this subscription and find appropriate queue.
  const auto worker_id = options_.thread_selector(msg_loop_->GetNumWorkers(),
                                                  parameters.namespace_id,
                                                  parameters.topic_name);
  RS_ASSERT(worker_id < subscriber_queues_.size());
  auto* worker_queue = subscriber_queues_[worker_id]->GetThreadLocal();

  // Create new subscription handle that encodes destination worker.
  const auto sub_handle = CreateNewHandle(worker_id);
  if (!sub_handle) {
    LOG_ERROR(options_.info_log, "Client run out of subscription handles");
    RS_ASSERT(false);
    return SubscriptionHandle(0);
  }
  auto sub_id = SubscriptionID::Unsafe(sub_handle);

  // Send command to responsible worker.
  auto moved_args = folly::makeMoveWrapper(
      std::make_tuple(std::move(parameters), std::move(observer)));
  std::unique_ptr<Command> command(
      MakeExecuteCommand([this, sub_id, moved_args, worker_id]() mutable {
        subscribers_[worker_id]->StartSubscription(
            sub_id,
            std::move(std::get<0>(*moved_args)),
            std::move(std::get<1>(*moved_args)));
      }));
  if (flow) {
    flow->Write(worker_queue, command);
  } else {
    if (!worker_queue->TryWrite(command)) {
      return SubscriptionHandle(0);
    }
  }
  return sub_handle;
}

bool MultiThreadedSubscriber::Unsubscribe(Flow* flow,
                                          SubscriptionHandle sub_handle) {
  if (!sub_handle) {
    LOG_ERROR(options_.info_log,
              "Cannot unsubscribe unengaged subscription handle.");
    return true;
  }

  // Determine corresponding worker, its queue, and subscription ID.
  const auto worker_id = GetWorkerID(sub_handle);
  if (worker_id < 0) {
    LOG_ERROR(options_.info_log, "Invalid worker encoded in the handle");
    return true;
  }
  auto* worker_queue = subscriber_queues_[worker_id]->GetThreadLocal();
  auto sub_id = SubscriptionID::Unsafe(sub_handle);

  // Send command to responsible worker.
  std::unique_ptr<Command> command(
      MakeExecuteCommand([this, sub_id, worker_id]() mutable {
        subscribers_[worker_id]->TerminateSubscription(sub_id);
      }));

  if (flow) {
    flow->Write(worker_queue, command);
  } else {
    if (!worker_queue->TryWrite(command)) {
      return false;
    }
  }
  return true;
}

bool MultiThreadedSubscriber::Acknowledge(Flow* flow,
                                          const MessageReceived& message) {
  const SubscriptionHandle sub_handle = message.GetSubscriptionHandle();
  if (!sub_handle) {
    LOG_ERROR(options_.info_log,
              "Cannot unsubscribe unengaged subscription handle.");
    return true;
  }

  // Determine corresponding worker, its queue, and subscription ID.
  const auto worker_id = GetWorkerID(sub_handle);
  if (worker_id < 0) {
    LOG_ERROR(options_.info_log, "Invalid worker encoded in the handle");
    return true;
  }
  auto* worker_queue = subscriber_queues_[worker_id]->GetThreadLocal();
  auto sub_id = SubscriptionID::Unsafe(sub_handle);

  // Send command to responsible worker.
  const auto seqno = message.GetSequenceNumber();
  std::unique_ptr<Command> command(
      MakeExecuteCommand([this, sub_id, worker_id, seqno]() mutable {
        subscribers_[worker_id]->Acknowledge(sub_id, seqno);
      }));

  if (flow) {
    flow->Write(worker_queue, command);
  } else {
    if (!worker_queue->TryWrite(command)) {
      return false;
    }
  }
  return true;
}

void MultiThreadedSubscriber::SaveSubscriptions(
    SaveSubscriptionsCallback save_callback) {
  if (!options_.storage) {
    save_callback(Status::NotInitialized());
    return;
  }

  std::shared_ptr<SubscriptionStorage::Snapshot> snapshot;
  Status st =
      options_.storage->CreateSnapshot(msg_loop_->GetNumWorkers(), &snapshot);
  if (!st.ok()) {
    LOG_WARN(options_.info_log,
             "Failed to create snapshot to save subscriptions: %s",
             st.ToString().c_str());
    save_callback(std::move(st));
    return;
  }

  // For each worker we attempt to append entries for all subscriptions.
  auto map = [this, snapshot](int worker_id) {
    const auto& subscriber = subscribers_[worker_id];
    return subscriber->SaveState(snapshot.get(), worker_id);
  };

  // Once all workers are done, we commit the snapshot and call the callback if
  // necessary.
  auto reduce = [save_callback, snapshot](std::vector<Status> statuses) {
    for (auto& status : statuses) {
      if (!status.ok()) {
        save_callback(std::move(status));
        return;
      }
    }
    Status status = snapshot->Commit();
    save_callback(std::move(status));
  };

  // Fan out commands to all workers.
  st = msg_loop_->Gather(std::move(map), std::move(reduce));
  if (!st.ok()) {
    LOG_WARN(options_.info_log,
             "Failed to send snapshot command to all workers: %s",
             st.ToString().c_str());
    save_callback(std::move(st));
    return;
  }
}

Statistics MultiThreadedSubscriber::GetStatisticsSync() {
  return msg_loop_->AggregateStatsSync(
      [this](int i) -> Statistics { return statistics_[i]->all; });
}

SubscriptionHandle MultiThreadedSubscriber::CreateNewHandle(size_t worker_id) {
  const auto num_workers = msg_loop_->GetNumWorkers();
  const auto handle = 1 + worker_id + num_workers * next_sub_id_++;
  if (static_cast<size_t>(GetWorkerID(handle)) != worker_id) {
    return SubscriptionHandle(0);
  }
  return handle;
}

ssize_t MultiThreadedSubscriber::GetWorkerID(
    SubscriptionHandle sub_handle) const {
  const auto num_workers = msg_loop_->GetNumWorkers();
  const auto worker_id = static_cast<int>((sub_handle - 1) % num_workers);
  if (worker_id < 0 || worker_id >= num_workers) {
    return -1;
  }
  return worker_id;
}

}  // namespace rocketspeed
