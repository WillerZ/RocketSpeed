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
#include "src/messages/unbounded_mpsc_queue.h"
#include "src/port/port.h"
#include "src/util/common/processor.h"
#include "src/util/common/random.h"
#include "src/util/common/rate_limiter_sink.h"
#include "src/util/common/statistics.h"
#include "src/util/timeout_list.h"
#include "external/xxhash/xxhash.h"

namespace rocketspeed {

///////////////////////////////////////////////////////////////////////////////
MultiThreadedSubscriber::MultiThreadedSubscriber(
    const ClientOptions& options, std::shared_ptr<MsgLoop> msg_loop)
: options_(options)
, msg_loop_(std::move(msg_loop))
, allocator_(msg_loop_->GetNumWorkers(), options_.allocator_size) {
  size_t max_subscriptions_per_thread =
      options_.max_subscriptions / options_.num_workers;
  size_t remaining_subscriptions =
      options_.max_subscriptions % options_.num_workers;

  for (int i = 0; i < msg_loop_->GetNumWorkers(); ++i) {
    EventLoop* event_loop = msg_loop_->GetEventLoop(i);
    statistics_.emplace_back(std::make_shared<SubscriberStats>("subscriber."));
    subscribers_.emplace_back(new MultiShardSubscriber(
        options_,
        event_loop,
        statistics_.back(),
        max_subscriptions_per_thread +
            ((static_cast<size_t>(i) < remaining_subscriptions) ? 1 : 0)));
    subscriber_queues_.emplace_back(
        new UnboundedMPSCQueue<std::unique_ptr<ExecuteCommand>>(
            options_.info_log,
            event_loop->GetQueueStats(),
            options_.queue_size,
            "metadata_queue-" + std::to_string(i)));
    InstallSource<std::unique_ptr<ExecuteCommand>>(
        event_loop,
        subscriber_queues_.back().get(),
        [this](Flow* flow, std::unique_ptr<ExecuteCommand> command) {
          command->Execute(flow);
        });
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
              void InstallHooks(const HooksParameters&,
                                std::shared_ptr<SubscriberHooks>) override{};

              void UnInstallHooks(const HooksParameters&) override {}

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

              void RefreshRouting() override {}
              void NotifyHealthy(bool isHealthy) override {}
              bool CallInSubscriptionThread(
                  SubscriptionParameters, std::function<void()> job) override {
                return false;
              }
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

void MultiThreadedSubscriber::InstallHooks(
    const HooksParameters& params, std::shared_ptr<SubscriberHooks> hooks) {
  const auto worker_id = options_.thread_selector(
      msg_loop_->GetNumWorkers(), params.namespace_id, params.topic_name);
  RS_ASSERT(static_cast<size_t>(worker_id) < subscriber_queues_.size());
  auto* worker_queue = subscriber_queues_[worker_id].get();

  std::unique_ptr<ExecuteCommand> command(
      MakeExecuteCommand([this, worker_id, params, hooks]() mutable {
        subscribers_[worker_id]->InstallHooks(params, hooks);
      }));
  worker_queue->Write(command);
}

void MultiThreadedSubscriber::UnInstallHooks(const HooksParameters& params) {
  const auto worker_id = options_.thread_selector(
      msg_loop_->GetNumWorkers(), params.namespace_id, params.topic_name);
  RS_ASSERT(static_cast<size_t>(worker_id) < subscriber_queues_.size());
  auto* worker_queue = subscriber_queues_[worker_id].get();

  std::unique_ptr<ExecuteCommand> command(
      MakeExecuteCommand([this, worker_id, params]() mutable {
        subscribers_[worker_id]->UnInstallHooks(params);
      }));
  worker_queue->Write(command);
}

class SubscribeCommand : public ExecuteCommand {
 public:
  SubscribeCommand(MultiThreadedSubscriber* subscriber,
                   int worker_id,
                   SubscriptionID sub_id,
                   SubscriptionParameters&& params,
                   std::unique_ptr<Observer>&& observer)
  : subscriber_(subscriber)
  , worker_id_(worker_id)
  , sub_id_(sub_id)
  , params_(std::move(params))
  , observer_(std::move(observer)) {}

  void Execute(Flow*) override {
    subscriber_->subscribers_[worker_id_]->StartSubscription(
      sub_id_, std::move(params_), std::move(observer_));
  }

  ~SubscribeCommand() = default;

  MultiThreadedSubscriber* subscriber_;
  int worker_id_;
  SubscriptionID sub_id_;
  SubscriptionParameters params_;
  std::unique_ptr<Observer> observer_;
};

SubscriptionHandle MultiThreadedSubscriber::Subscribe(
    SubscriptionParameters parameters,
    std::unique_ptr<Observer>& observer) {
  // Find a shard this subscripttion belongs to.
  const auto shard_id = options_.sharding->GetShard(parameters.namespace_id,
                                                    parameters.topic_name);
  // Choose worker for this subscription and find appropriate queue.
  const auto worker_id = options_.thread_selector(msg_loop_->GetNumWorkers(),
                                                  parameters.namespace_id,
                                                  parameters.topic_name);
  RS_ASSERT(static_cast<size_t>(worker_id) < subscriber_queues_.size());
  auto* worker_queue = subscriber_queues_[worker_id].get();

  // Create new subscription handle that encodes destination worker.
  const auto sub_id = CreateNewHandle(shard_id, worker_id);
  if (!sub_id) {
    LOG_ERROR(options_.info_log, "Client run out of subscription handles");
    RS_ASSERT(false);
    return SubscriptionHandle(0);
  }

  // Create command to subscribe, to be sent to sharded worker.
  RS_ASSERT(worker_id <= std::numeric_limits<int>::max());
  std::unique_ptr<SubscribeCommand> sub_command(
      new SubscribeCommand(this,
                           static_cast<int>(worker_id),
                           sub_id,
                           std::move(parameters),
                           std::move(observer)));

  // Type-erase, as TryWrite needs l-value.
  std::unique_ptr<ExecuteCommand> command(std::move(sub_command));

  // Send command to responsible worker.
  if (!worker_queue->TryWrite(command)) {
    // Make sure we still have the command and observer, and give the
    // observer back to the caller, so they can retry later.
    RS_ASSERT(!!command);
    sub_command.reset(static_cast<SubscribeCommand*>(command.release()));
    RS_ASSERT(!!sub_command->observer_);
    observer = std::move(sub_command->observer_);
    return SubscriptionHandle(0);
  }
  return sub_id;
}

void MultiThreadedSubscriber::Unsubscribe(SubscriptionHandle sub_handle) {
  const auto sub_id = SubscriptionID::Unsafe(sub_handle);
  if (!sub_id) {
    LOG_ERROR(options_.info_log, "Invalid SubscriptionID");
    return;
  }

  // Determine corresponding worker, its queue, and subscription ID.
  const auto worker_id = GetWorkerID(sub_id);
  if (worker_id < 0) {
    LOG_ERROR(options_.info_log, "Invalid worker encoded in the handle");
    return;
  }
  RS_ASSERT(static_cast<size_t>(worker_id) < subscriber_queues_.size());
  auto* worker_queue = subscriber_queues_[worker_id].get();

  // Send command to responsible worker.
  std::unique_ptr<ExecuteCommand> command(
      MakeExecuteCommand([this, sub_id, worker_id]() mutable {
        subscribers_[worker_id]->TerminateSubscription(sub_id);
      }));

  // Unsubscribe uses Write instead of TryWrite so that the write always
  // succeeds. Flow control is not needed as number of unsubscribes in flight
  // is bounded by total number of subscriptions + the number of subscribes
  // in flight.
  worker_queue->Write(command);
}

bool MultiThreadedSubscriber::Acknowledge(const MessageReceived& message) {
  const auto sub_id = SubscriptionID::Unsafe(message.GetSubscriptionHandle());
  if (!sub_id) {
    LOG_ERROR(options_.info_log, "Invalid SubscriptionID");
    return true;
  }

  // Determine corresponding worker, its queue, and subscription ID.
  const auto worker_id = GetWorkerID(sub_id);
  if (worker_id < 0) {
    LOG_ERROR(options_.info_log, "Invalid worker encoded in the handle");
    return true;
  }

  RS_ASSERT(static_cast<size_t>(worker_id) < subscriber_queues_.size());
  auto* worker_queue = subscriber_queues_[worker_id].get();

  // Send command to responsible worker.
  const auto seqno = message.GetSequenceNumber();
  std::unique_ptr<ExecuteCommand> command(
      MakeExecuteCommand([this, sub_id, worker_id, seqno]() mutable {
        subscribers_[worker_id]->Acknowledge(sub_id, seqno);
      }));

  if (!worker_queue->TryWrite(command)) {
    return false;
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

bool MultiThreadedSubscriber::CallInSubscriptionThread(
    SubscriptionParameters params, std::function<void()> job) {
  const auto worker_id = options_.thread_selector(
      msg_loop_->GetNumWorkers(), params.namespace_id, params.topic_name);
  RS_ASSERT(static_cast<size_t>(worker_id) < subscriber_queues_.size());
  auto* worker_queue = subscriber_queues_[worker_id].get();

  std::unique_ptr<ExecuteCommand> command(
      MakeExecuteCommand([ this, worker_id, params, f = std::move(job) ]() {
        subscribers_[worker_id]->CallInSubscriptionThread(params, std::move(f));
      }));

  if (!worker_queue->TryWrite(command)) {
    return false;
  }
  return true;
}

Statistics MultiThreadedSubscriber::GetStatisticsSync() {
  return msg_loop_->AggregateStatsSync(
      [this](int i) -> Statistics { return statistics_[i]->all; });
}

SubscriptionID MultiThreadedSubscriber::CreateNewHandle(size_t shard_id,
                                                        size_t worker_id) {
  RS_ASSERT(shard_id <= std::numeric_limits<ShardID>::max());
  return allocator_.Next(static_cast<ShardID>(shard_id), worker_id);
}

ssize_t MultiThreadedSubscriber::GetWorkerID(SubscriptionID sub_id) const {
  const size_t num_workers = msg_loop_->GetNumWorkers();
  const size_t worker_id = allocator_.GetWorkerID(sub_id);
  if (worker_id >= num_workers) {
    return -1;
  }
  return static_cast<ssize_t>(worker_id);
}

}  // namespace rocketspeed
