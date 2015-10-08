// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "worker.h"

#include <vector>

#include "include/Status.h"
#include "include/Types.h"

#include "src/copilot/control_tower_router.h"
#include "src/copilot/copilot.h"
#include "src/rollcall/rollcall_impl.h"

#include "external/folly/move_wrapper.h"

namespace rocketspeed {

CopilotWorker::CopilotWorker(
    const CopilotOptions& options,
    std::shared_ptr<ControlTowerRouter> control_tower_router,
    const int myid,
    Copilot* copilot,
    std::shared_ptr<ClientImpl> client)
: options_(options)
, control_tower_router_(std::move(control_tower_router))
, copilot_(copilot)
, myid_(myid) {
  // copilot is required.
  assert(copilot_);

  // Cached calculation.
  resubscriptions_per_tick_ = std::max<uint64_t>(1,
    options_.resubscriptions_per_second *
    options_.timer_interval_micros /
    1000000 /
    options_.msg_loop->GetNumWorkers());

  rebalances_per_tick_ = std::max<uint64_t>(1,
    options_.rebalances_per_second *
    options_.timer_interval_micros /
    1000000 /
    options_.msg_loop->GetNumWorkers());

  LOG_VITAL(options_.info_log, "Created a new CopilotWorker");
  options_.info_log->Flush();

  client_queues_ = options_.msg_loop->CreateWorkerQueues();
  tower_queues_ = options_.msg_loop->CreateWorkerQueues();

  // Create Rollcall topic writer
  if (options_.rollcall_enabled) {
    assert(client);
    rollcall_.reset(new RollcallImpl(std::move(client),
                                     InvalidTenant,
                                     "copilot.rollcall"));
    rollcall_error_queues_ = options_.msg_loop->CreateThreadLocalQueues(myid_);
  }
}

CopilotWorker::~CopilotWorker() {
}

std::unique_ptr<Command>
CopilotWorker::WorkerCommand(LogID logid,
                             std::unique_ptr<Message> msg,
                             int worker_id,
                             StreamID origin) {
  auto moved_msg = folly::makeMoveWrapper(std::move(msg));
  std::unique_ptr<Command> command(
    MakeExecuteCommand([this, moved_msg, logid, worker_id, origin]() mutable {
      auto message = moved_msg.move();
      switch (message->GetMessageType()) {
        case MessageType::mDeliverData: {
          ProcessData(std::move(message), origin);
        } break;

        case MessageType::mDeliverGap: {
          ProcessGap(std::move(message), origin);
        } break;

        case MessageType::mTailSeqno: {
          ProcessTailSeqno(std::move(message), origin);
        } break;

        case MessageType::mSubscribe: {
          auto subscribe = static_cast<MessageSubscribe*>(message.get());
          ProcessSubscribe(subscribe->GetTenantID(),
                           subscribe->GetNamespace(),
                           subscribe->GetTopicName(),
                           subscribe->GetStartSequenceNumber(),
                           subscribe->GetSubID(),
                           logid,
                           worker_id,
                           origin);
        } break;

        case MessageType::mUnsubscribe: {
          auto unsubscribe = static_cast<MessageUnsubscribe*>(message.get());
          ProcessUnsubscribe(unsubscribe->GetTenantID(),
                             unsubscribe->GetSubID(),
                             unsubscribe->GetReason(),
                             worker_id,
                             origin);
        } break;

        case MessageType::mGoodbye: {
          ProcessGoodbye(std::move(message), origin);
        } break;

        default: {
          LOG_WARN(options_.info_log,
                   "Unexpected message type in copilot worker %d",
                   static_cast<int>(message->GetMessageType()));
        }
      }
    }));
  return command;
}

std::unique_ptr<Command> CopilotWorker::WorkerCommand(
    std::shared_ptr<ControlTowerRouter> new_router) {
  std::unique_ptr<Command> command(MakeExecuteCommand(
      std::bind(&CopilotWorker::ProcessRouterUpdate, this, new_router)));
  return command;
}

Statistics CopilotWorker::GetStatistics() {
  stats_.subscribed_topics->Set(topics_.size());

  size_t total_sockets = 0;
  for (const auto& entry : control_tower_sockets_) {
    total_sockets += entry.second.size();
  }
  stats_.control_tower_sockets->Set(total_sockets);
  stats_.orphaned_topics->Set(active_resubscribe_requests_by_topic_.size());

  Statistics stats = stats_.all;
  if (options_.rollcall_enabled) {
    stats.Aggregate(rollcall_->GetStatistics());
  }
  return stats;
}

void CopilotWorker::ProcessData(std::unique_ptr<Message> message,
                                StreamID origin) {
  MessageDeliverData* msg = static_cast<MessageDeliverData*>(message.get());

  auto ptr = sub_to_topic_.Find(origin, msg->GetSubID());
  if (!ptr) {
    // This is somewhat expected due to the lag of propagating unsubscription
    // to the control tower.
    LOG_DEBUG(options_.info_log,
      "Deliver for unknown subscription StreamID(%llu) SubID(%" PRIu64 ")",
      origin, msg->GetSubID());
    return;
  }
  const TopicUUID uuid = *ptr;

  // Get the list of subscriptions for this topic.
  LOG_DEBUG(options_.info_log,
            "Copilot received deliver (%.16s)@%" PRIu64 " for %s",
            msg->GetPayload().ToString().c_str(),
            msg->GetSequenceNumber(),
            uuid.ToString().c_str());

  auto it = topics_.find(uuid);
  if (it != topics_.end()) {
    TopicState& topic = it->second;
    const auto seqno = msg->GetSequenceNumber();
    const auto prev_seqno = msg->GetPrevSequenceNumber();

    // Find tower for this origin and update its state.
    AdvanceTowers(&topic, prev_seqno, seqno, origin, msg->GetSubID());

    // Send to all subscribers.
    bool delivered_at_least_once = false;
    for (auto& sub : topic.subscriptions) {
      StreamID recipient = sub->stream_id;

      // Do not send a response if the seqno is too low.
      if (sub->seqno > seqno) {
        LOG_DEBUG(options_.info_log,
                  "Data not delivered to %llu ID(%" PRIu64 ")"
                  " (seqno@%" PRIu64 " too low, currently @%" PRIu64 ")",
                  recipient,
                  sub->sub_id,
                  seqno,
                  sub->seqno);
        continue;
      }

      // or too high.
      if (sub->seqno < prev_seqno) {
        LOG_DEBUG(options_.info_log,
                  "Data not delivered to %llu ID(%" PRIu64 ")"
                  " (prev_seqno@%" PRIu64 " too high, currently @%" PRIu64 ")",
                  recipient,
                  sub->sub_id,
                  prev_seqno,
                  sub->seqno);
        continue;
      }

      // or not matching zeroes.
      if ((sub->seqno == 0 && prev_seqno != 0) ||
          (sub->seqno != 0 && prev_seqno == 0)) {
        LOG_DEBUG(options_.info_log,
                  "Data not delivered to %llu ID(%" PRIu64 ")"
                  " (prev_seqno@%" PRIu64 " not 0)",
                  recipient,
                  sub->sub_id,
                  prev_seqno);
        continue;
      }

      // Mark even if fail to send.
      // The point is that it wasn't out of order.
      delivered_at_least_once = true;

      // Send message to the client.
      MessageDeliverData data(sub->tenant_id,
                              sub->sub_id,
                              msg->GetMessageID(),
                              msg->GetPayload());
      data.SetSequenceNumbers(prev_seqno, seqno);
      auto command = options_.msg_loop->ResponseCommand(data, recipient);
      if (client_queues_[sub->worker_id]->Write(command)) {
        sub->seqno = seqno + 1;
        ++topic.records_sent;

        LOG_DEBUG(options_.info_log,
                  "Sent data (%.16s)@%" PRIu64 " for ID(%" PRIu64
                  ") %s to %llu",
                  msg->GetPayload().ToString().c_str(),
                  msg->GetSequenceNumber(),
                  data.GetSubID(),
                  uuid.ToString().c_str(),
                  recipient);
      } else {
        LOG_WARN(options_.info_log,
                 "Failed to distribute message to %llu",
                 recipient);
      }
    }
    if (!delivered_at_least_once) {
      stats_.data_dropped_out_of_order->Add(1);
    }
  } else {
    stats_.data_on_unsubscribed_topic->Add(1);
  }
}

void CopilotWorker::ProcessGap(std::unique_ptr<Message> message,
                               StreamID origin) {
  MessageDeliverGap* msg = static_cast<MessageDeliverGap*>(message.get());

  auto ptr = sub_to_topic_.Find(origin, msg->GetSubID());
  if (!ptr) {
    // This is somewhat expected due to the lag of propagating unsubscription
    // to the control tower.
    LOG_DEBUG(options_.info_log,
      "Gap for unknown subscription StreamID(%llu) SubID(%" PRIu64 ")",
      origin, msg->GetSubID());
    return;
  }
  const TopicUUID uuid = *ptr;

  // Get the list of subscriptions for this topic.
  LOG_DEBUG(options_.info_log,
            "Copilot received gap %" PRIu64 "-%" PRIu64 " for %s",
            msg->GetFirstSequenceNumber(),
            msg->GetLastSequenceNumber(),
            uuid.ToString().c_str());
  auto it = topics_.find(uuid);
  if (it != topics_.end()) {
    TopicState& topic = it->second;
    const auto prev_seqno = msg->GetFirstSequenceNumber();
    const auto next_seqno = msg->GetLastSequenceNumber();

    // Find tower for this origin and update its state.
    AdvanceTowers(&topic, prev_seqno, next_seqno, origin, msg->GetSubID());

    // Send to all subscribers.
    bool delivered_at_least_once = false;
    for (auto& sub : topic.subscriptions) {
      StreamID recipient = sub->stream_id;

      // Ignore if the seqno is too low.
      if (sub->seqno > next_seqno) {
        LOG_DEBUG(options_.info_log,
                  "Gap ignored for %llu"
                  " (next_seqno@%" PRIu64 " too low, currently @%" PRIu64 ")",
                  recipient,
                  next_seqno,
                  sub->seqno);
        continue;
      }

      // or too high.
      if (sub->seqno < prev_seqno) {
        LOG_DEBUG(options_.info_log,
                  "Gap ignored for %llu"
                  " (prev_seqno@%" PRIu64 " too high, currently @%" PRIu64 ")",
                  recipient,
                  prev_seqno,
                  sub->seqno);
        continue;
      }

      // or not matching zeroes.
      if ((sub->seqno == 0 && prev_seqno != 0) ||
          (sub->seqno != 0 && prev_seqno == 0)) {
        LOG_DEBUG(options_.info_log,
                  "Gap ignored for %llu"
                  " (prev_seqno@%" PRIu64 " not 0)",
                  recipient,
                  prev_seqno);
        continue;
      }

      // Mark even if fail to send.
      // The point is that it wasn't out of order.
      delivered_at_least_once = true;

      // Send message to the client.
      MessageDeliverGap gap(
        sub->tenant_id,
        sub->sub_id,
        msg->GetGapType());
      gap.SetSequenceNumbers(prev_seqno, next_seqno);
      auto command = options_.msg_loop->ResponseCommand(gap, recipient);
      if (client_queues_[sub->worker_id]->Write(command)) {
        sub->seqno = next_seqno + 1;
        ++topic.gaps_sent;

        LOG_DEBUG(options_.info_log,
                 "Sent gap %" PRIu64 "-%" PRIu64
                 " for subscription ID(%" PRIu64 ") %s to %llu",
                 msg->GetFirstSequenceNumber(),
                 msg->GetLastSequenceNumber(),
                 gap.GetSubID(),
                 uuid.ToString().c_str(),
                 recipient);
      } else {
        LOG_WARN(options_.info_log,
                 "Failed to distribute gap to %llu",
                 recipient);
      }
    }

    if (!delivered_at_least_once) {
      stats_.gap_dropped_out_of_order->Add(1);
    }

    if (prev_seqno == 0) {
      // When prev_seqno == 0, this was a gap to inform us what the current
      // sequence number is. It could be the case that it's actually lower than
      // all other subscriptions (e.g. because we have "future" subscriptions).
      // In this case, we need to actually rewind to this older subscription,
      // so we have to (potentially) update subscriptions here.
      UpdateTowerSubscriptions(uuid, topic);

      // TODO(pja): if we have a higher subscription, and that subscription has
      // received messages then we can use that as a more accurate tail
      // position, and avoid a resubscribe.
    }
  } else {
    stats_.gap_on_unsubscribed_topic->Add(1);
  }
}

void CopilotWorker::ProcessTailSeqno(std::unique_ptr<Message> message,
                                     StreamID origin) {
  MessageTailSeqno* msg = static_cast<MessageTailSeqno*>(message.get());
  // Get the list of subscriptions for this topic.
  TopicUUID uuid(msg->GetNamespace(), msg->GetTopicName());
  LOG_DEBUG(options_.info_log,
            "Copilot received tail senqo %" PRIu64 " for %s",
            msg->GetSequenceNumber(),
            uuid.ToString().c_str());
  auto it = topics_.find(uuid);
  if (it != topics_.end()) {
    TopicState& topic = it->second;
    const auto next_seqno = msg->GetSequenceNumber();

    // Advance all towers subscribed at 0.
    for (auto& tower : topic.towers) {
      if (tower.stream->GetStreamID() == origin) {
        if (tower.next_seqno == 0) {
          LOG_DEBUG(options_.info_log,
                    "Tower subscription %llu advanced from 0 to %" PRIu64,
                    tower.stream->GetStreamID(),
                    next_seqno);
          tower.next_seqno = next_seqno;
        }
      }
    }

    // Send to all subscribers subscribed at 0.
    for (auto& sub : topic.subscriptions) {
      StreamID recipient = sub->stream_id;

      if (sub->seqno != 0) {
        continue;
      }

      // Send gap to the client.
      MessageDeliverGap gap(sub->tenant_id,
                            sub->sub_id,
                            GapType::kBenign);
      gap.SetSequenceNumbers(0, next_seqno - 1);
      auto command = options_.msg_loop->ResponseCommand(gap, recipient);
      if (client_queues_[sub->worker_id]->Write(command)) {
        sub->seqno = next_seqno;
        ++topic.gaps_sent;

        LOG_DEBUG(options_.info_log,
                 "Sent tail senqo %" PRIu64
                 " for subscription ID(%" PRIu64 ") %s to %llu",
                 next_seqno,
                 gap.GetSubID(),
                 uuid.ToString().c_str(),
                 recipient);
      } else {
        LOG_WARN(options_.info_log,
                 "Failed to distribute tail seqno to %llu",
                 recipient);
      }
    }
    // Now that we know tail seqno, we may need to actually subscribe to it
    // (if any existing subscription is ahead of that point).
    UpdateTowerSubscriptions(uuid, topic);
  } else {
    stats_.gap_on_unsubscribed_topic->Add(1);
  }
}

void CopilotWorker::ProcessSubscribe(const TenantID tenant_id,
                                     const NamespaceID& namespace_id,
                                     const Topic& topic_name,
                                     const SequenceNumber start_seqno,
                                     const SubscriptionID sub_id,
                                     const LogID logid,
                                     const int worker_id,
                                     const StreamID subscriber) {
  TopicUUID uuid(namespace_id, topic_name);
  LOG_INFO(options_.info_log,
      "Received subscribe request ID(%" PRIu64
      ") for %s@%" PRIu64 " for %llu",
      sub_id,
      uuid.ToString().c_str(),
      start_seqno,
      subscriber);

  // Insert into client-topic map.
  client_subscriptions_[subscriber]
      .emplace(sub_id, TopicInfo{topic_name, namespace_id, logid});

  // Find/insert topic state.
  auto topic_iter = topics_.find(uuid);
  if (topic_iter == topics_.end()) {
    topic_iter = topics_.emplace(uuid, TopicState(logid)).first;
  }
  TopicState& topic = topic_iter->second;

  // First check if we already have a subscription for this subscriber.
  bool found = false;
  for (auto& sub : topic.subscriptions) {
    if (sub->stream_id == subscriber && sub->sub_id == sub_id) {
      // Existing subscription: update sequence number.
      sub->seqno = start_seqno;
      assert(sub->worker_id == worker_id);
      found = true;
      break;
    }
  }

  if (!found) {
    // No existing subscription, so insert new one.
    topic.subscriptions.emplace_back(
      new Subscription(subscriber,
                       start_seqno,
                       worker_id,
                       tenant_id,
                       sub_id));
    stats_.incoming_subscriptions->Add(1);
  }

  // Update the copilot's subscriptions on the control tower(s) to reflect this
  // new topic subscription.
  UpdateTowerSubscriptions(uuid, topic);

  // Update rollcall topic.
  RollcallWrite(sub_id,
                tenant_id,
                uuid,
                MetadataType::mSubscribe,
                logid,
                worker_id,
                subscriber);
}

void CopilotWorker::ProcessUnsubscribe(TenantID tenant_id,
                                       SubscriptionID sub_id,
                                       MessageUnsubscribe::Reason reason,
                                       int worker_id,
                                       StreamID subscriber) {
  LOG_INFO(options_.info_log,
           "Received unsubscribe request for ID (%" PRIu64 ") on stream %llu",
           sub_id,
           subscriber);

  switch (reason) {
    case MessageUnsubscribe::Reason::kRequested:
      // Inbound unsubscription from client.
      RemoveSubscription(tenant_id, sub_id, subscriber, worker_id);
      break;

    case MessageUnsubscribe::Reason::kBackOff:
      // Not used.
      break;

    case MessageUnsubscribe::Reason::kInvalid:
      // Control Tower has rejected our subscription.
      HandleInvalidSubscription(subscriber, sub_id);
      break;
  }
}

void CopilotWorker::RemoveSubscription(const TenantID tenant_id,
                                       const SubscriptionID sub_id,
                                       const StreamID subscriber,
                                       const int worker_id) {
  NamespaceID namespace_id;
  Topic topic_name;
  LogID logid;
  {  // Remove from client-topic map.
    auto& client_subscriptions = client_subscriptions_[subscriber];
    auto it = client_subscriptions.find(sub_id);
    if (it == client_subscriptions.end()) {
      // We broadcast unsubscribes to all workers, this is perfectly normal
      // situation.
      return;
    }
    auto& topic_info = it->second;
    namespace_id = std::move(topic_info.namespace_id);
    topic_name = std::move(topic_info.topic_name);
    logid = topic_info.logid;
    client_subscriptions.erase(it);
  }

  TopicUUID uuid(namespace_id, topic_name);
  auto topic_iter = topics_.find(uuid);
  if (topic_iter != topics_.end()) {
    // Find our subscription and remove it.
    TopicState& topic = topic_iter->second;
    auto& subscriptions = topic.subscriptions;
    for (auto it = subscriptions.begin(); it != subscriptions.end(); ) {
      Subscription* sub = it->get();
      if (sub->stream_id == subscriber && sub->sub_id == sub_id) {
        // This is our subscription, remove it.
        it = subscriptions.erase(it);
        stats_.incoming_subscriptions->Add(-1);
      } else {
        ++it;
      }
    }

    // Unsubscribe from control towers if necessary.
    if (topic.subscriptions.empty()) {
      UnsubscribeControlTowers(uuid, topic);
      topic.towers.clear();
    }

    // Update rollcall topic.
    RollcallWrite(sub_id, tenant_id, uuid,
                  MetadataType::mUnSubscribe,
                  logid, worker_id, subscriber);

    // No more subscriptions, so remove from map.
    if (topic.subscriptions.empty()) {
      topics_.erase(topic_iter);
      CancelResubscribeRequest(uuid);
      topic_checkup_list_.Erase(uuid);
    }
  }
}

void CopilotWorker::HandleInvalidSubscription(StreamID origin,
                                              SubscriptionID sub_id) {
  // HandleInvalidSubscription is invoked when a control tower has responded
  // to a subscribe message with an unsubscribe message (with reason kInvalid).
  // This means that the topic is invalid, for whatever reason.

  // Find the topic that this subscription was for.
  auto ptr = sub_to_topic_.Find(origin, sub_id);
  if (!ptr) {
    LOG_WARN(options_.info_log,
             "Unknown subscription StreamID(%llu) SubID(%" PRIu64 ")",
             origin,
             sub_id);
    return;
  }
  const TopicUUID uuid = *ptr;

  auto it = topics_.find(uuid);
  if (it != topics_.end()) {
    TopicState& topic = it->second;

    // Forward the unsubscribe to all clients.
    for (auto& sub : topic.subscriptions) {
      RollcallWrite(sub->sub_id, sub->tenant_id, uuid,
                    MetadataType::mUnSubscribe,
                    topic.log_id, sub->worker_id, sub->stream_id);

      MessageUnsubscribe message(sub->tenant_id,
                                 sub->sub_id,
                                 MessageUnsubscribe::Reason::kInvalid);
      auto cmd = options_.msg_loop->ResponseCommand(message, sub->stream_id);
      if (client_queues_[sub->worker_id]->Write(cmd)) {
        LOG_DEBUG(options_.info_log,
          "Sent unsubscribe (invalid) for StreamID(%llu) SubID(%" PRIu64 ")",
          sub->stream_id, sub->sub_id);
      } else {
        LOG_WARN(
            options_.info_log,
            "Failed unsubscribe (invalid) for StreamID(%llu) SubID(%" PRIu64
            ")",
            sub->stream_id,
            sub->sub_id);
      }
    }
    stats_.incoming_subscriptions->Add(-int64_t(topic.subscriptions.size()));

    // Unsubscribe all other control towers too -- we no longer need them.
    UnsubscribeControlTowers(uuid, topic);
    topics_.erase(it);
    CancelResubscribeRequest(uuid);
    topic_checkup_list_.Erase(uuid);
  }
}

void CopilotWorker::UnsubscribeControlTowers(
    const TopicUUID& topic_uuid, TopicState& topic) {

  const TenantID tenant_id = GuestTenant;

  // No more subscriptions on this topic, so unsubscribe from control towers.
  for (auto& tower : topic.towers) {
    SendUnsubscribe(tenant_id,
                    tower.stream,
                    tower.sub_id,
                    tower.worker_id);
  }
}

void CopilotWorker::ProcessGoodbye(std::unique_ptr<Message> message,
                                   StreamID origin) {
  MessageGoodbye* goodbye = static_cast<MessageGoodbye*>(message.get());

  switch (goodbye->GetOriginType()) {
    case MessageGoodbye::OriginType::Client: {
      // This is a goodbye from one of the clients.
      LOG_INFO(options_.info_log,
           "Copilot received goodbye for client %llu",
           origin);

      auto it = client_subscriptions_.find(origin);
      if (it != client_subscriptions_.end()) {
        // Unsubscribe from all topics.
        // Making a copy because RemoveSubscription will modify
        // client_subscriptions_;
        auto topics_copy = it->second;
        for (const auto& entry : topics_copy) {
          RemoveSubscription(goodbye->GetTenantID(),
                             entry.first,
                             origin,
                             0);  // The worked id is a dummy because we do not
                                // need to send any response back to the client
        }
        client_subscriptions_.erase(it);
      }
      break;
    }

    case MessageGoodbye::OriginType::Server: {
      LOG_WARN(options_.info_log,
               "Copilot received goodbye for server %llu",
               origin);
      CloseControlTowerStream(origin);
      break;
    }
  }
}

void CopilotWorker::ProcessRouterUpdate(
    std::shared_ptr<ControlTowerRouter> router) {
  LOG_VITAL(options_.info_log, "Updating control tower router");
  control_tower_router_ = std::move(router);
  control_tower_cache_.clear();
}

void CopilotWorker::ProcessTimerTick() {
  // On each tick, we loop through orphan topics to check if we can find
  // a control tower subscription for then. We limit the number sent per second
  // to avoid thundering herd on the control tower.
  uint64_t count = resubscriptions_per_tick_;
  while (count-- && HasActiveResubscribeRequests()) {
    SafeResubscribeRequest resubscribe_request = PopNextResubscribeRequest();
    auto topic_it = topics_.find(resubscribe_request->topic_uuid);
    bool topic_valid = topic_it != topics_.end();
    assert(topic_valid);
    if (topic_valid) {
      UpdateTowerSubscriptions(
        resubscribe_request->topic_uuid,
        topic_it->second, /* topic */
        resubscribe_request->sequence_number,
        resubscribe_request->have_zero_sub);
      stats_.orphaned_resubscribes->Add(1);
    }
  }

  if (options_.rollcall_enabled) {
    rollcall_->CheckBatchTimeouts(options_.rollcall_flush_latency);
  }

  // Get a list of topics/tower subscriptions that are due a check up.
  std::vector<TopicUUID> updates;
  topic_checkup_list_.GetExpired(
    options_.tower_subscriptions_check_period,
    std::back_inserter(updates),
    static_cast<int>(rebalances_per_tick_));

  for (TopicUUID& uuid : updates) {
    auto it = topics_.find(uuid);
    if (it != topics_.end()) {
      if (!CorrectTopicTowers(it->second)) {
        // Remove subscriptions and resubscribe to correct towers.
        const bool force_resub = true;
        UpdateTowerSubscriptions(uuid, it->second, force_resub);
        stats_.tower_rebalances_performed->Add(1);
      }
      // Put back in the list to check again later.
      topic_checkup_list_.Add(std::move(uuid));
    }
  }
  stats_.tower_rebalances_checked->Add(updates.size());
}

void CopilotWorker::CloseControlTowerStream(StreamID stream) {
  sub_to_topic_.Remove(stream);

  // Removes upstream connection for affected subscriptions.
  for (auto& uuid_topic : topics_) {
    const TopicUUID& uuid = uuid_topic.first;
    TopicState& topic = uuid_topic.second;
    for (auto it = topic.towers.begin(); it != topic.towers.end(); ) {
      if (it->stream->GetStreamID() == stream) {
        it = topic.towers.erase(it);
        ScheduleResubscribeRequest(uuid, topic);
      } else {
        ++it;
      }
    }
  }

  // Update control_tower_sockets_.
  // Removes all entries with this control tower stream.
  for (auto& socket : control_tower_sockets_) {
    std::unordered_map<int, StreamSocket>& streams = socket.second;
    for (auto it = streams.begin(); it != streams.end(); ) {
      if (it->second.GetStreamID() == stream) {
        it = streams.erase(it);
      } else {
        ++it;
      }
    }
  }
}

bool CopilotWorker::SendSubscribe(TenantID tenant_id,
                                  TopicUUID uuid,
                                  SequenceNumber seqno,
                                  StreamSocket* stream,
                                  SubscriptionID sub_id,
                                  int worker_id) {
  Slice namespace_id;
  Slice topic_name;
  uuid.GetTopicID(&namespace_id, &topic_name);

  MessageSubscribe message(tenant_id,
                           namespace_id.ToString(),
                           topic_name.ToString(),
                           seqno,
                           sub_id);

  auto command = options_.msg_loop->RequestCommand(message, stream);
  if (tower_queues_[worker_id]->Write(command)) {
    LOG_DEBUG(options_.info_log,
      "Sent %s@%" PRIu64 " subscription to tower stream %llu ID(%" PRIu64 ")",
      uuid.ToString().c_str(),
      seqno,
      stream->GetStreamID(),
      sub_id);
    sub_to_topic_.Insert(stream->GetStreamID(), sub_id, std::move(uuid));
    return true;
  } else {
    LOG_WARN(options_.info_log,
             "Failed to send %s subscribe to tower stream %llu",
             uuid.ToString().c_str(),
             stream->GetStreamID());
    return false;
  }

}

bool CopilotWorker::SendUnsubscribe(TenantID tenant_id,
                                    StreamSocket* stream,
                                    SubscriptionID sub_id,
                                    int worker_id) {
  MessageUnsubscribe message(tenant_id,
                             sub_id,
                             MessageUnsubscribe::Reason::kRequested);

  auto command = options_.msg_loop->RequestCommand(message, stream);
  if (tower_queues_[worker_id]->Write(command)) {
    LOG_DEBUG(options_.info_log,
      "Sent unsubscription to tower stream %llu",
      stream->GetStreamID());
    sub_to_topic_.Remove(stream->GetStreamID(), sub_id);
    return true;
  } else {
    LOG_WARN(options_.info_log,
             "Failed to send unsubscribe to tower stream %llu",
             stream->GetStreamID());
    return false;
  }
}

// Find earliest non-zero subscription, or zero if only zero subscriptions.
SequenceNumber CopilotWorker::FindLowestSequenceNumber(
    const TopicState& topic, bool* have_zero_sub) {

  SequenceNumber new_seqno = 0;
  for (auto& sub : topic.subscriptions) {
    if (sub->seqno != 0) {
      if (new_seqno == 0 || sub->seqno < new_seqno) {
        new_seqno = sub->seqno;
      }
    } else {
      *have_zero_sub = true;
    }
  }
  return new_seqno;
}


void CopilotWorker::UpdateTowerSubscriptions(const TopicUUID& uuid,
                                             TopicState& topic,
                                             bool force_resub) {
  bool have_zero_sub = false;
  SequenceNumber new_seqno = FindLowestSequenceNumber(topic, &have_zero_sub);

  UpdateTowerSubscriptions(
      uuid, topic, new_seqno, have_zero_sub, force_resub);
}

void CopilotWorker::UpdateTowerSubscriptions(
    const TopicUUID& uuid,
    TopicState& topic,
    SequenceNumber new_seqno,
    const bool have_zero_sub,
    bool force_resub) {

  LOG_INFO(options_.info_log,
    "Refreshing tower subscriptions for %s",
    uuid.ToString().c_str());

  const LogID log_id = topic.log_id;
  const TenantID tenant_id = GuestTenant;

  // Find control towers responsible for this topic's log.
  std::vector<HostId const*> recipients;
  if (!GetControlTowers(log_id, &recipients).ok()) {
    // This should only ever happen if all control towers are offline.
    LOG_WARN(options_.info_log,
             "Failed to find control towers for log ID %" PRIu64,
             static_cast<uint64_t>(log_id));
  }

  // Check if we need to resubscribe to control towers.
  // First check if we have enough tower subscriptions.
  size_t expected_towers_per_log = recipients.size();

  // With one control tower, we can always subscribe directly to 0 since there
  // is no possibility of multiple towers subscribing to different seqnos.
  if (expected_towers_per_log == 1) {
    if (have_zero_sub) {
      // Actually subscribe to 0 instead of sending request for latest seqno.
      new_seqno = 0;
    }
  }

  // Note: it could be the case that the only subscriptions we have are
  // subscriptions at 0, so new_seqno will be 0 in that case, and we will
  // subscribe the copilot at 0 without sending a FindTailSeqno request.
  const bool send_latest_request = have_zero_sub && new_seqno != 0;

  bool resub_needed = topic.towers.size() < expected_towers_per_log;
  if (resub_needed) {
    LOG_INFO(options_.info_log,
      "Not enough control tower subscriptions for %s (%zu/%zu), resubscribing",
      uuid.ToString().c_str(),
      topic.towers.size(),
      expected_towers_per_log);
  }

  // If we have enough, check that all the tower subscriptions are suitable.
  if (!resub_needed) {
    for (auto& tower : topic.towers) {
      if (tower.next_seqno > new_seqno ||
          (tower.next_seqno != 0 && new_seqno == 0) ||
          (tower.next_seqno == 0 && new_seqno != 0)) {
        LOG_INFO(options_.info_log,
          "Tower sub %llu unsuitable for %s (%" PRIu64 " v.s. %" PRIu64 ")",
          tower.stream->GetStreamID(),
          uuid.ToString().c_str(),
          tower.next_seqno,
          new_seqno);
        resub_needed = true;
        break;
      }
    }
  }

  resub_needed = resub_needed || force_resub;

  // If needed, find a list of control tower connections.
  using TowerConnection = std::pair<StreamSocket*, int>;  // socket + worker_id
  autovector<TowerConnection, kMaxTowerConnections> tower_conns;
  if (resub_needed || send_latest_request) {
    for (HostId const* recipient : recipients) {
      int outgoing_worker_id = copilot_->GetTowerWorker(log_id, *recipient);

      // Find or open a new stream socket to this control tower.
      auto socket = GetControlTowerSocket(
          *recipient, options_.msg_loop, outgoing_worker_id);

      tower_conns.emplace_back(socket, outgoing_worker_id);
    }
  }

  // Do we need new tower subscription for this subscriber?
  if (resub_needed) {
    // Unsubscribe all current subscriptions.
    for (TopicState::Tower& tower : topic.towers) {
      SendUnsubscribe(tenant_id,
                      tower.stream,
                      tower.sub_id,
                      tower.worker_id);
    }

    // Clear old subscriptions.
    topic.towers.clear();

    for (TowerConnection& tower_conn : tower_conns) {
      // Send request to control tower to update the copilot subscription.
      StreamSocket* const socket = tower_conn.first;
      const int outgoing_worker_id = tower_conn.second;

      SubscriptionID sub_id =
        GenerateSubscriptionID(&next_sub_id_state_,
                               myid_,
                               options_.msg_loop->GetNumWorkers());

      bool success = SendSubscribe(tenant_id,
                                   uuid,
                                   new_seqno,
                                   socket,
                                   sub_id,
                                   outgoing_worker_id);
      if (success) {
        // Update the towers for the subscription.
        assert(!topic.FindTower(socket));  // we just cleared all towers.
        topic.towers.emplace_back(socket,
                                  sub_id,
                                  new_seqno,
                                  outgoing_worker_id);
      }
    }
  }

  // For zero sequence numbers, we just request it from the control tower.
  if (send_latest_request) {
    Slice namespace_id;
    Slice topic_name;
    uuid.GetTopicID(&namespace_id, &topic_name);

    MessageFindTailSeqno msg(tenant_id,
                             namespace_id.ToString(),
                             topic_name.ToString());

    // Send to all control towers.
    for (TowerConnection& tower_conn : tower_conns) {
      StreamSocket* const stream = tower_conn.first;
      const int worker_id = tower_conn.second;
      auto command = options_.msg_loop->RequestCommand(msg, stream);
      if (!tower_queues_[worker_id]->Write(command)) {
        LOG_WARN(options_.info_log,
                 "Failed to send %s FindTailSeqno to tower stream %llu",
                 uuid.ToString().c_str(),
                 stream->GetStreamID());
      } else {
        LOG_INFO(options_.info_log,
          "Sent %s FindTailSeqno to tower stream %llu",
          uuid.ToString().c_str(),
          stream->GetStreamID());
      }
    }
  }

  if (topic.towers.size() < expected_towers_per_log) {
    // Still not enough tower subscriptions, so put onto orphan list.
    // This will happen if e.g. sending the subscription failed due to full
    // queue, or if there simply aren't any control towers currently available.
    ReScheduleResubscribeRequest(uuid, topic, new_seqno, have_zero_sub);
  } else {
    if (resub_needed) {
      // We successfully resubscribed to all towers, so add to checkup list
      // (or push to the back of the queue, since subscriptions are up to date).
      topic_checkup_list_.Add(uuid);
    }
  }
}

//
// Inserts an entry into the rollcall topic.
//
void
CopilotWorker::RollcallWrite(const SubscriptionID sub_id,
                             const TenantID tenant_id,
                             const TopicUUID& topic,
                             const MetadataType type,
                             const LogID logid,
                             int worker_id,
                             StreamID origin) {
  if (!options_.rollcall_enabled) {
    return;
  }

  // Write to rollcall topic failed. If this was a 'subscription' event,
  // then send unsubscribe message to copilot worker. This will send an
  // unsubscribe response to appropriate client.
  //
  // If the write fails, process_error will be called asynchronously, so it
  // cannot reference local variables.
  std::function<void()> process_error;
  if (type == MetadataType::mSubscribe) {
    process_error = [this, worker_id, origin, tenant_id, sub_id]() {
      // We can't do any proper error handling from this thread, as it belongs
      // to the client used by RollCall.
      std::unique_ptr<Command> command(MakeExecuteCommand(
        [this, worker_id, origin, tenant_id, sub_id]() {
          // Start the automatic unsubscribe process. We rely on the assumption
          // that the unsubscribe request can fail only if the client is
          // un-communicable, in which case the client's subscriptions are
          // reaped.
          const auto reason = MessageUnsubscribe::Reason::kRequested;
          ProcessUnsubscribe(tenant_id, sub_id, reason, worker_id, origin);

          // Send back message to the client, saying that it should resubscribe.
          MessageUnsubscribe msg(tenant_id, sub_id, reason);
          auto unsub_command = options_.msg_loop->ResponseCommand(msg, origin);
          client_queues_[worker_id]->Write(unsub_command);

          stats_.rollcall_writes_failed->Add(1);
        }));

      if (!rollcall_error_queues_->GetThreadLocal()->Write(command)) {
        LOG_ERROR(options_.info_log,
                  "Failed to process RollCall writes failure");
      }
    };
  }

  // This callback is called when the write to the rollcall topic is complete
  auto publish_callback = [this, process_error]
                          (Status status) {
    if (!status.ok() && process_error) {
      process_error();
    }
  };

  // Issue the write to rollcall topic
  Status status = rollcall_->WriteEntry(
                               tenant_id,
                               topic,
                               static_cast<size_t>(logid),
                               type == MetadataType::mSubscribe ? true : false,
                               publish_callback,
                               options_.rollcall_max_batch_size_bytes);
  stats_.rollcall_writes_total->Add(1);
  if (status.ok()) {
    LOG_INFO(options_.info_log,
             "Send rollcall write (%ssubscribe) for %s",
             type == MetadataType::mSubscribe ? "" : "un",
             topic.ToString().c_str());
  } else {
    LOG_WARN(options_.info_log,
             "Failed to send rollcall write (%ssubscribe) for %s status %s",
             type == MetadataType::mSubscribe ? "" : "un",
             topic.ToString().c_str(),
             status.ToString().c_str());
    // If we are unable to write to the rollcall topic and it is a subscription
    // request, then we need to terminate that subscription.
    if (process_error) {
      process_error();
    }
  }

  rollcall_->CheckBatchTimeouts(options_.rollcall_flush_latency);
}

StreamSocket* CopilotWorker::GetControlTowerSocket(const HostId& tower,
                                                   MsgLoop* msg_loop,
                                                   int outgoing_worker_id) {
  auto& tower_sockets = control_tower_sockets_[tower];
  auto it = tower_sockets.find(outgoing_worker_id);
  if (it == tower_sockets.end()) {
    it = tower_sockets.emplace(outgoing_worker_id,
                               msg_loop->CreateOutboundStream(
                                   tower, outgoing_worker_id))
             .first;
    stats_.control_tower_socket_creations->Add(1);
  }
  return &it->second;
}

void CopilotWorker::AdvanceTowers(TopicState* topic,
                                  SequenceNumber prev,
                                  SequenceNumber next,
                                  StreamID origin,
                                  SubscriptionID sub_id) {
  assert(topic);
  for (auto& tower : topic->towers) {
    if (tower.stream->GetStreamID() == origin && tower.sub_id == sub_id) {
      if (prev <= tower.next_seqno &&
          next >= tower.next_seqno &&
          !(prev == 0 && tower.next_seqno != 0) &&
          !(prev != 0 && tower.next_seqno == 0)) {
        LOG_DEBUG(options_.info_log,
                  "Tower subscription %llu advanced from %" PRIu64
                  " to %" PRIu64,
                  tower.stream->GetStreamID(),
                  tower.next_seqno,
                  next + 1);
        tower.next_seqno = next + 1;
      } else {
        stats_.out_of_order_seqno_from_tower->Add(1);
      }
      return;
    }
  }
  stats_.message_from_unexpected_tower->Add(1);
}


std::string CopilotWorker::GetTowersForLog(LogID log_id) const {
  std::string result;
  std::vector<HostId const*> towers;
  if (GetControlTowers(log_id, &towers).ok()) {
    for (HostId const* tower : towers) {
      result += tower->ToString();
      result += '\n';
    }
  } else {
    result = "No towers for log";
  }
  return result;
}

std::string CopilotWorker::GetSubscriptionInfo(std::string filter,
                                               int max) const {
  std::string result;
  for (const auto& entry : topics_) {
    char buffer[4096];
    const TopicUUID& topic = entry.first;
    const TopicState& state = entry.second;
    std::string topic_name = topic.ToString();
    if (!strstr(topic_name.c_str(), filter.c_str())) {
      continue;
    }
    if (!max--) {
      break;
    }
    int n = 0;
    n += snprintf(buffer + n, sizeof(buffer),
                  "%s.log_id: %" PRIu64 "\n",
                  topic_name.c_str(), state.log_id);
    n += snprintf(buffer + n, sizeof(buffer),
                  "%s.subscription_count: %zu\n",
                  topic_name.c_str(), state.subscriptions.size());
    n += snprintf(buffer + n, sizeof(buffer),
                  "%s.records_sent: %" PRIu32 "\n",
                  topic_name.c_str(), state.records_sent);
    n += snprintf(buffer + n, sizeof(buffer),
                  "%s.gaps_sent: %" PRIu32 "\n",
                  topic_name.c_str(), state.gaps_sent);
    size_t t = 0;
    for (const TopicState::Tower& tower : state.towers) {
      n += snprintf(buffer + n, sizeof(buffer),
                    "%s.tower[%zu].next_seqno: %" PRIu64 "\n",
                    topic_name.c_str(), t, tower.next_seqno);
      ++t;
    }
    result += buffer;
  }
  return result;
}

bool CopilotWorker::CorrectTopicTowers(TopicState& topic) {
  std::vector<HostId const*> recipients;
  if (GetControlTowers(topic.log_id, &recipients).ok()) {
    // Update subscription on all control towers.
    for (HostId const* recipient : recipients) {
      // Find or open a new stream socket to this control tower.
      int worker_id = copilot_->GetTowerWorker(topic.log_id, *recipient);
      auto socket = GetControlTowerSocket(
        *recipient, options_.msg_loop, worker_id);

      // Check topic is subscribed to this tower.
      if (!topic.FindTower(socket)) {
        return false;
      }
    }
  }
  return true;
}

Status CopilotWorker::GetControlTowers(LogID log_id,
                                       std::vector<HostId const*>* out) const {
  Status st;
  auto it = control_tower_cache_.find(log_id);
  if (it != control_tower_cache_.end()) {
    *out = it->second;
  } else {
    st = control_tower_router_->GetControlTowers(log_id, out);
    if (st.ok()) {
      control_tower_cache_.emplace(log_id, *out);
    }
  }
  return st;
}

/***
 * Resubscription
 */

// Gets rid of all cancelled requests
void CopilotWorker::CleanRequestQueues() {
  CleanRequestQueue(current_resubscribe_request_queue_);
  CleanRequestQueue(pending_resubscribe_request_queue_);


  // Queue with next active request should always be the 'current' queue.
  if (current_resubscribe_request_queue_.size() == 0
      && pending_resubscribe_request_queue_.size() > 0) {
    std::swap(
        current_resubscribe_request_queue_,
        pending_resubscribe_request_queue_);
  }

}

void CopilotWorker::CleanRequestQueue(ResubscribeRequestQueue& request_queue) {
  while (request_queue.size() > 0) {
    if (!request_queue.top()->cancelled) {
      return; // Active request is now at front
    }
    request_queue.pop();
  }
}

bool CopilotWorker::HasActiveResubscribeRequests() {
  return !active_resubscribe_requests_by_topic_.empty();
}

bool CopilotWorker::HasActiveResubscribeRequest(const TopicUUID& topic_uuid) {
  auto active_subscription_it
    = active_resubscribe_requests_by_topic_.find(topic_uuid);

  return active_subscription_it != active_resubscribe_requests_by_topic_.end();
}

void CopilotWorker::CancelResubscribeRequest(const TopicUUID& topic_uuid) {
  auto request_it = active_resubscribe_requests_by_topic_.find(topic_uuid);
  if (request_it != active_resubscribe_requests_by_topic_.end()) {
    request_it->second->cancelled = true;
    active_resubscribe_requests_by_topic_.erase(request_it);
  }
  // Note: cancelled request will be destroyed when it is popped.
}

CopilotWorker::SafeResubscribeRequest
CopilotWorker::PopNextResubscribeRequest() {
  CleanRequestQueues();

  // const_cast is to get around bug in priority_queue,
  // which prevents moving unique_ptr item when calling top().
  SafeResubscribeRequest top_request
    = std::move(const_cast<SafeResubscribeRequest&>(
          current_resubscribe_request_queue_.top()));

  current_resubscribe_request_queue_.pop();
  active_resubscribe_requests_by_topic_.erase(top_request->topic_uuid);

  return top_request;
}

void CopilotWorker::ReScheduleResubscribeRequest(
    const TopicUUID& topic_uuid,
    const TopicState& topic_state,
    const SequenceNumber new_seqno,
    const bool have_zero_sub) {

  // A re-scheduled request should go in the next batch.
  ScheduleResubscribeRequest(
      topic_uuid,
      topic_state,
      new_seqno,
      have_zero_sub,
      pending_resubscribe_request_queue_);
}

void CopilotWorker::ScheduleResubscribeRequest(
    const TopicUUID& topic_uuid, const TopicState& topic_state) {

  bool have_zero_sub = false;
  auto new_seqno = FindLowestSequenceNumber(topic_state, &have_zero_sub);

  // If the next batch has started filling up (i.e. there was a previous error),
  // then use next batch, otherwise use current batch.
  ResubscribeRequestQueue& request_queue =
      pending_resubscribe_request_queue_.size() > 0 ?
          pending_resubscribe_request_queue_ :
          current_resubscribe_request_queue_;

  ScheduleResubscribeRequest(
      topic_uuid,
      topic_state,
      new_seqno,
      have_zero_sub,
      request_queue);
}

void CopilotWorker::ScheduleResubscribeRequest(
    const TopicUUID& topic_uuid,
    const TopicState& topic_state,
    const SequenceNumber new_seqno,
    const bool have_zero_sub,
    ResubscribeRequestQueue& resubscribe_request_queue) {

  if (HasActiveResubscribeRequest(topic_uuid)) {
    return;
  }

  auto request = new ResubscribeRequest(
      topic_uuid, new_seqno, have_zero_sub, false);

  resubscribe_request_queue.push(SafeResubscribeRequest(request));
  active_resubscribe_requests_by_topic_.insert(
      std::make_pair(topic_uuid, request));
}

}  // namespace rocketspeed
