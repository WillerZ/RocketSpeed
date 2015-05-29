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
#include "src/copilot/copilot.h"
#include "src/util/control_tower_router.h"
#include "src/util/hostmap.h"

#include "external/folly/move_wrapper.h"

namespace rocketspeed {

CopilotWorker::CopilotWorker(
    const CopilotOptions& options,
    std::shared_ptr<ControlTowerRouter> control_tower_router,
    const int myid,
    Copilot* copilot)
: options_(options)
, control_tower_router_(std::move(control_tower_router))
, copilot_(copilot)
, myid_(myid) {
  // copilot is required.
  assert(copilot_);

  LOG_INFO(options_.info_log, "Created a new CopilotWorker");
  options_.info_log->Flush();
}

bool CopilotWorker::Forward(LogID logid,
                            std::unique_ptr<Message> msg,
                            int worker_id,
                            StreamID origin) {
  auto moved_msg = folly::makeMoveWrapper(std::move(msg));
  std::unique_ptr<ExecuteCommand> command(
    new ExecuteCommand([this, moved_msg, logid, worker_id, origin]() mutable {
      auto message = moved_msg.move();
      switch (message->GetMessageType()) {
        case MessageType::mMetadata: {
          auto metadata = static_cast<MessageMetadata*>(message.get());
          const std::vector<TopicPair>& topics = metadata->GetTopicInfo();
          // Workers only handle 1 topic at a time.
          assert(topics.size() == 1);
          const TopicPair& request = topics[0];

          if (metadata->GetMetaType() ==
              MessageMetadata::MetaType::Response) {
            if (request.topic_type == MetadataType::mSubscribe) {
              // Response from Control Tower.
              ProcessMetadataResponse(request, logid, worker_id);
            }
          }
        } break;

        case MessageType::mDeliver: {
          ProcessDeliver(std::move(message), origin);
        } break;

        case MessageType::mGap: {
          ProcessGap(std::move(message), origin);
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
  return options_.msg_loop->SendCommand(std::move(command), myid_).ok();
}

bool CopilotWorker::Forward(std::shared_ptr<ControlTowerRouter> new_router) {
  std::unique_ptr<ExecuteCommand> command(new ExecuteCommand(
      std::bind(&CopilotWorker::ProcessRouterUpdate, this, new_router)));
  return options_.msg_loop->SendCommand(std::move(command), myid_).ok();
}

const Statistics& CopilotWorker::GetStatistics() {
  stats_.subscribed_topics->Set(topics_.size());

  size_t total_sockets = 0;
  for (const auto& entry : control_tower_sockets_) {
    total_sockets += entry.second.size();
  }
  stats_.control_tower_sockets->Set(total_sockets);
  return stats_.all;
}

void CopilotWorker::ProcessMetadataResponse(const TopicPair& request,
                                            LogID logid,
                                            int worker_id) {
  LOG_INFO(options_.info_log,
      "Received %ssubscribe response for Topic(%s)",
      request.topic_type == MetadataType::mSubscribe ? "" : "un",
      request.topic_name.c_str());

  if (request.topic_type == MetadataType::mUnSubscribe) {
    // TODO(pja) 1 : Need to handle this, but control tower doesn't currently
    // send unsolicited unsubscribes, so there's no use case for this yet.
    // The only time control tower will send us an unsubscribe is if we ask
    // for one, in which case we've already handled it.
  }
}

void CopilotWorker::ProcessDeliver(std::unique_ptr<Message> message,
                                   StreamID origin) {
  MessageData* msg = static_cast<MessageData*>(message.get());
  // Get the list of subscriptions for this topic.
  LOG_INFO(options_.info_log,
      "Copilot received data (%.16s)@%" PRIu64 " for Topic(%s)",
      msg->GetPayload().ToString().c_str(),
      msg->GetSequenceNumber(),
      msg->GetTopicName().ToString().c_str());

  TopicUUID uuid(msg->GetNamespaceId(), msg->GetTopicName());
  auto it = topics_.find(uuid);
  if (it != topics_.end()) {
    TopicState& topic = it->second;
    const auto seqno = msg->GetSequenceNumber();
    const auto prev_seqno = msg->GetPrevSequenceNumber();

    // Find tower for this origin and update its state.
    AdvanceTowers(&topic, prev_seqno, seqno, origin);

    // Send to all subscribers.
    bool delivered_at_least_once = false;
    for (auto& sub : topic.subscriptions) {
      StreamID recipient = sub->stream_id;

      // Do not send a response if the seqno is too low.
      if (sub->seqno > seqno) {
        LOG_INFO(options_.info_log,
                 "Data not delivered to %llu"
                 " (seqno@%" PRIu64 " too low, currently @%" PRIu64 ")",
                 recipient,
                 seqno,
                 sub->seqno);
        continue;
      }

      // or too high.
      if (sub->seqno < prev_seqno) {
        LOG_INFO(options_.info_log,
                 "Data not delivered to %llu"
                 " (prev_seqno@%" PRIu64 " too high, currently @%" PRIu64 ")",
                 recipient,
                 prev_seqno,
                 sub->seqno);
        continue;
      }

      // or not matching zeroes.
      if ((sub->seqno == 0 && prev_seqno != 0) ||
          (sub->seqno != 0 && prev_seqno == 0)) {
        LOG_INFO(options_.info_log,
                 "Data not delivered to %llu"
                 " (prev_seqno@%" PRIu64 " not 0)",
                 recipient,
                 prev_seqno);
        continue;
      }

      // Mark even if fail to send.
      // The point is that it wasn't out of order.
      delivered_at_least_once = true;

      // Send message to the client.
      MessageDeliverData data(sub->tenant_id,
                              sub->sub_id,
                              msg->GetMessageId(),
                              msg->GetPayload());
      data.SetSequenceNumbers(prev_seqno, seqno);
      Status status = options_.msg_loop->SendResponse(data,
                                                      recipient,
                                                      sub->worker_id);

      if (status.ok()) {
        sub->seqno = seqno + 1;

        LOG_INFO(options_.info_log,
                 "Sent data (%.16s)@%" PRIu64 " for ID(%" PRIu64
                 ") Topic(%s) to %llu",
                 msg->GetPayload().ToString().c_str(),
                 msg->GetSequenceNumber(),
                 data.GetSubID(),
                 msg->GetTopicName().ToString().c_str(),
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
  MessageGap* msg = static_cast<MessageGap*>(message.get());
  // Get the list of subscriptions for this topic.
  LOG_INFO(options_.info_log,
      "Copilot received gap %" PRIu64 "-%" PRIu64 " for Topic(%s)",
      msg->GetStartSequenceNumber(),
      msg->GetEndSequenceNumber(),
      msg->GetTopicName().c_str());
  TopicUUID uuid(msg->GetNamespaceId(), msg->GetTopicName());
  auto it = topics_.find(uuid);
  if (it != topics_.end()) {
    TopicState& topic = it->second;
    const auto prev_seqno = msg->GetStartSequenceNumber();
    const auto next_seqno = msg->GetEndSequenceNumber();

    // Find tower for this origin and update its state.
    AdvanceTowers(&topic, prev_seqno, next_seqno, origin);

    // Send to all subscribers.
    bool delivered_at_least_once = false;
    for (auto& sub : topic.subscriptions) {
      StreamID recipient = sub->stream_id;

      // Ignore if the seqno is too low.
      if (sub->seqno > next_seqno) {
        LOG_INFO(options_.info_log,
                 "Gap ignored for %llu"
                 " (next_seqno@%" PRIu64 " too low, currently @%" PRIu64 ")",
                 recipient,
                 next_seqno,
                 sub->seqno);
        continue;
      }

      // or too high.
      if (sub->seqno < prev_seqno) {
        LOG_INFO(options_.info_log,
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
        LOG_INFO(options_.info_log,
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
      MessageDeliverGap gap(sub->tenant_id, sub->sub_id, msg->GetType());
      gap.SetSequenceNumbers(prev_seqno, next_seqno);
      Status status = options_.msg_loop->SendResponse(gap,
                                                      recipient,
                                                      sub->worker_id);

      if (status.ok()) {
        sub->seqno = next_seqno + 1;

        LOG_INFO(options_.info_log,
                 "Sent gap %" PRIu64 "-%" PRIu64
                 " for  subscription ID(%" PRIu64 ") Topic(%s) to %llu",
                 msg->GetStartSequenceNumber(),
                 msg->GetEndSequenceNumber(),
                 gap.GetSubID(),
                 msg->GetTopicName().c_str(),
                 recipient);
      } else {
        LOG_WARN(
            options_.info_log, "Failed to distribute gap to %llu", recipient);
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

void CopilotWorker::ProcessSubscribe(const TenantID tenant_id,
                                     const NamespaceID& namespace_id,
                                     const Topic& topic_name,
                                     const SequenceNumber start_seqno,
                                     const SubscriptionID sub_id,
                                     const LogID logid,
                                     const int worker_id,
                                     const StreamID subscriber) {
  LOG_INFO(options_.info_log,
      "Received subscribe request ID(%" PRIu64
      ") for Topic(%s)@%" PRIu64 " for %llu",
      sub_id,
      topic_name.c_str(),
      start_seqno,
      subscriber);

  // Insert into client-topic map.
  client_subscriptions_[subscriber]
      .emplace(sub_id, TopicInfo{topic_name, namespace_id, logid});

  // Find/insert topic state.
  TopicUUID uuid(namespace_id, topic_name);
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
                topic_name,
                namespace_id,
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

  RemoveSubscription(tenant_id, sub_id, subscriber, worker_id);
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

    // Update subscriptions to the control tower if necessary.
    UpdateTowerSubscriptions(uuid, topic);

    // Update rollcall topic.
    RollcallWrite(sub_id, tenant_id, topic_name,
                  namespace_id, MetadataType::mUnSubscribe,
                  logid, worker_id, subscriber);

    // No more subscriptions, so remove from map.
    if (topic.subscriptions.empty()) {
      topics_.erase(topic_iter);
    }
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

  // Resubscribe all subscriptions without enough control tower subscriptions.
  for (auto& uuid_topic : topics_) {
    const TopicUUID& uuid = uuid_topic.first;
    TopicState& topic = uuid_topic.second;
    if (topic.towers.size() < options_.control_towers_per_log) {
      UpdateTowerSubscriptions(uuid, topic);
    }
  }
}

void CopilotWorker::CloseControlTowerStream(StreamID stream) {
  // Removes upstream connection for affected subscriptions.
  for (auto& uuid_topic : topics_) {
    TopicState& topic = uuid_topic.second;
    for (auto it = topic.towers.begin(); it != topic.towers.end(); ) {
      if (it->stream->GetStreamID() == stream) {
        it = topic.towers.erase(it);
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

bool CopilotWorker::SendMetadata(TenantID tenant_id,
                                 MetadataType type,
                                 const TopicUUID& uuid,
                                 SequenceNumber seqno,
                                 StreamSocket* stream,
                                 int worker_id) {
  Slice namespace_id;
  Slice topic_name;
  uuid.GetTopicID(&namespace_id, &topic_name);

  MessageMetadata message(
    tenant_id,
    MessageMetadata::MetaType::Request,
    {TopicPair(seqno, topic_name.ToString(), type, namespace_id.ToString())});

  Status status = options_.msg_loop->SendRequest(message, stream, worker_id);
  if (!status.ok()) {
    LOG_WARN(options_.info_log,
      "Failed to send %s %ssubscribe to tower stream %llu",
      uuid.ToString().c_str(),
      type == MetadataType::mUnSubscribe ? "un" : "",
      stream->GetStreamID());
  } else {
    LOG_INFO(options_.info_log,
      "Sent %s %ssubscription to tower stream %llu",
      uuid.ToString().c_str(),
      type == MetadataType::mUnSubscribe ? "un" : "",
      stream->GetStreamID());
  }
  return status.ok();
}

void CopilotWorker::UpdateTowerSubscriptions(const TopicUUID& uuid,
                                             TopicState& topic) {
  LOG_INFO(options_.info_log,
    "Refreshing tower subscriptions for %s",
    uuid.ToString().c_str());

  const LogID log_id = topic.log_id;
  const TenantID tenant_id = GuestTenant;

  if (topic.subscriptions.empty()) {
    // No more subscriptions on this topic, so unsubscribe from control towers.
    for (auto& tower : topic.towers) {
      SendMetadata(tenant_id,
                   MetadataType::mUnSubscribe,
                   uuid,
                   0,  // seqno: irrelevant for unsubscribe
                   tower.stream,
                   tower.worker_id);
    }
    topic.towers.clear();
    return;
  }

  // Find earliest non-zero subscription, or zero if only zero subscriptions.
  bool have_zero_sub = false;
  SequenceNumber new_seqno = 0;
  for (auto& sub : topic.subscriptions) {
    if (sub->seqno != 0) {
      if (new_seqno == 0 || sub->seqno < new_seqno) {
        new_seqno = sub->seqno;
      }
    } else {
      have_zero_sub = true;
    }
  }

  // Note: it could be the case that the only subscriptions we have are
  // subscriptions at 0, so new_seqno will be 0 in that case, and we will
  // subscribe the copilot at 0 without sending a FindTailSeqno request.
  const bool send_latest_request = have_zero_sub && new_seqno != 0;

  // Check if we need to resubscribe to control towers.
  // First check if we have enough tower subscriptions.
  bool resub_needed = topic.towers.size() < options_.control_towers_per_log;
  if (resub_needed) {
    LOG_INFO(options_.info_log,
      "Not enough control tower subscriptions for %s (%zu/%zu), resubscribing",
      uuid.ToString().c_str(),
      topic.towers.size(),
      options_.control_towers_per_log);
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

  // If needed, find a list of control tower connections.
  using TowerConnection = std::pair<StreamSocket*, int>;  // socket + worker_id
  autovector<TowerConnection, kMaxTowerConnections> tower_conns;
  if (resub_needed || send_latest_request) {
    // Find control towers responsible for this topic's log.
    std::vector<HostId const*> recipients;
    if (control_tower_router_->GetControlTowers(log_id, &recipients).ok()) {
      // Update subscription on all control towers.
      for (HostId const* recipient : recipients) {
        int outgoing_worker_id = copilot_->GetTowerWorker(log_id, *recipient);

        // Find or open a new stream socket to this control tower.
        auto socket = GetControlTowerSocket(
            *recipient, options_.msg_loop, outgoing_worker_id);

        tower_conns.emplace_back(socket, outgoing_worker_id);
      }
    } else {
      // This should only ever happen if all control towers are offline.
      LOG_WARN(options_.info_log,
        "Failed to find control towers for log ID %" PRIu64,
        static_cast<uint64_t>(log_id));
    }
  }

  // Do we need new tower subscription for this subscriber?
  if (resub_needed) {
    // Keep track of previous susbcriptions. If we subscribe to different
    // towers then we need to unsubscribe to the old ones.
    auto old_towers = std::move(topic.towers);

    // Clear old subscriptions.
    topic.towers.clear();

    for (TowerConnection& tower_conn : tower_conns) {
      // Send request to control tower to update the copilot subscription.
      StreamSocket* const socket = tower_conn.first;
      const int outgoing_worker_id = tower_conn.second;

      bool success = SendMetadata(tenant_id,
                                  MetadataType::mSubscribe,
                                  uuid,
                                  new_seqno,
                                  socket,
                                  outgoing_worker_id);
      if (success) {
        // Update the towers for the subscription.
        assert(!topic.FindTower(socket));  // we just cleared all towers.
        topic.towers.emplace_back(socket, new_seqno, outgoing_worker_id);
      }
    }

    // Find old_towers that aren't in the new towers.
    for (TopicState::Tower& tower : old_towers) {
      if (!topic.FindTower(tower.stream)) {
        // No longer want to be subscribed to this tower, so unsubscribe.
        SendMetadata(tenant_id,
                     MetadataType::mUnSubscribe,
                     uuid,
                     0,  // seqno: irrelevant for unsubscribe
                     tower.stream,
                     tower.worker_id);
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
      Status status = options_.msg_loop->SendRequest(msg, stream, worker_id);
      if (!status.ok()) {
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
}

//
// Inserts an entry into the rollcall topic.
//
void
CopilotWorker::RollcallWrite(const SubscriptionID sub_id,
                             const TenantID tenant_id,
                             const Topic& topic_name,
                             const NamespaceID& namespace_id,
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
    process_error = [this, topic_name, namespace_id, logid, worker_id, origin,
                     tenant_id, sub_id]() {
        std::unique_ptr<Message> newmsg(new MessageUnsubscribe(
            tenant_id, sub_id, MessageUnsubscribe::Reason::kRequested));
        // Start the automatic unsubscribe process. We rely on the assumption
        // that the unsubscribe request can fail only if the client is
        // un-communicable, in which case the client's subscritions are reaped.
        Forward(logid, std::move(newmsg), worker_id, origin);
        // Send back message to the client, saying that it should resubscribe.
        MessageUnsubscribe unsubscribe(tenant_id,
                                       sub_id,
                                       MessageUnsubscribe::Reason::kRequested);
        options_.msg_loop->SendResponse(unsubscribe, origin, worker_id);
        // TODO(dhruba)
        // We can't do any proper error handling from this thread, as it belongs
        // to the client used by RollCall.

        // The counter must be updtaed from worker thread.
        Status st = options_.msg_loop->SendCommand(
            std::unique_ptr<Command>(new ExecuteCommand(
                [this]() { stats_.rollcall_writes_failed->Add(1); })),
            myid_);
        if (!st.ok()) {
          LOG_ERROR(options_.info_log,
                    "Failed to increment failed RollCall writes counter");
        }
    };
  }

  // This callback is called when the write to the rollcall topic is complete
  auto publish_callback = [this, process_error]
                          (std::unique_ptr<ResultStatus> status) {
    if (!status->GetStatus().ok() && process_error) {
      process_error();
    }
  };

  // Issue the write to rollcall topic
  Status status = copilot_->GetRollcallLogger()->WriteEntry(
                               tenant_id,
                               topic_name,
                               namespace_id,
                               type == MetadataType::mSubscribe ? true : false,
                               publish_callback);
  stats_.rollcall_writes_total->Add(1);
  if (status.ok()) {
    LOG_INFO(options_.info_log,
             "Send rollcall write (%ssubscribe) for Topic(%s) in "
             "namespace %s",
             type == MetadataType::mSubscribe ? "" : "un",
             topic_name.c_str(),
             namespace_id.c_str());
  } else {
    LOG_INFO(options_.info_log,
             "Failed to send rollcall write (%ssubscribe) for Topic(%s) in "
             "namespace %s status %s",
             type == MetadataType::mSubscribe ? "" : "un",
             topic_name.c_str(),
             namespace_id.c_str(),
             status.ToString().c_str());
    // If we are unable to write to the rollcall topic and it is a subscription
    // request, then we need to terminate that subscription.
    if (process_error) {
      process_error();
    }
  }
}

StreamSocket* CopilotWorker::GetControlTowerSocket(const HostId& tower,
                                                   MsgLoop* msg_loop,
                                                   int outgoing_worker_id) {
  auto& tower_sockets = control_tower_sockets_[tower];
  auto it = tower_sockets.find(outgoing_worker_id);
  if (it == tower_sockets.end()) {
    it = tower_sockets.emplace(
      outgoing_worker_id,
      msg_loop->CreateOutboundStream(
        tower.ToClientId(), outgoing_worker_id)).first;
    stats_.control_tower_socket_creations->Add(1);
  }
  return &it->second;
}

void CopilotWorker::AdvanceTowers(TopicState* topic,
                                  SequenceNumber prev,
                                  SequenceNumber next,
                                  StreamID origin) {
  assert(topic);
  for (auto& tower : topic->towers) {
    if (tower.stream->GetStreamID() == origin) {
      if (prev <= tower.next_seqno &&
          next >= tower.next_seqno &&
          !(prev == 0 && tower.next_seqno != 0) &&
          !(prev != 0 && tower.next_seqno == 0)) {
        LOG_INFO(options_.info_log,
          "Tower subscription %llu advanced from %" PRIu64 " to %" PRIu64,
          tower.stream->GetStreamID(),
          tower.next_seqno,
          next + 1);
        tower.next_seqno = next + 1;
      } else {
        stats_.out_of_order_seqno_from_tower->Add(1);
      }
      break;
    }
  }
  stats_.message_from_unexpected_tower->Add(1);
}

std::string CopilotWorker::GetTowersForLog(LogID log_id) const {
  std::string result;
  std::vector<HostId const*> towers;
  if (control_tower_router_->GetControlTowers(log_id, &towers).ok()) {
    for (HostId const* tower : towers) {
      result += tower->ToString();
      result += '\n';
    }
  } else {
    result = "No towers for log";
  }
  return result;
}

std::string CopilotWorker::GetSubscriptionInfo(std::string filter) const {
  std::string result;
  for (const auto& entry : topics_) {
    char buffer[4096];
    const TopicUUID& topic = entry.first;
    const TopicState& state = entry.second;
    std::string topic_name = topic.ToString();
    if (!strstr(topic_name.c_str(), filter.c_str())) {
      continue;
    }
    int n = 0;
    n += snprintf(buffer + n, sizeof(buffer),
                  "%s.log_id: %" PRIu64 "\n",
                  topic_name.c_str(), state.log_id);
    n += snprintf(buffer + n, sizeof(buffer),
                  "%s.subscription_count: %zu\n",
                  topic_name.c_str(), state.subscriptions.size());
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

}  // namespace rocketspeed
