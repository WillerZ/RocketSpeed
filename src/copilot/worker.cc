// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "src/copilot/worker.h"
#include <vector>
#include "include/Status.h"
#include "include/Types.h"
#include "src/copilot/copilot.h"
#include "src/util/control_tower_router.h"
#include "src/util/hostmap.h"

namespace rocketspeed {

CopilotWorker::CopilotWorker(
    const CopilotOptions& options,
    std::shared_ptr<ControlTowerRouter> control_tower_router,
    const int myid,
    Copilot* copilot)
: worker_loop_(options.worker_queue_size)
, options_(options)
, control_tower_router_(std::move(control_tower_router))
, copilot_(copilot)
, myid_(myid) {
  // copilot is required.
  assert(copilot_);

  LOG_INFO(options_.info_log, "Created a new CopilotWorker");
  options_.info_log->Flush();
}

void CopilotWorker::Run() {
  LOG_INFO(options_.info_log, "Starting worker loop");
  worker_loop_.Run([this] (CopilotWorkerCommand command) {
    CommandCallback(std::move(command));
  });
}

bool CopilotWorker::Forward(LogID logid,
                            std::unique_ptr<Message> msg,
                            int worker_id,
                            StreamID origin) {
  return worker_loop_.Send(logid, std::move(msg), worker_id, origin);
}

bool CopilotWorker::Forward(std::shared_ptr<ControlTowerRouter> new_router) {
  return worker_loop_.Send(std::move(new_router));
}

void CopilotWorker::CommandCallback(CopilotWorkerCommand command) {
  // Process CopilotWorkerCommand
  if (command.IsRouterUpdate()) {
    ProcessRouterUpdate(command.GetRouterUpdate());
  } else {
    std::unique_ptr<Message> message = command.GetMessage();
    Message* msg = message.get();
    assert(msg);

    switch (msg->GetMessageType()) {
    case MessageType::mMetadata: {
        MessageMetadata* metadata = static_cast<MessageMetadata*>(msg);
        const std::vector<TopicPair>& topics = metadata->GetTopicInfo();
        assert(topics.size() == 1);  // Workers only handle 1 topic at a time.
        const TopicPair& request = topics[0];

        if (metadata->GetMetaType() == MessageMetadata::MetaType::Response) {
          if (request.topic_type == MetadataType::mSubscribe) {
            // Response from Control Tower.
            ProcessMetadataResponse(request,
                                    command.GetLogID(),
                                    command.GetWorkerId());
          }
        }
      }
      break;

    case MessageType::mDeliver: {
        // Data to forward to client.
        ProcessDeliver(std::move(message));
      }
      break;

    case MessageType::mGap: {
        // Data to forward to client.
        ProcessGap(std::move(message));
      }
      break;

    case MessageType::mSubscribe: {
      // Subscribe request from a client.
      auto subscribe = static_cast<MessageSubscribe*>(message.get());
      ProcessSubscribe(subscribe->GetTenantID(),
                       subscribe->GetNamespace(),
                       subscribe->GetTopicName(),
                       subscribe->GetStartSequenceNumber(),
                       subscribe->GetSubID(),
                       command.GetLogID(),
                       command.GetWorkerId(),
                       command.GetOrigin());
    } break;

    case MessageType::mUnsubscribe: {
      auto unsubscribe = static_cast<MessageUnsubscribe*>(message.get());
      ProcessUnsubscribe(unsubscribe->GetTenantID(),
                         unsubscribe->GetSubID(),
                         unsubscribe->GetReason(),
                         command.GetWorkerId(),
                         command.GetOrigin());
    } break;

    case MessageType::mGoodbye: {
        ProcessGoodbye(std::move(message), command.GetOrigin());
      }
      break;

    default: {
        LOG_WARN(options_.info_log,
            "Unexpected message type in copilot worker %d",
            msg->GetMessageType());
      }
      break;
    }
  }
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

void CopilotWorker::ProcessDeliver(std::unique_ptr<Message> message) {
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

    // Send to all subscribers.
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
      if (sub->seqno == 0 && prev_seqno != 0) {
        LOG_INFO(options_.info_log,
                 "Data not delivered to %llu"
                 " (prev_seqno@%" PRIu64 " not 0)",
                 recipient,
                 prev_seqno);
        continue;
      }

      // Send message to the client.
      MessageDeliverData data(sub->tenant_id, sub->sub_id, msg->GetPayload());
      data.SetSequenceNumbers(prev_seqno, seqno);
      Status status = options_.msg_loop->SendResponse(data,
                                                      recipient,
                                                      sub->worker_id);
      if (status.ok()) {
        sub->seqno = seqno + 1;

        LOG_INFO(options_.info_log,
                 "Sent data (%.16s)@%" PRIu64 " for ID(%u) Topic(%s) to %llu",
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
  }
}

void CopilotWorker::ProcessGap(std::unique_ptr<Message> message) {
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

    // Send to all subscribers.
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
      if (sub->seqno == 0 && prev_seqno != 0) {
        LOG_INFO(options_.info_log,
                 "Gap ignored for %llu"
                 " (prev_seqno@%" PRIu64 " not 0)",
                 recipient,
                 prev_seqno);
        continue;
      }

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
                 " for  subscription ID(%u) Topic(%s) to %llu",
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
      "Received subscribe request ID(%u) for Topic(%s)@%" PRIu64 " for %llu",
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
  Subscription* this_sub = nullptr;
  SequenceNumber earliest_seqno = start_seqno + 1;
  Subscription* earliest_sub = nullptr;
  for (auto& sub : topic.subscriptions) {
    if (sub->stream_id == subscriber && sub->sub_id == sub_id) {
      assert(!this_sub);  // should never have a duplicate subscription
      if (sub->seqno != start_seqno) {
        sub->towers.clear();  // new upstream sub needed
      }
      sub->seqno = start_seqno;
      sub->worker_id = worker_id;
      this_sub = sub.get();
    }
    // Find earliest seqno subscription for this topic.
    if (sub->seqno != 0) {
      earliest_seqno = std::min(earliest_seqno, sub->seqno);
      earliest_sub = sub.get();
    }
  }

  if (!this_sub) {
    // No existing subscription, so insert new one.
    topic.subscriptions.emplace_back(
      new Subscription(subscriber,
                       start_seqno,
                       worker_id,
                       tenant_id,
                       sub_id));
    this_sub = topic.subscriptions.back().get();
  }

  // Do we need new tower subscription for this subscriber?
  if (this_sub->towers.empty()) {
    // Check if existing tower subscription can be used.
    if (earliest_sub &&
        earliest_seqno <= start_seqno &&
        !earliest_sub->towers.empty()) {
      // Use the same towers and let them catch up.
      this_sub->towers = earliest_sub->towers;
    } else {
      // Find control tower responsible for this topic's log.
      HostId const* recipient = nullptr;
      if (control_tower_router_->GetControlTower(logid, &recipient).ok()) {
        int outgoing_worker_id = copilot_->GetLogWorker(logid, *recipient);

        // Find or open a new stream socket to this control tower.
        auto socket = GetControlTowerSocket(
            *recipient, options_.msg_loop, outgoing_worker_id);

        // Send request to control tower to update the copilot subscription.
        MessageMetadata message(
            tenant_id, MessageMetadata::MetaType::Request,
            {TopicPair(start_seqno,
                       topic_name,
                       MetadataType::mSubscribe,
                       namespace_id)});
        Status status = options_.msg_loop->SendRequest(message,
                                                       socket,
                                                       outgoing_worker_id);
        if (!status.ok()) {
          LOG_WARN(options_.info_log,
            "Failed to send subscribe to %s",
            recipient->ToString().c_str());
        } else {
          LOG_INFO(options_.info_log,
            "Sent subscription for Topic(%s)@%" PRIu64 " to %s",
            topic_name.c_str(), start_seqno,
            recipient->ToString().c_str());

          // Update the towers for the subscription.
          if (this_sub) {
            StreamID stream_id = socket->GetStreamID();
            Subscription::Tower* tower = this_sub->FindTower(stream_id);
            if (!tower) {
              this_sub->towers.emplace_back(stream_id);
            }
          }

          // Update rollcall topic.
          RollcallWrite(sub_id, tenant_id,
                        topic_name, namespace_id,
                        MetadataType::mSubscribe, logid, worker_id, subscriber);
        }
      } else {
        // This should only ever happen if all control towers are offline.
        LOG_WARN(options_.info_log,
          "Failed to find control tower for log ID %" PRIu64,
          static_cast<uint64_t>(logid));
      }
    }
  }
}

void CopilotWorker::ProcessUnsubscribe(TenantID tenant_id,
                                       SubscriptionID sub_id,
                                       MessageUnsubscribe::Reason reason,
                                       int worker_id,
                                       StreamID subscriber) {
  LOG_INFO(options_.info_log,
           "Received unsubscribe request for ID (%u) on stream %llu",
           sub_id,
           subscriber);

  RemoveSubscription(tenant_id,
                     sub_id,
                     subscriber,
                     worker_id);
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
    SequenceNumber earliest_other_seqno = 0;
    SequenceNumber our_seqno = 0;
    auto& subscriptions = topic.subscriptions;
    for (auto it = subscriptions.begin(); it != subscriptions.end(); ) {
      Subscription* sub = it->get();
      if (sub->stream_id == subscriber && sub->sub_id == sub_id) {
        // This is our subscription, remove it.
        our_seqno = sub->seqno;
        it = subscriptions.erase(it);
      } else {
        // Find earliest other seqno subscription for this topic.
        earliest_other_seqno = std::min(earliest_other_seqno, sub->seqno);
        ++it;
      }
    }

    if (subscriptions.empty()) {
      // No subscriptions on this topic left, tell control tower to unsubscribe
      // this copilot worker.
      HostId const* recipient = nullptr;
      if (control_tower_router_->GetControlTower(logid, &recipient).ok()) {
        // Forward unsubscribe request to control tower, with this copilot
        // worker as the subscriber.
        int outgoing_worker_id = copilot_->GetLogWorker(logid, *recipient);
        MessageMetadata newmsg(tenant_id,
                               MessageMetadata::MetaType::Request,
                               { TopicPair(0,  // seqno doesnt matter
                                           topic_name,
                                           MetadataType::mUnSubscribe,
                                           namespace_id) });

        // Find or open a new stream socket to this control tower.
        auto socket = GetControlTowerSocket(
            *recipient, options_.msg_loop, outgoing_worker_id);

        // Send the message.
        Status status = options_.msg_loop->SendRequest(newmsg,
                                                       socket,
                                                       outgoing_worker_id);
        if (!status.ok()) {
          LOG_INFO(options_.info_log,
              "Failed to send unsubscribe request to %s",
              recipient->ToString().c_str());
        }
      }

      // Remove from topic map.
      topics_.erase(topic_iter);
    } else if (our_seqno < earliest_other_seqno) {
      // Need to update control tower.
      HostId const* recipient = nullptr;
      if (control_tower_router_->GetControlTower(logid, &recipient).ok()) {
        // Re subscribe our control tower subscription with the later seqno.
        int outgoing_worker_id = copilot_->GetLogWorker(logid, *recipient);
        MessageMetadata newmsg(tenant_id,
                               MessageMetadata::MetaType::Request,
                               { TopicPair(earliest_other_seqno,
                                           topic_name,
                                           MetadataType::mSubscribe,
                                           namespace_id) });

        // Find or open a new stream socket to this control tower.
        auto socket = GetControlTowerSocket(
            *recipient, options_.msg_loop, outgoing_worker_id);

        // Send the message.
        Status status = options_.msg_loop->SendRequest(newmsg,
                                                       socket,
                                                       outgoing_worker_id);
          if (!status.ok()) {
          LOG_INFO(options_.info_log,
              "Failed to send unsubscribe request to %s",
              recipient->ToString().c_str());
        }
      } else {
        // This should only ever happen if all control towers are offline.
        LOG_WARN(options_.info_log,
          "Failed to find control tower for log ID %" PRIu64,
          logid);
      }
    }
    // Update rollcall topic.
    RollcallWrite(sub_id, tenant_id, topic_name,
                  namespace_id, MetadataType::mUnSubscribe,
                  logid, worker_id, subscriber);
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

  // Resubscribe all subscriptions that don't have a control tower subscription.
  for (auto& uuid_topic : topics_) {
    const TopicUUID& uuid = uuid_topic.first;
    for (auto& sub : uuid_topic.second.subscriptions) {
      if (sub->towers.empty()) {
        ResendSubscriptions(uuid_topic.second.log_id, uuid, sub.get());
      }
    }
  }
}

void CopilotWorker::CloseControlTowerStream(StreamID stream) {
  // Update control_tower_sockets_.
  // Removes all entries with StreamID == stream.
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

  // Resubscribe all subscriptions being served by the removed stream.
  for (auto& uuid_topic : topics_) {
    const TopicUUID& uuid = uuid_topic.first;
    for (auto& sub : uuid_topic.second.subscriptions) {
      // sub->towers are the tower streams providing data for this subscription.
      // Remove the streams that matches the closed stream.
      bool affected = false;
      for (auto it = sub->towers.begin(); it != sub->towers.end(); ) {
        if (it->stream_id == stream) {
          affected = true;
          it = sub->towers.erase(it);
        } else {
          ++it;
        }
      }

      // If a stream was closed on this subscription, re-send the subscription
      // so that it can be processed by other control towers.
      if (affected) {
        ResendSubscriptions(uuid_topic.second.log_id, uuid, sub.get());
      }
    }
  }
}

void CopilotWorker::ResendSubscriptions(LogID log_id,
                                        const TopicUUID& uuid,
                                        Subscription* sub) {
  LOG_INFO(options_.info_log,
    "Re-establishing subscription for %llu on %s@%" PRIu64,
    sub->stream_id,
    uuid.ToString().c_str(),
    sub->seqno);

  Slice namespace_id;
  Slice topic_name;
  uuid.GetTopicID(&namespace_id, &topic_name);

  ProcessSubscribe(sub->tenant_id,
                   namespace_id.ToString(),
                   topic_name.ToString(),
                   sub->seqno,
                   sub->sub_id,
                   log_id,
                   sub->worker_id,
                   sub->stream_id);
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
        copilot_->GetStats(myid_)->numwrites_rollcall_failed->Add(1);
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
        // We can't do any proper error handling from this thread, as it belongs
        // to the client used by RollCall.
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
  copilot_->GetStats(myid_)->numwrites_rollcall_total->Add(1);
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
  }
  return &it->second;
}

}  // namespace rocketspeed
