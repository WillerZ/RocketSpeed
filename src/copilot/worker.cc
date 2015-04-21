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
#include "src/util/hostmap.h"

namespace rocketspeed {

CopilotWorker::CopilotWorker(const CopilotOptions& options,
                             const ControlTowerRouter* control_tower_router,
                             const int myid,
                             Copilot* copilot)
: worker_loop_(options.worker_queue_size)
, options_(options)
, control_tower_router_(control_tower_router)
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

void CopilotWorker::CommandCallback(CopilotWorkerCommand command) {
  // Process CopilotWorkerCommand
  std::unique_ptr<Message> message = command.GetMessage();
  Message* msg = message.get();
  assert(msg);

  switch (msg->GetMessageType()) {
  case MessageType::mMetadata: {
      MessageMetadata* metadata = static_cast<MessageMetadata*>(msg);
      const std::vector<TopicPair>& topics = metadata->GetTopicInfo();
      assert(topics.size() == 1);  // Workers only handle 1 topic at a time.
      const TopicPair& request = topics[0];

      if (metadata->GetMetaType() == MessageMetadata::MetaType::Request) {
        if (request.topic_type == MetadataType::mSubscribe) {
          // Subscribe
          ProcessSubscribe(std::move(message),
                           request,
                           command.GetLogID(),
                           command.GetWorkerId(),
                           command.GetOrigin());
        } else {
          // Unsubscribe
          ProcessUnsubscribe(std::move(message),
                             request,
                             command.GetLogID(),
                             command.GetWorkerId(),
                             command.GetOrigin());
        }
      } else {
        if (request.topic_type == MetadataType::mSubscribe) {
          // Response from Control Tower.
          ProcessMetadataResponse(std::move(message),
                                  request,
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

void CopilotWorker::ProcessMetadataResponse(std::unique_ptr<Message> message,
                                            const TopicPair& request,
                                            LogID logid,
                                            int worker_id) {
  MessageMetadata* msg = static_cast<MessageMetadata*>(message.get());

  LOG_INFO(options_.info_log,
      "Received %ssubscribe response for Topic(%s)",
      request.topic_type == MetadataType::mSubscribe ? "" : "un",
      request.topic_name.c_str());

  // Get the list of subscriptions for this topic.
  TopicUUID uuid(request.namespace_id, request.topic_name);
  auto it = subscriptions_.find(uuid);
  if (it != subscriptions_.end()) {
    for (auto& subscription : it->second) {
      // If the subscription is awaiting a response.
      if (subscription.awaiting_ack) {
        // Send to client's worker.
        Status status = options_.msg_loop->SendResponse(*msg,
                                                        subscription.stream_id,
                                                        subscription.worker_id);
        if (status.ok()) {
          subscription.awaiting_ack = false;

          // When the subscription seqno is 0, it was waiting on the correct
          // seqno for *now*. The response has provided the correct seqno.
          if (subscription.seqno == 0) {
            subscription.seqno = request.seqno;
          }
          LOG_INFO(options_.info_log,
            "Sending %ssubscribe response for Topic(%s) to %llu on worker %d",
            request.topic_type == MetadataType::mSubscribe ? "" : "un",
            request.topic_name.c_str(),
            subscription.stream_id,
            subscription.worker_id);
          // Update rollcall topic.
          RollcallWrite(std::move(message), msg->GetTenantID(),
                        request.topic_name,
                        request.namespace_id, request.topic_type,
                        logid, worker_id, subscription.stream_id);
        } else {
          LOG_WARN(options_.info_log,
            "Failed to send metadata response to %llu",
            subscription.stream_id);
          // We were unable to forward the subscribe-response to the client
          // so the client is unaware that it has a confirmed subscription.
          // There is no need to write the rollcall topic.
        }
      }
    }
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
  auto it = subscriptions_.find(uuid);
  if (it != subscriptions_.end()) {
    auto seqno = msg->GetSequenceNumber();
    auto prev_seqno = msg->GetPrevSequenceNumber();
    std::vector<std::pair<ClientID, int>> destinations;

    // Send to all subscribers.
    for (auto& subscription : it->second) {
      StreamID recipient = subscription.stream_id;

      // If the subscription is awaiting a response, do not forward.
      if (subscription.awaiting_ack) {
        LOG_INFO(options_.info_log,
                 "Data not delivered to %llu (awaiting ack)",
                 recipient);
        continue;
      }

      // Also do not send a response if the seqno is too low.
      if (subscription.seqno > seqno) {
        LOG_INFO(options_.info_log,
                 "Data not delivered to %llu"
                 " (seqno@%" PRIu64 " too low, currently @%" PRIu64 ")",
                 recipient,
                 seqno,
                 subscription.seqno);
        continue;
      }

      // or too high.
      if (subscription.seqno < prev_seqno) {
        LOG_INFO(options_.info_log,
                 "Data not delivered to %llu"
                 " (prev_seqno@%" PRIu64 " too high, currently @%" PRIu64 ")",
                 recipient,
                 prev_seqno,
                 subscription.seqno);
        continue;
      }

      // Send to worker loop.
      Status status = options_.msg_loop->SendResponse(*msg,
                                                      recipient,
                                                      subscription.worker_id);
      if (status.ok()) {
        subscription.seqno = seqno + 1;

        LOG_INFO(options_.info_log,
                 "Sent data (%.16s)@%" PRIu64 " for Topic(%s) to %llu",
                 msg->GetPayload().ToString().c_str(),
                 msg->GetSequenceNumber(),
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
      msg->GetTopicName().ToString().c_str());
  TopicUUID uuid(msg->GetNamespaceId(), msg->GetTopicName());
  auto it = subscriptions_.find(uuid);
  if (it != subscriptions_.end()) {
    auto prev_seqno = msg->GetStartSequenceNumber();
    auto next_seqno = msg->GetEndSequenceNumber();

    // Send to all subscribers.
    for (auto& subscription : it->second) {
      StreamID recipient = subscription.stream_id;

      // If the subscription is awaiting a response, do not forward.
      if (subscription.awaiting_ack) {
        LOG_INFO(options_.info_log,
                 "Gap ignored for %llu (awaiting ack)",
                 recipient);
        continue;
      }

      // Also ignore if the seqno is too low.
      if (subscription.seqno > next_seqno) {
        LOG_INFO(options_.info_log,
                 "Gap ignored for %llu"
                 " (next_seqno@%" PRIu64 " too low, currently @%" PRIu64 ")",
                 recipient,
                 next_seqno,
                 subscription.seqno);
        continue;
      }

      // or too high.
      if (subscription.seqno < prev_seqno) {
        LOG_INFO(options_.info_log,
                 "Gap ignored for %llu"
                 " (prev_seqno@%" PRIu64 " too high, currently @%" PRIu64 ")",
                 recipient,
                 prev_seqno,
                 subscription.seqno);
        continue;
      }

      // Send to worker loop.
      Status status = options_.msg_loop->SendResponse(
          *msg, recipient, subscription.worker_id);
      if (status.ok()) {
        subscription.seqno = next_seqno + 1;

        LOG_INFO(options_.info_log,
                 "Sent gap %" PRIu64 "-%" PRIu64 " for Topic(%s) to %llu",
                 msg->GetStartSequenceNumber(),
                 msg->GetEndSequenceNumber(),
                 msg->GetTopicName().ToString().c_str(),
                 recipient);
      } else {
        LOG_WARN(
            options_.info_log, "Failed to distribute gap to %llu", recipient);
      }
    }
  }
}

void CopilotWorker::ProcessSubscribe(std::unique_ptr<Message> message,
                                     const TopicPair& request,
                                     LogID logid,
                                     int worker_id,
                                     StreamID subscriber) {
  LOG_INFO(options_.info_log,
      "Received subscribe request for Topic(%s)@%" PRIu64 " for %llu",
      request.topic_name.c_str(),
      request.seqno,
      subscriber);

  bool notify_origin = false;
  bool notify_control_tower = false;
  MessageMetadata* msg = static_cast<MessageMetadata*>(message.get());

  // Insert into client-topic map. Doesn't matter if it was already there.
  TopicInfo topic_info { request.topic_name, request.namespace_id, logid };
  client_topics_[subscriber].insert(topic_info);

  TopicUUID uuid(request.namespace_id, request.topic_name);
  auto topic_iter = subscriptions_.find(uuid);
  if (topic_iter == subscriptions_.end()) {
    // No subscribers on this topic, create new topic entry.
    std::vector<Subscription> subscribers{ Subscription(subscriber,
                                                        request.seqno,
                                                        true,
                                                        worker_id) };
    subscriptions_.emplace(uuid, subscribers);
    notify_control_tower = true;
  } else {
    // Already have subscriptions on this topic.
    // First check if we already have a subscription for this subscriber.
    bool found = false;
    SequenceNumber earliest_seqno = request.seqno + 1;
    for (auto& subscription : topic_iter->second) {
      if (subscription.stream_id == subscriber) {
        assert(!found);  // should never have a duplicate subscription
        // Already a subscriber. Do we need to update seqno?
        if (request.seqno == 0 || subscription.seqno == 0) {
          // Requesting with seqno == 0 means we want to subscribe from *now*.
          // Always need to notify the control_tower because the control tower
          // will need to tell us the correct seqno, which will be updated
          // locally in the metadata response.
          subscription.seqno = request.seqno;
          subscription.awaiting_ack = true;
          notify_control_tower = true;
        } else if (subscription.seqno == request.seqno) {
          // Already subscribed at the correct seqno, just ack again.
          subscription.awaiting_ack = false;
          notify_origin = true;
        } else if (subscription.seqno > request.seqno) {
          // Existing subscription is ahead, rewind control tower subscription
          // TODO(pja) 1 : may need new subscriber ID here
          subscription.seqno = request.seqno;
          subscription.awaiting_ack = true;
          notify_control_tower = true;
        } else {
          // Existing subscription is behind, just let it catch up.
          // TODO(pja) 1 : may need new subscriber ID here
          subscription.seqno = request.seqno;
          subscription.awaiting_ack = false;
          notify_origin = true;
        }
        // Update the worker_id for this subscription, in case it changed.
        subscription.worker_id = worker_id;
        found = true;
      }
      // Find earliest seqno subscription for this topic.
      if (subscription.seqno != 0) {
        earliest_seqno = std::min(earliest_seqno, subscription.seqno);
      }
    }

    if (!found) {
      if (earliest_seqno <= request.seqno) {
        // Already subscribed to a point before the request, we can just ack
        // the client request and let the tailer catch up.
        // TODO(pja) 1 : may take a long time to catch up, so may need to
        // notify the CT.
        topic_iter->second.emplace_back(subscriber,
                                        request.seqno,
                                        false,
                                        worker_id);
        notify_origin = true;
      } else {
        // Need to add new subscription and rewind existing subscription.
        topic_iter->second.emplace_back(subscriber,
                                        request.seqno,
                                        true,
                                        worker_id);
        notify_control_tower = true;
      }
    }
  }
  // check that we need to notify only one, not both
  assert(!notify_control_tower || !notify_origin);

  if (notify_control_tower) {
    // Find control tower responsible for this topic's log.
    HostId const* recipient = nullptr;
    if (control_tower_router_->GetControlTower(logid, &recipient).ok()) {
      int outgoing_worker_id = copilot_->GetLogWorker(logid);

      // Find or open a new stream socket to this control tower.
      auto socket = GetControlTowerSocket(
          *recipient, options_.msg_loop, outgoing_worker_id);

      // Forward request to control tower to update the copilot subscription.
      Status status = options_.msg_loop->SendRequest(*msg,
                                                     socket,
                                                     outgoing_worker_id);
      if (!status.ok()) {
        LOG_INFO(options_.info_log,
          "Failed to send metadata response to %s",
          recipient->ToString().c_str());
      } else {
        LOG_INFO(options_.info_log,
          "Sent subscription for Topic(%s)@%" PRIu64 " to %s",
          request.topic_name.c_str(), request.seqno,
          recipient->ToString().c_str());
      }
    } else {
      // This should only ever happen if all control towers are offline.
      LOG_WARN(options_.info_log,
        "Failed to find control tower for log ID %" PRIu64,
        static_cast<uint64_t>(logid));
    }
  }

  if (notify_origin) {
    // Send response to origin to notify that subscription has been processed.
    msg->SetMetaType(MessageMetadata::MetaType::Response);
    Status st = options_.msg_loop->SendResponse(*msg, subscriber, worker_id);
    if (!st.ok()) {
      // Failed to send response. The origin will re-send the subscription
      // again in the future, and we'll try to immediately respond again.
      LOG_INFO(options_.info_log,
               "Failed to send subscribe response to %llu",
               subscriber);
    }
    // Update rollcall topic.
    RollcallWrite(std::move(message), msg->GetTenantID(),
                  request.topic_name, request.namespace_id,
                  request.topic_type, logid, worker_id, subscriber);
  }
}

void CopilotWorker::ProcessUnsubscribe(std::unique_ptr<Message> message,
                                       const TopicPair& request,
                                       LogID logid,
                                       int worker_id,
                                       StreamID subscriber) {
  LOG_INFO(options_.info_log,
           "Received unsubscribe request for Topic(%s) for %llu",
           request.topic_name.c_str(),
           subscriber);

  MessageMetadata* msg = static_cast<MessageMetadata*>(message.get());

  RemoveSubscription(msg->GetTenantID(),
                     subscriber,
                     request.namespace_id,
                     request.topic_name,
                     logid,
                     worker_id);

  // Send response to origin to notify that subscription has been processed.
  msg->SetMetaType(MessageMetadata::MetaType::Response);
  Status status = options_.msg_loop->SendResponse(*msg, subscriber, worker_id);
  if (!status.ok()) {
    // Failed to send response. The origin will re-send the subscription
    // again in the future, and we'll try to immediately respond again.
    LOG_WARN(options_.info_log,
        "Failed to send unsubscribe response on Topic(%s) to %llu",
        request.topic_name.c_str(),
        subscriber);
  } else {
    LOG_INFO(options_.info_log,
             "Send unsubscribe response on Topic(%s) to %llu",
             request.topic_name.c_str(),
             subscriber);
  }
}

void CopilotWorker::RemoveSubscription(TenantID tenant_id,
                                       const StreamID subscriber,
                                       const NamespaceID& namespace_id,
                                       const Topic& topic_name,
                                       LogID logid,
                                       int worker_id) {
  // Remove from client-topic map. Doesn't matter if it was already there.
  TopicInfo topic_info { topic_name, namespace_id, logid };
  client_topics_[subscriber].erase(topic_info);

  TopicUUID uuid(namespace_id, topic_name);
  auto topic_iter = subscriptions_.find(uuid);
  if (topic_iter != subscriptions_.end()) {
    // Find our subscription and remove it.
    SequenceNumber earliest_other_seqno = 0;
    SequenceNumber our_seqno = 0;
    auto& subscriptions = topic_iter->second;
    for (auto it = subscriptions.begin(); it != subscriptions.end(); ) {
      if (it->stream_id == subscriber) {
        // This is our subscription, remove it.
        our_seqno = it->seqno;
        it = subscriptions.erase(it);
      } else {
        // Find earliest other seqno subscription for this topic.
        earliest_other_seqno = std::min(earliest_other_seqno, it->seqno);
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
        int outgoing_worker_id = copilot_->GetLogWorker(logid);
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
    } else if (our_seqno < earliest_other_seqno) {
      // Need to update control tower.
      HostId const* recipient = nullptr;
      if (control_tower_router_->GetControlTower(logid, &recipient).ok()) {
        // Re subscribe our control tower subscription with the later seqno.
        int outgoing_worker_id = copilot_->GetLogWorker(logid);
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
    RollcallWrite(nullptr, tenant_id, topic_name,
                  namespace_id, MetadataType::mUnSubscribe,
                  logid, worker_id, subscriber);
  }
}

void CopilotWorker::ProcessGoodbye(std::unique_ptr<Message> message,
                                   StreamID origin) {
  MessageGoodbye* goodbye = static_cast<MessageGoodbye*>(message.get());

  LOG_INFO(options_.info_log,
           "Copilot received goodbye for client %llu",
           origin);

  if (goodbye->GetOriginType() == MessageGoodbye::OriginType::Client) {
    // This is a goodbye from one of the clients.
    auto it = client_topics_.find(origin);
    if (it != client_topics_.end()) {
      // Unsubscribe from all topics.
      // Making a copy because RemoveSubscription will modify client_topics_;
      auto topics_copy = it->second;
      for (const TopicInfo& info : topics_copy) {
        RemoveSubscription(goodbye->GetTenantID(),
                           origin,
                           info.namespace_id,
                           info.topic_name,
                           info.logid,
                           0);  // The worked id is a dummy because we do not
                                //  need to send any response back to the client
      }
      client_topics_.erase(it);
    }
  } else {
    // TODO(stupaq) resubscribe all lost subscriptions
  }
}

//
// Inserts an entry into the rollcall topic.
//
void
CopilotWorker::RollcallWrite(std::unique_ptr<Message> msg,
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

  assert(msg || type == MetadataType::mUnSubscribe);

  // Write to rollcall topic failed. If this was a 'subscription' event,
  // then send unsubscribe message to copilot worker. This will send an
  // unsubscribe response to appropriate client.
  //
  // If the write fails, process_error will be called asynchronously, so it
  // cannot reference local variables.
  std::function<void()> process_error;
  if (type == MetadataType::mSubscribe) {
    TenantID tenant = msg->GetTenantID();
    process_error =
      [this, topic_name, namespace_id, logid, worker_id, origin, tenant] () {
        copilot_->GetStats(myid_)->numwrites_rollcall_failed->Add(1);
        std::vector<TopicPair> topics = { TopicPair(0, topic_name,
                                                  MetadataType::mUnSubscribe,
                                                  namespace_id) };
        std::unique_ptr<Message> newmsg(new MessageMetadata(
                                          tenant,
                                          MessageMetadata::MetaType::Request,
                                          topics));
        // Start the automatic unsubscribe process. We rely on the assumption
        // that the unsubscribe request can fail only if the client is
        // un-communicable, in which case the client's subscritions are reaped.
        Forward(logid, std::move(newmsg), worker_id, origin);
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
