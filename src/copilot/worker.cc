// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/copilot/worker.h"
#include <vector>
#include "include/Status.h"
#include "include/Types.h"
#include "src/messages/commands.h"

namespace rocketspeed {

CopilotWorker::CopilotWorker(const CopilotOptions& options,
                             const ControlTowerRouter* control_tower_router,
                             MsgClient* msg_client)
: worker_loop_(options.worker_queue_size)
, options_(options)
, control_tower_router_(control_tower_router)
, msg_client_(msg_client) {
  // msg_client is required.
  assert(msg_client_);

  Log(InfoLogLevel::INFO_LEVEL, options_.info_log,
    "Created a new CopilotWorker");
  options_.info_log->Flush();
}

bool CopilotWorker::Forward(LogID logid, std::unique_ptr<Message> msg) {
  return worker_loop_.Send(logid, std::move(msg));
}

void CopilotWorker::CommandCallback(CopilotWorkerCommand command) {
  // Process CopilotWorkerCommand
  Message* msg = command.GetMessage();
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
          ProcessSubscribe(metadata, request, command.GetLogID());
        } else {
          // Unsubscribe
          ProcessUnsubscribe(metadata, request, command.GetLogID());
        }
      } else {
        if (request.topic_type == MetadataType::mSubscribe) {
          // Response from Control Tower.
          ProcessMetadataResponse(metadata, request);
        }
      }
    }
    break;

  case MessageType::mData: {
      // Data to forward to client.
      MessageData* data = static_cast<MessageData*>(msg);
      ProcessData(data);
    }
    break;

  default: {
      Log(InfoLogLevel::WARN_LEVEL, options_.info_log,
          "Unexpected message type in copilot worker %d",
          msg->GetMessageType());
    }
    break;
  }
}

void CopilotWorker::ProcessMetadataResponse(MessageMetadata* msg,
                                            const TopicPair& request) {
  // Get the list of subscriptions for this topic.
  auto it = subscriptions_.find(request.topic_name);
  if (it != subscriptions_.end()) {
    // Serialize message once instead of in the loop.
    Slice msg_serial = msg->Serialize();
    for (auto& subscription : it->second) {
      // If the subscription is awaiting a response.
      if (subscription.awaiting_ack) {
        // Send the response.
        const HostId& recipient = subscription.host_id;
        Status status = msg_client_->Send(recipient, msg_serial);
        if (!status.ok()) {
          Log(InfoLogLevel::INFO_LEVEL, options_.info_log,
            "Failed to send metadata response to %s:%d",
            recipient.hostname.c_str(), recipient.port);
        } else {
          // Successful, don't send any more acks.
          // (if the response didn't actually make it then the client
          // will resubscribe anyway).
          subscription.awaiting_ack = false;
        }
      }
    }
  }
}

void CopilotWorker::ProcessData(MessageData* msg) {
  // Get the list of subscriptions for this topic.
  Log(InfoLogLevel::INFO_LEVEL, options_.info_log,
      "Copilot received data (%.16s) for topic %s",
      msg->GetPayload().ToString().c_str(),
      msg->GetTopicName().ToString().c_str());
  auto it = subscriptions_.find(msg->GetTopicName().ToString());
  if (it != subscriptions_.end()) {
    // Serialize message once instead of in the loop.
    Slice msg_serial = msg->Serialize();
    auto seqno = msg->GetSequenceNumber();
    for (auto& subscription : it->second) {
      const HostId& recipient = subscription.host_id;

      // If the subscription is awaiting a response, do not forward.
      if (subscription.awaiting_ack) {
        Log(InfoLogLevel::INFO_LEVEL, options_.info_log,
          "Data not delivered to %s:%d (awaiting ack)",
          recipient.hostname.c_str(), recipient.port);
        continue;
      }

      // Also do not send a response if the seqno is too low.
      if (subscription.seqno >= seqno) {
        Log(InfoLogLevel::INFO_LEVEL, options_.info_log,
          "Data not delivered to %s:%d (seqno too low)",
          recipient.hostname.c_str(), recipient.port);
        continue;
      }

      // Send the response.
      Status status = msg_client_->Send(recipient, msg_serial);
      if (status.ok()) {
        Log(InfoLogLevel::INFO_LEVEL, options_.info_log,
          "Sent data (%.16ss) for topic %s to %s:%d",
          msg->GetPayload().ToString().c_str(),
          msg->GetTopicName().ToString().c_str(),
          recipient.hostname.c_str(),
          recipient.port);
        subscription.seqno = seqno;
      } else {
        // Message failed to send. Possible reasons:
        // 1. Connection closed at other end.
        // 2. Outgoing socket buffer is full.
        // 3. Some other low-level error.
        // TODO(pja) 1 : handle these gracefully.
        Log(InfoLogLevel::INFO_LEVEL, options_.info_log,
          "Failed to forward data to %s:%d",
          recipient.hostname.c_str(), recipient.port);
      }
    }
  }
}

void CopilotWorker::ProcessSubscribe(MessageMetadata* msg,
                                     const TopicPair& request,
                                     LogID logid) {
  bool notify_origin = false;
  bool notify_control_tower = false;

  const HostId& subscriber = msg->GetOrigin();
  auto topic_iter = subscriptions_.find(request.topic_name);
  if (topic_iter == subscriptions_.end()) {
    // No subscribers on this topic, create new topic entry.
    std::vector<Subscription> subscribers{ Subscription(subscriber,
                                                        request.seqno,
                                                        true) };
    subscriptions_.emplace(request.topic_name, subscribers);
    notify_control_tower = true;
  } else {
    // Already have subscriptions on this topic.
    // First check if we already have a subscription for this subscriber.
    bool found = false;
    SequenceNumber earliest_seqno = request.seqno + 1;
    for (auto& subscription : topic_iter->second) {
      if (subscription.host_id == subscriber) {
        assert(!found);  // should never have a duplicate subscription
        // Already a subscriber. Do we need to update seqno?
        if (subscription.seqno == request.seqno) {
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
        found = true;
      }
      // Find earliest seqno subscription for this topic.
      earliest_seqno = std::min(earliest_seqno, subscription.seqno);
    }

    if (!found) {
      if (earliest_seqno <= request.seqno) {
        // Already subscribed to a point before the request, we can just ack
        // the client request and let the tailer catch up.
        // TODO(pja) 1 : may take a long time to catch up, so may need to
        // notify the CT.
        topic_iter->second.emplace_back(subscriber, request.seqno, false);
        notify_origin = true;
      } else {
        // Need to add new subscription and rewind existing subscription.
        topic_iter->second.emplace_back(subscriber, request.seqno, true);
        notify_control_tower = true;
      }
    }
  }

  if (notify_control_tower) {
    // Find control tower responsible for this topic's log.
    HostId const* recipient = nullptr;
    if (control_tower_router_->GetControlTower(logid, &recipient).ok()) {
      // Forward request to control tower to update the copilot subscription.
      msg->SetOrigin(GetHostId());
      if (!msg_client_->Send(*recipient, msg).ok()) {
        Log(InfoLogLevel::INFO_LEVEL, options_.info_log,
          "Failed to send metadata response to %s:%d",
          recipient->hostname.c_str(), recipient->port);
      }
    } else {
      // This should only ever happen if all control towers are offline.
      Log(InfoLogLevel::WARN_LEVEL, options_.info_log,
        "Failed to find control tower for log ID %llu",
        static_cast<uint64_t>(logid));
    }
  }

  if (notify_origin) {
    // Send response to origin to notify that subscription has been processed.
    msg->SetMetaType(MessageMetadata::MetaType::Response);
    if (!msg_client_->Send(subscriber, msg).ok()) {
      // Failed to send response. The origin will re-send the subscription
      // again in the future, and we'll try to immediately respond again.
      Log(InfoLogLevel::INFO_LEVEL, options_.info_log,
          "Failed to send subscribe response to %s:%d",
          subscriber.hostname.c_str(), subscriber.port);
    }
  }
}

void CopilotWorker::ProcessUnsubscribe(MessageMetadata* msg,
                                       const TopicPair& request,
                                       LogID logid) {
  const HostId& subscriber = msg->GetOrigin();
  auto topic_iter = subscriptions_.find(request.topic_name);
  if (topic_iter != subscriptions_.end()) {
    // Find our subscription and remove it.
    SequenceNumber earliest_other_seqno = 0;
    SequenceNumber our_seqno = 0;
    auto& subscriptions = topic_iter->second;
    for (auto it = subscriptions.begin(); it != subscriptions.end(); ) {
      if (it->host_id == subscriber) {
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
        MessageMetadata ct_msg(msg->GetTenantID(),
                               MessageMetadata::MetaType::Request,
                               GetHostId(),
                               { TopicPair(request.seqno,
                                           request.topic_name,
                                           MetadataType::mUnSubscribe,
                                           request.namespace_id) });
        Status status = msg_client_->Send(*recipient, &ct_msg);
        if (!status.ok()) {
          Log(InfoLogLevel::INFO_LEVEL, options_.info_log,
              "Failed to send unsubscribe request to %s:%d",
              recipient->hostname.c_str(), recipient->port);
        }
      }
    } else if (our_seqno < earliest_other_seqno) {
      // Need to update control tower.
      HostId const* recipient = nullptr;
      if (control_tower_router_->GetControlTower(logid, &recipient).ok()) {
        // Re subscribe our control tower subscription with the later seqno.
        MessageMetadata ct_msg(msg->GetTenantID(),
                               MessageMetadata::MetaType::Request,
                               GetHostId(),
                               { TopicPair(earliest_other_seqno,
                                           request.topic_name,
                                           MetadataType::mSubscribe,
                                           request.namespace_id) });
        Status status = msg_client_->Send(*recipient, &ct_msg);
        if (!status.ok()) {
          Log(InfoLogLevel::INFO_LEVEL, options_.info_log,
              "Failed to send unsubscribe request to %s:%d",
              recipient->hostname.c_str(), recipient->port);
        }
      } else {
        // This should only ever happen if all control towers are offline.
        Log(InfoLogLevel::WARN_LEVEL, options_.info_log,
          "Failed to find control tower for log ID %llu",
          static_cast<uint64_t>(logid));
      }
    }
  }

  // Send response to origin to notify that subscription has been processed.
  msg->SetMetaType(MessageMetadata::MetaType::Response);
  if (!msg_client_->Send(subscriber, msg).ok()) {
    // Failed to send response. The origin will re-send the subscription
    // again in the future, and we'll try to immediately respond again.
    Log(InfoLogLevel::INFO_LEVEL, options_.info_log,
        "Failed to send subscribe response to %s:%d",
        subscriber.hostname.c_str(), subscriber.port);
  }
}

}  // namespace rocketspeed
