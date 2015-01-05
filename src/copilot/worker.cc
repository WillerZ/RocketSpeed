// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/copilot/worker.h"
#include <vector>
#include "include/Status.h"
#include "include/Types.h"
#include "src/copilot/copilot.h"
#include "src/util/hostmap.h"

namespace rocketspeed {

CopilotWorker::CopilotWorker(const CopilotOptions& options,
                             const ControlTowerRouter* control_tower_router,
                             Copilot* copilot)
: worker_loop_(options.env,
               options.worker_queue_size)
, options_(options)
, control_tower_router_(control_tower_router)
, copilot_(copilot) {
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
                            int worker_id) {
  return worker_loop_.Send(logid, std::move(msg), worker_id);
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
                           command.GetWorkerId());
        } else {
          // Unsubscribe
          ProcessUnsubscribe(std::move(message),
                             request,
                             command.GetLogID(),
                             command.GetWorkerId());
        }
      } else {
        if (request.topic_type == MetadataType::mSubscribe) {
          // Response from Control Tower.
          ProcessMetadataResponse(std::move(message), request);
        }
      }
    }
    break;

  case MessageType::mDeliver: {
      // Data to forward to client.
      ProcessDeliver(std::move(message));
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
                                            const TopicPair& request) {
  MessageMetadata* msg = static_cast<MessageMetadata*>(message.get());

  LOG_INFO(options_.info_log,
      "Received %ssubscribe response for Topic(%s)",
      request.topic_type == MetadataType::mSubscribe ? "" : "un",
      request.topic_name.c_str());

  // Get the list of subscriptions for this topic.
  auto it = subscriptions_.find(request.topic_name);
  if (it != subscriptions_.end()) {
    std::vector<std::pair<ClientID, int>> destinations;

    // serialize message
    std::string serial;
    msg->SerializeToString(&serial);

    for (auto& subscription : it->second) {
      // If the subscription is awaiting a response.
      if (subscription.awaiting_ack) {
        destinations.emplace_back(subscription.client_id,
                                  subscription.worker_id);
        LOG_INFO(options_.info_log,
          "Sending %ssubscribe response for Topic(%s) to %s on worker %d",
          request.topic_type == MetadataType::mSubscribe ? "" : "un",
          request.topic_name.c_str(),
          subscription.client_id.c_str(),
          subscription.worker_id);
      }
    }

    if (destinations.size() > 0) {
      DistributeCommand(std::move(serial), std::move(destinations));

      // Successful, don't send any more acks.
      // (if the response didn't actually make it then the client
      // will resubscribe anyway).
      for (auto& subscription : it->second) {
        if (subscription.awaiting_ack) {
          subscription.awaiting_ack = false;

          // When the subscription seqno is 0, it was waiting on the correct
          // seqno for *now*. The response has provided the correct seqno.
          if (subscription.seqno == 0) {
            subscription.seqno = request.seqno;
          }
        }
      }
    }
  }
}

void CopilotWorker::ProcessDeliver(std::unique_ptr<Message> message) {
  MessageData* msg = static_cast<MessageData*>(message.get());
  // Get the list of subscriptions for this topic.
  LOG_INFO(options_.info_log,
      "Copilot received data (%.16s)@%lu for Topic(%s)",
      msg->GetPayload().ToString().c_str(),
      msg->GetSequenceNumber(),
      msg->GetTopicName().ToString().c_str());
  auto it = subscriptions_.find(msg->GetTopicName().ToString());
  if (it != subscriptions_.end()) {
    auto seqno = msg->GetSequenceNumber();
    std::vector<std::pair<ClientID, int>> destinations;

    // serialize message
    std::string serial;
    msg->SerializeToString(&serial);

    // accumulate all possible recipients
    for (auto& subscription : it->second) {
      const ClientID& recipient = subscription.client_id;

      // If the subscription is awaiting a response, do not forward.
      if (subscription.awaiting_ack) {
        LOG_INFO(options_.info_log,
          "Data not delivered to %s (awaiting ack)",
          recipient.c_str());
        continue;
      }

      // Also do not send a response if the seqno is too low.
      if (subscription.seqno > seqno) {
        LOG_INFO(options_.info_log,
          "Data not delivered to %s (seqno too low, currently @%lu)",
          recipient.c_str(), subscription.seqno);
        continue;
      }
      destinations.emplace_back(recipient, subscription.worker_id);
    }

    DistributeCommand(std::move(serial), std::move(destinations));

    int count = 0;
    for (auto& subscription : it->second) {
      LOG_INFO(options_.info_log,
               "Sent data (%.16s)@%lu for Topic(%s) to %s",
               msg->GetPayload().ToString().c_str(),
               msg->GetSequenceNumber(),
               msg->GetTopicName().ToString().c_str(),
               subscription.client_id.c_str());
      subscription.seqno = seqno + 1;
      count++;
    }
  }
}

void CopilotWorker::ProcessSubscribe(std::unique_ptr<Message> message,
                                     const TopicPair& request,
                                     LogID logid,
                                     int worker_id) {
  LOG_INFO(options_.info_log,
      "Received subscribe request for Topic(%s)@%lu for %s",
      request.topic_name.c_str(),
      request.seqno,
      message->GetOrigin().c_str());

  bool notify_origin = false;
  bool notify_control_tower = false;
  MessageMetadata* msg = static_cast<MessageMetadata*>(message.get());

  // Intentionally making copy since msg->SetOrigin may be called later.
  const ClientID subscriber = msg->GetOrigin();

  auto topic_iter = subscriptions_.find(request.topic_name);
  if (topic_iter == subscriptions_.end()) {
    // No subscribers on this topic, create new topic entry.
    std::vector<Subscription> subscribers{ Subscription(subscriber,
                                                        request.seqno,
                                                        true,
                                                        worker_id) };
    subscriptions_.emplace(request.topic_name, subscribers);
    notify_control_tower = true;
  } else {
    // Already have subscriptions on this topic.
    // First check if we already have a subscription for this subscriber.
    bool found = false;
    SequenceNumber earliest_seqno = request.seqno + 1;
    for (auto& subscription : topic_iter->second) {
      if (subscription.client_id == subscriber) {
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
    ClientID const* recipient = nullptr;
    if (control_tower_router_->GetControlTower(logid, &recipient).ok()) {
      int outgoing_worker_id = copilot_->GetLogWorker(logid);
      msg->SetOrigin(copilot_->GetClientId(outgoing_worker_id));

      // serialize
      std::string serial;
      msg->SerializeToString(&serial);

      // Forward request to control tower to update the copilot subscription.
      std::unique_ptr<Command> cmd(
        new SerializedSendCommand(std::move(serial),
                                  *recipient,
                                  options_.env->NowMicros()));
      Status status = copilot_->SendCommand(std::move(cmd),
                                            outgoing_worker_id);
      if (!status.ok()) {
        LOG_INFO(options_.info_log,
          "Failed to send metadata response to %s",
          recipient->c_str());
      } else {
        LOG_INFO(options_.info_log,
          "Sent subscription for Topic(%s)@%lu to %s",
          request.topic_name.c_str(), request.seqno, recipient->c_str());
      }
    } else {
      // This should only ever happen if all control towers are offline.
      LOG_WARN(options_.info_log,
        "Failed to find control tower for log ID %lu",
        static_cast<uint64_t>(logid));
    }
  }

  if (notify_origin) {
    // Send response to origin to notify that subscription has been processed.
    msg->SetMetaType(MessageMetadata::MetaType::Response);
    // serialize
    std::string serial;
    msg->SerializeToString(&serial);
    std::unique_ptr<Command> cmd(
      new SerializedSendCommand(std::move(serial),
                                subscriber,
                                options_.env->NowMicros()));
    Status status = copilot_->SendCommand(std::move(cmd), worker_id);
    if (!status.ok()) {
      // Failed to send response. The origin will re-send the subscription
      // again in the future, and we'll try to immediately respond again.
      LOG_INFO(options_.info_log,
          "Failed to send subscribe response to %s",
          subscriber.c_str());
    }
  }
}

void CopilotWorker::ProcessUnsubscribe(std::unique_ptr<Message> message,
                                       const TopicPair& request,
                                       LogID logid,
                                       int worker_id) {
  LOG_INFO(options_.info_log,
      "Received unsubscribe request for Topic(%s) for %s",
      request.topic_name.c_str(),
      message->GetOrigin().c_str());

  MessageMetadata* msg = static_cast<MessageMetadata*>(message.get());

  // Intentionally making copy since msg->SetOrigin may be called later.
  const ClientID subscriber = msg->GetOrigin();

  auto topic_iter = subscriptions_.find(request.topic_name);
  if (topic_iter != subscriptions_.end()) {
    // Find our subscription and remove it.
    SequenceNumber earliest_other_seqno = 0;
    SequenceNumber our_seqno = 0;
    auto& subscriptions = topic_iter->second;
    for (auto it = subscriptions.begin(); it != subscriptions.end(); ) {
      if (it->client_id == subscriber) {
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
      ClientID const* recipient = nullptr;
      if (control_tower_router_->GetControlTower(logid, &recipient).ok()) {
        // Forward unsubscribe request to control tower, with this copilot
        // worker as the subscriber.
        int outgoing_worker_id = copilot_->GetLogWorker(logid);
        MessageMetadata newmsg(msg->GetTenantID(),
                               MessageMetadata::MetaType::Request,
                               copilot_->GetClientId(outgoing_worker_id),
                               { TopicPair(request.seqno,
                                           request.topic_name,
                                           MetadataType::mUnSubscribe,
                                           request.namespace_id) });
        // serialize message
        std::string serial;
        newmsg.SerializeToString(&serial);
        std::unique_ptr<Command> cmd(
          new SerializedSendCommand(std::move(serial),
                                    *recipient,
                                    options_.env->NowMicros()));
        Status status = copilot_->SendCommand(std::move(cmd),
                                              outgoing_worker_id);
        if (!status.ok()) {
          LOG_INFO(options_.info_log,
              "Failed to send unsubscribe request to %s",
              recipient->c_str());
        }
      }
    } else if (our_seqno < earliest_other_seqno) {
      // Need to update control tower.
      ClientID const* recipient = nullptr;
      if (control_tower_router_->GetControlTower(logid, &recipient).ok()) {
        // Re subscribe our control tower subscription with the later seqno.
        int outgoing_worker_id = copilot_->GetLogWorker(logid);
        MessageMetadata newmsg(msg->GetTenantID(),
                               MessageMetadata::MetaType::Request,
                               copilot_->GetClientId(outgoing_worker_id),
                               { TopicPair(earliest_other_seqno,
                                           request.topic_name,
                                           MetadataType::mSubscribe,
                                           request.namespace_id) });
        // serialize message
        std::string serial;
        newmsg.SerializeToString(&serial);
        std::unique_ptr<Command> cmd(
          new SerializedSendCommand(std::move(serial),
                                    *recipient,
                                    options_.env->NowMicros()));
        Status status = copilot_->SendCommand(std::move(cmd),
                                              outgoing_worker_id);
        if (!status.ok()) {
          LOG_INFO(options_.info_log,
              "Failed to send unsubscribe request to %s",
              recipient->c_str());
        }
      } else {
        // This should only ever happen if all control towers are offline.
        LOG_WARN(options_.info_log,
          "Failed to find control tower for log ID %llu",
          static_cast<unsigned long long>(logid));
      }
    }
  }

  // Send response to origin to notify that subscription has been processed.
  msg->SetOrigin(copilot_->GetClientId(worker_id));
  msg->SetMetaType(MessageMetadata::MetaType::Response);
  std::string serial;
  msg->SerializeToString(&serial);
  std::unique_ptr<Command> cmd(
    new SerializedSendCommand(std::move(serial),
                              subscriber,
                              options_.env->NowMicros()));
  Status status = copilot_->SendCommand(std::move(cmd), worker_id);
  if (!status.ok()) {
    // Failed to send response. The origin will re-send the subscription
    // again in the future, and we'll try to immediately respond again.
    LOG_WARN(options_.info_log,
        "Failed to send unsubscribe response on Topic(%s) to %s",
        request.topic_name.c_str(),
        subscriber.c_str());
  } else {
    LOG_INFO(options_.info_log,
        "Send unsubscribe response on Topic(%s) to %s",
        request.topic_name.c_str(),
        subscriber.c_str());
  }
}

void CopilotWorker::DistributeCommand(
    std::string msg,
    std::vector<std::pair<ClientID, int>> destinations) {
  // Destinations per worker.
  std::unordered_map<int, SendCommand::Recipients> sub_destinations;

  // Find which worker the command needs to be sent to for each client.
  for (auto& elem : destinations) {
    sub_destinations[elem.second].push_back(std::move(elem.first));
  }

  // Send the command to each worker.
  for (auto it = sub_destinations.begin(); it != sub_destinations.end(); ++it) {
    int worker_id = it->first;
    SendCommand::Recipients& recipients = it->second;

    // Send the response.
    // TODO(pja) 1 : Find way to avoid msg copy.
    std::unique_ptr<Command> cmd(
      new SerializedSendCommand(msg,
                                std::move(recipients),
                                options_.env->NowMicros()));
    Status status = copilot_->SendCommand(std::move(cmd), worker_id);
    if (!status.ok()) {
      LOG_WARN(options_.info_log,
        "Failed to distribute message to %s",
        HostMap::ToString(recipients).c_str());
    }
  }
}


}  // namespace rocketspeed
