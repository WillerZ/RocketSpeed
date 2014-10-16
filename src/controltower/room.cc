//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/controltower/room.h"
#include <map>
#include <mutex>
#include <string>
#include <vector>
#include "src/util/coding.h"
#include "src/controltower/tower.h"

namespace rocketspeed {

ControlRoom::ControlRoom(const ControlTowerOptions& options,
                         ControlTower* control_tower,
                         unsigned int room_number,
                         int port_number) :
  control_tower_(control_tower),
  room_number_(room_number),
  room_id_(HostId(options.hostname, port_number)),
  topic_map_(control_tower->GetTailer()),
  room_loop_(options.worker_queue_size, WorkerLoopType::kMultiProducer) {
}

ControlRoom::~ControlRoom() {
}

// static method to start the loop for processing Room events
void ControlRoom::Run(void* arg) {
  ControlRoom* room = static_cast<ControlRoom*>(arg);
  LOG_INFO(room->control_tower_->GetOptions().info_log,
      "Starting ControlRoom Loop at port %ld", (long)room->room_id_.port);

  // Define a lambda to process Commands by the room_loop_.
  auto command_callback = [room] (RoomCommand command) {
    std::unique_ptr<Message> message = command.GetMessage();
    MessageType type = message->GetMessageType();
    if (type == MessageType::mData) {
      // data message from Tailer
      room->ProcessData(std::move(message), command.GetLogId());
    } else if (type == MessageType::mMetadata) {
      // subscription message from ControlTower
      room->ProcessMetadata(std::move(message), command.GetLogId());
    } else if (type == MessageType::mGap) {
      // gap message from Tailer
      room->ProcessGap(std::move(message), command.GetLogId());
    }
  };

  room->room_loop_.Run(command_callback);
}

// The Control Tower uses this method to forward a message to this Room.
Status
ControlRoom::Forward(std::unique_ptr<Message> msg, LogID logid) {
  if (room_loop_.Send(std::move(msg), logid)) {
    return Status::OK();
  } else {
    return Status::InternalError("Worker queue full");
  }
}

// Process Metadata messages that are coming in from ControlTower.
void
ControlRoom::ProcessMetadata(std::unique_ptr<Message> msg, LogID logid) {
  ControlTower* ct = control_tower_;
  Status st;

  // get the request message
  MessageMetadata* request = static_cast<MessageMetadata*>(msg.get());
  assert(request->GetMetaType() == MessageMetadata::MetaType::Request);
  if (request->GetMetaType() != MessageMetadata::MetaType::Request) {
    LOG_WARN(ct->GetOptions().info_log,
        "MessageMetadata with bad type %d received, ignoring...",
        request->GetMetaType());
    return;
  }

  // There should be only one topic for this message. The ControlTower
  // splits every topic into a distinct separate messages per ControlRoom.
  const std::vector<TopicPair>& topic = request->GetTopicInfo();
  assert(topic.size() == 1);
  const HostId& origin = request->GetOrigin();

  // Handle to 0 sequence number special case.
  // Zero means to start reading from the latest records, so we first need
  // to asynchronously consult the tailer for the latest seqno, and then
  // process the subscription.
  if (topic[0].seqno == 0) {
    // Create a callback to enqueue a subscribe command.
    // TODO(pja) 1: When this is passed to FindLatestSeqno, it will allocate
    // when converted to an std::function - could use an alloc pool for this.
    auto callback = [this, logid, request] (Status st, SequenceNumber seqno) {
      std::unique_ptr<Message> msg(request);
      if (!st.ok()) {
        LOG_WARN(control_tower_->GetOptions().info_log,
                 "Failed to find latest sequence number in log ID %lu",
                 logid);
        return;
      }

      request->GetTopicInfo()[0].seqno = seqno;  // update seqno
      st = Forward(std::move(msg), logid);
      if (!st.ok()) {
        // TODO(pja) 1: may need to do some flow control if this is due
        // to receiving too many subscriptions.
        LOG_WARN(control_tower_->GetOptions().info_log,
                 "Failed to enqueue subscription (%s)",
                 st.ToString().c_str());
      } else {
        const HostId& origin = request->GetOrigin();
        const std::vector<TopicPair>& topic = request->GetTopicInfo();
        LOG_INFO(control_tower_->GetOptions().info_log,
                 "Subscribing %s:%ld at latest seqno for Topic(%s)@%lu",
                 origin.hostname.c_str(),
                 (long)origin.port,
                 topic[0].topic_name.c_str(),
                 topic[0].seqno);
      }
    };

    // Ownership of message is in the callback.
    msg.release();
    st = ct->GetTailer()->FindLatestSeqno(logid, callback);
    if (!st.ok()) {
      // If call to FindLatestSeqno failed then callback will not be called,
      // so delete now.
      delete request;

      // TODO(pja) 1: may need to do some flow control if this is due
      // to receiving too many subscriptions.
      LOG_WARN(ct->GetOptions().info_log,
               "Failed to find latest seqno (%s)",
               st.ToString().c_str());
    } else {
      LOG_INFO(ct->GetOptions().info_log,
        "Sent FindLatestSeqno request for %s:%ld for Topic(%s)",
        origin.hostname.c_str(),
        (long)origin.port,
        topic[0].topic_name.c_str());
    }
    return;
  }

  // Map the origin to a HostNumber
  HostNumber hostnum = ct->GetHostMap().Lookup(origin);
  if (hostnum == -1) {
    hostnum = ct->GetHostMap().Insert(origin);
  }
  assert(hostnum >= 0);

  // Check that the topic name do map to the specified logid
  LogID checkid __attribute__((unused)) = 0;
  assert((ct->GetLogRouter().GetLogID(topic[0].topic_name, &checkid)).ok() &&
         (logid == checkid));

  // Prefix the namespace id to the topic name
  NamespaceTopic topic_name;
  PutNamespaceId(&topic_name, topic[0].namespace_id);
  topic_name.append(topic[0].topic_name);

  // Remember this subscription request
  if (topic[0].topic_type == MetadataType::mSubscribe) {
    topic_map_.AddSubscriber(topic_name,
                             topic[0].seqno,
                             logid, hostnum, room_number_);
    LOG_INFO(ct->GetOptions().info_log,
        "Added subscriber %s:%ld for Topic(%s)@%lu",
        origin.hostname.c_str(),
        (long)origin.port,
        topic[0].topic_name.c_str(),
        topic[0].seqno);
  } else if (topic[0].topic_type == MetadataType::mUnSubscribe) {
    topic_map_.RemoveSubscriber(topic_name,
                                logid, hostnum, room_number_);
    LOG_INFO(ct->GetOptions().info_log,
        "Removed subscriber %s:%ld from Topic(%s)",
        origin.hostname.c_str(),
        (long)origin.port,
        topic[0].topic_name.c_str());
  }

  // change it to a response ack message
  request->SetMetaType(MessageMetadata::MetaType::Response);

  // send reponse back to client
  st = ct->GetClient().Send(origin, std::move(msg));
  if (!st.ok()) {
    LOG_INFO(ct->GetOptions().info_log,
        "Unable to send Metadata response to %s:%ld",
        origin.hostname.c_str(), (long)origin.port);
  } else {
    LOG_INFO(ct->GetOptions().info_log,
        "Send Metadata response to %s:%ld",
        origin.hostname.c_str(), (long)origin.port);
  }
  ct->GetOptions().info_log->Flush();
}

// Process Data messages that are coming in from Tailer.
void
ControlRoom::ProcessData(std::unique_ptr<Message> msg, LogID logid) {
  ControlTower* ct = control_tower_;
  Status st;

  // get the request message
  MessageData* request = static_cast<MessageData*>(msg.get());

  LOG_INFO(ct->GetOptions().info_log,
      "Received data (%.16s)@%lu for Topic(%s)",
      request->GetPayload().ToString().c_str(),
      request->GetSequenceNumber(),
      request->GetTopicName().ToString().c_str());

  // Check that seqno is as expected.
  if (topic_map_.GetLastRead(logid) + 1 != request->GetSequenceNumber()) {
    // Out of order sequence number, skip!
    LOG_INFO(ct->GetOptions().info_log,
      "Out of order seqno on log %lu. Received:%lu Expected:%lu.",
      logid,
      request->GetSequenceNumber(),
      topic_map_.GetLastRead(logid) + 1);
    return;
  }

  // Prefix the namespace id to the topic name
  NamespaceTopic topic_name;
  PutNamespaceId(&topic_name, request->GetNamespaceId());
  topic_name.append(request->GetTopicName().ToString());

  // map the topic to a list of subscribers
  TopicList* list = topic_map_.GetSubscribers(topic_name);

  // send the messages to subscribers
  if (list != nullptr && !list->empty()) {
    // serialize the message only once
    Slice serialized = request->Serialize();

    // send serialized message to all subscribers.
    for (const auto& elem : *list) {
      // convert HostNumber to HostId
      HostId* hostid = ct->GetHostMap().Lookup(elem);
      assert(hostid != nullptr);
      if (hostid != nullptr) {
        st = ct->GetClient().Send(*hostid, serialized);
        if (st.ok()) {
          LOG_INFO(ct->GetOptions().info_log,
              "Sent data (%.16s)@%lu for Topic(%s) to %s:%ld",
              request->GetPayload().ToString().c_str(),
              request->GetSequenceNumber(),
              request->GetTopicName().ToString().c_str(),
              hostid->hostname.c_str(),
              (long)hostid->port);
        } else {
          LOG_INFO(ct->GetOptions().info_log,
              "Unable to forward Data message to %s:%ld",
              hostid->hostname.c_str(), (long)hostid->port);
          ct->GetOptions().info_log->Flush();
        }
      }
    }
  } else {
    LOG_INFO(ct->GetOptions().info_log,
      "No subscribers for Topic(%s)",
      request->GetTopicName().ToString().c_str());
  }

  // update the last message received for this log
  topic_map_.SetLastRead(logid, request->GetSequenceNumber());
}

// Process Gap messages that are coming in from Tailer.
void
ControlRoom::ProcessGap(std::unique_ptr<Message> msg, LogID logid) {
  MessageGap* request = static_cast<MessageGap*>(msg.get());

  if (topic_map_.GetLastRead(logid) + 1 == request->GetStartSequenceNumber()) {
    topic_map_.SetLastRead(logid, request->GetEndSequenceNumber());
    // TODO(pja) 1 : Forward data loss gap to copilot to notify consumer.
  }
}

}  // namespace rocketspeed
