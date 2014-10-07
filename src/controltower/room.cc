//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/controltower/room.h"
#include <map>
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
  room_loop_(options.worker_queue_size) {
}

ControlRoom::~ControlRoom() {
}

// static method to start the loop for processing Room events
void ControlRoom::Run(void* arg) {
  ControlRoom* room = static_cast<ControlRoom*>(arg);
  Log(InfoLogLevel::INFO_LEVEL,
      room->control_tower_->GetOptions().info_log,
      "Starting ControlRoom Loop at port %d", room->room_id_.port);

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
    Log(InfoLogLevel::WARN_LEVEL, ct->GetOptions().info_log,
        "MessageMetadata with bad type %d received, ignoring...",
        request->GetMetaType());
    return;
  }

  // There should be only one topic for this message. The ControlTower
  // splits every topic into a distinct separate messages per ControlRoom.
  const std::vector<TopicPair>& topic = request->GetTopicInfo();
  assert(topic.size() == 1);
  const HostId origin = request->GetOrigin();

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
  } else if (topic[0].topic_type == MetadataType::mUnSubscribe) {
    topic_map_.RemoveSubscriber(topic_name,
                                logid, hostnum, room_number_);
  }

  // change it to a response ack message
  request->SetMetaType(MessageMetadata::MetaType::Response);

  // send reponse back to client
  st = ct->GetClient().Send(origin, std::move(msg));
  if (!st.ok()) {
    Log(InfoLogLevel::INFO_LEVEL, ct->GetOptions().info_log,
        "Unable to send Metadata response to %s:%d",
        origin.hostname.c_str(), origin.port);
  } else {
    Log(InfoLogLevel::INFO_LEVEL, ct->GetOptions().info_log,
        "Send Metadata response to %s:%d",
        origin.hostname.c_str(), origin.port);
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

  // Prefix the namespace id to the topic name
  NamespaceTopic topic_name;
  PutNamespaceId(&topic_name, request->GetNamespaceId());
  topic_name.append(request->GetTopicName().ToString());

  // map the topic to a list of subscribers
  TopicList* list = topic_map_.GetSubscribers(topic_name);

  // send the messages to subscribers
  if (list != nullptr) {
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
          Log(InfoLogLevel::INFO_LEVEL, ct->GetOptions().info_log,
            "Sent data (%.16s) for topic %s to %s:%d",
            request->GetPayload().ToString().c_str(),
            request->GetTopicName().ToString().c_str(),
            hostid->hostname.c_str(),
            hostid->port);
        } else {
          Log(InfoLogLevel::INFO_LEVEL, ct->GetOptions().info_log,
              "Unable to forward Data message to %s:%d",
              hostid->hostname.c_str(), hostid->port);
          ct->GetOptions().info_log->Flush();
        }
      }
    }
  }
  // update the last message received for this log
  topic_map_.SetLastRead(logid, request->GetSequenceNumber());
}

}  // namespace rocketspeed
