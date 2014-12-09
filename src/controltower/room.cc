//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/controltower/room.h"

#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "src/controltower/tower.h"
#include "src/util/common/coding.h"

namespace rocketspeed {

ControlRoom::ControlRoom(const ControlTowerOptions& options,
                         ControlTower* control_tower,
                         unsigned int room_number) :
  control_tower_(control_tower),
  room_number_(room_number),
  topic_map_(control_tower->GetTailer()),
  room_loop_(options.env,
             options.worker_queue_size,
             WorkerLoopType::kMultiProducer) {
}

ControlRoom::~ControlRoom() {
}

// static method to start the loop for processing Room events
void ControlRoom::Run(void* arg) {
  ControlRoom* room = static_cast<ControlRoom*>(arg);
  LOG_INFO(room->control_tower_->GetOptions().info_log,
      "Starting ControlRoom Loop at room number %d",
      room->room_number_);

  // Define a lambda to process Commands by the room_loop_.
  auto command_callback = [room] (RoomCommand command) {
    std::unique_ptr<Message> message = command.GetMessage();
    MessageType type = message->GetMessageType();
    LogID logid = command.GetLogId();
    int worker_id = command.GetWorkerId();
    if (type == MessageType::mDeliver) {
      // data message from Tailer
      assert(worker_id == -1);  // from tailer
      room->ProcessDeliver(std::move(message), logid);
    } else if (type == MessageType::mMetadata) {
      // subscription message from ControlTower
      assert(worker_id != -1);  // from tower
      room->ProcessMetadata(std::move(message), logid, worker_id);
    } else if (type == MessageType::mGap) {
      // gap message from Tailer
      assert(worker_id == -1);  // from tailer
      room->ProcessGap(std::move(message), logid);
    }
  };

  room->room_loop_.Run(command_callback);
}

// The Control Tower uses this method to forward a message to this Room.
// The Control Room forwards some messages (those with seqno = 0) to
// itself by using this method.
Status
ControlRoom::Forward(std::unique_ptr<Message> msg, LogID logid, int worker_id) {
  if (room_loop_.Send(std::move(msg), logid, worker_id)) {
    return Status::OK();
  } else {
    return Status::InternalError("Worker queue full");
  }
}

// Process Metadata messages that are coming in from ControlTower.
void
ControlRoom::ProcessMetadata(std::unique_ptr<Message> msg,
                             LogID logid,
                             int worker_id) {
  ControlTower* ct = control_tower_;
  ControlTowerOptions& options = ct->GetOptions();
  Status st;

  // get the request message
  MessageMetadata* request = static_cast<MessageMetadata*>(msg.get());
  assert(request->GetMetaType() == MessageMetadata::MetaType::Request);
  if (request->GetMetaType() != MessageMetadata::MetaType::Request) {
    LOG_WARN(options.info_log,
        "MessageMetadata with bad type %d received, ignoring...",
        request->GetMetaType());
    return;
  }

  // There should be only one topic for this message. The ControlTower
  // splits every topic into a distinct separate messages per ControlRoom.
  const std::vector<TopicPair>& topic = request->GetTopicInfo();
  assert(topic.size() == 1);
  const ClientID& origin = request->GetOrigin();

  // Handle to 0 sequence number special case.
  // Zero means to start reading from the latest records, so we first need
  // to asynchronously consult the tailer for the latest seqno, and then
  // process the subscription.
  if (topic[0].topic_type == MetadataType::mSubscribe &&
      topic[0].seqno == 0) {
    // Create a callback to enqueue a subscribe command.
    // TODO(pja) 1: When this is passed to FindLatestSeqno, it will allocate
    // when converted to an std::function - could use an alloc pool for this.
    auto callback = [this, logid, request, worker_id] (Status st,
                                                       SequenceNumber seqno) {
      std::unique_ptr<Message> msg(request);
      if (!st.ok()) {
        LOG_WARN(control_tower_->GetOptions().info_log,
                 "Failed to find latest sequence number in Log(%lu)",
                 logid);
        return;
      }

      request->GetTopicInfo()[0].seqno = seqno;  // update seqno
      // send message back to this Room with the seqno appropriately
      // filled up in the message.
      assert(seqno != 0);
      st = Forward(std::move(msg), logid, worker_id);
      if (!st.ok()) {
        // TODO(pja) 1: may need to do some flow control if this is due
        // to receiving too many subscriptions.
        LOG_WARN(control_tower_->GetOptions().info_log,
                 "Failed to enqueue subscription (%s)",
                 st.ToString().c_str());
      } else {
        const ClientID& origin = request->GetOrigin();
        const std::vector<TopicPair>& topic = request->GetTopicInfo();
        LOG_INFO(control_tower_->GetOptions().info_log,
                 "Subscribing %s at latest seqno for Topic(%s)@%lu",
                 origin.c_str(),
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
      LOG_WARN(options.info_log,
               "Failed to find latest seqno (%s)",
               st.ToString().c_str());
    } else {
      LOG_INFO(options.info_log,
        "Sent FindLatestSeqno request for %s for Topic(%s)",
        origin.c_str(),
        topic[0].topic_name.c_str());
    }
    return;
  }

  // Map the origin to a HostNumber
  int test_worker_id = -1;
  HostNumber hostnum = ct->LookupHost(origin, &test_worker_id);
  if (hostnum == -1) {
    hostnum = ct->InsertHost(origin, worker_id);
  } else {
    assert(test_worker_id == worker_id);
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
    LOG_INFO(options.info_log,
        "Added subscriber %s for Topic(%s)@%lu",
        origin.c_str(),
        topic[0].topic_name.c_str(),
        topic[0].seqno);
  } else if (topic[0].topic_type == MetadataType::mUnSubscribe) {
    topic_map_.RemoveSubscriber(topic_name,
                                logid, hostnum, room_number_);
    LOG_INFO(options.info_log,
        "Removed subscriber %s from Topic(%s)",
        origin.c_str(),
        topic[0].topic_name.c_str());
  }

  // change it to a response ack message
  request->SetMetaType(MessageMetadata::MetaType::Response);

  // serialize message
  std::string out;
  request->SerializeToString(&out);

  // send response back to client
  std::unique_ptr<Command> cmd(new TowerCommand(std::move(out),
                                                origin,
                                                options.env->NowMicros()));
  st = ct->SendCommand(std::move(cmd), worker_id);
  if (!st.ok()) {
    LOG_WARN(options.info_log,
        "Unable to send %s response for Topic(%s)@%lu to tower for %s",
        topic[0].topic_type == MetadataType::mSubscribe
          ? "subscribe" : "unsubscribe",
        topic[0].topic_name.c_str(),
        topic[0].seqno,
        origin.c_str());
  } else {
    LOG_INFO(options.info_log,
        "Sent %s response for Topic(%s)@%lu to tower for %s",
        topic[0].topic_type == MetadataType::mSubscribe
          ? "subscribe" : "unsubscribe",
        topic[0].topic_name.c_str(),
        topic[0].seqno,
        origin.c_str());
  }
  options.info_log->Flush();
}

// Process Data messages that are coming in from Tailer.
void
ControlRoom::ProcessDeliver(std::unique_ptr<Message> msg, LogID logid) {
  ControlTower* ct = control_tower_;
  ControlTowerOptions& options = ct->GetOptions();
  Status st;

  // get the request message
  MessageData* request = static_cast<MessageData*>(msg.get());

  LOG_INFO(options.info_log,
      "Received data (%.16s)@%lu for Topic(%s)",
      request->GetPayload().ToString().c_str(),
      request->GetSequenceNumber(),
      request->GetTopicName().ToString().c_str());

  // Check that seqno is as expected.
  if (topic_map_.GetLastRead(logid) + 1 != request->GetSequenceNumber()) {
    // Out of order sequence number, skip!
    LOG_INFO(options.info_log,
      "Out of order seqno on Log(%lu). Received:%lu Expected:%lu.",
      logid,
      request->GetSequenceNumber(),
      topic_map_.GetLastRead(logid) + 1);
    return;
  }

  // Prefix the namespace id to the topic name
  NamespaceTopic topic_name;
  PutNamespaceId(&topic_name, request->GetNamespaceId());
  topic_name.append(request->GetTopicName().ToString());

  // serialize msg
  std::string serial;
  request->SerializeToString(&serial);

  // map the topic to a list of subscribers
  TopicList* list = topic_map_.GetSubscribers(topic_name);

  // send the messages to subscribers
  if (list != nullptr && !list->empty()) {
    // Recipients for each control tower event loop worker.
    std::unordered_map<int, SendCommand::Recipients> destinations;

    // find all subscribers
    for (const auto& hostnum : *list) {
      // convert HostNumber to ClientID
      int worker_id = -1;
      const ClientID* hostid = ct->LookupHost(hostnum, &worker_id);
      assert(hostid != nullptr);
      assert(worker_id != -1);
      if (hostid != nullptr && worker_id != -1) {
        destinations[worker_id].push_back(*hostid);
      }
    }

    // Send command to each worker loop.
    for (auto it = destinations.begin(); it != destinations.end(); ++it) {
      int worker_id = it->first;
      SendCommand::Recipients& recipients = it->second;

      std::unique_ptr<TowerCommand> cmd(
        new TowerCommand(serial,  // TODO(pja) 1 : avoid copy here
                         std::move(recipients),
                         options.env->NowMicros()));

      // Send to correct worker loop.
      st = ct->SendCommand(std::move(cmd), worker_id);

      if (st.ok()) {
        LOG_INFO(options.info_log,
                "Sent data (%.16s)@%lu for Topic(%s) to %s",
                request->GetPayload().ToString().c_str(),
                request->GetSequenceNumber(),
                request->GetTopicName().ToString().c_str(),
                HostMap::ToString(recipients).c_str());
      } else {
        LOG_INFO(options.info_log,
                "Unable to forward Data message to subscriber %s",
                HostMap::ToString(recipients).c_str());
        options.info_log->Flush();
      }
    }
  } else {
    LOG_INFO(options.info_log,
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
