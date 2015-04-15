//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "src/controltower/room.h"

#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "src/controltower/topic_tailer.h"
#include "src/controltower/tower.h"
#include "src/util/common/coding.h"
#include "src/util/topic_uuid.h"

namespace rocketspeed {

ControlRoom::ControlRoom(const ControlTowerOptions& options,
                         ControlTower* control_tower,
                         unsigned int room_number) :
  control_tower_(control_tower),
  room_number_(room_number),
  topic_tailer_(control_tower->GetTopicTailer(room_number)),
  room_loop_(options.worker_queue_size) {
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
    if (type == MessageType::mMetadata) {
      // subscription message from ControlTower
      int worker_id = command.GetWorkerId();
      StreamID origin = command.GetOrigin();
      assert(worker_id != -1);  // from tower
      room->ProcessMetadata(std::move(message), worker_id, origin);
    } else if (type == MessageType::mDeliver) {
      room->ProcessDeliver(std::move(message), command.GetHosts());
    } else if (type == MessageType::mGap) {
      room->ProcessGap(std::move(message), command.GetHosts());
    }
  };

  room->room_loop_.Run(command_callback);
}

// The Control Tower uses this method to forward a message to this Room.
// The Control Room forwards some messages (those with seqno = 0) to
// itself by using this method.
Status
ControlRoom::Forward(std::unique_ptr<Message> msg,
                     int worker_id,
                     StreamID origin) {
  if (room_loop_.Send(std::move(msg), worker_id, origin)) {
    return Status::OK();
  } else {
    return Status::NoBuffer();
  }
}

// Process Metadata messages that are coming in from ControlTower.
void
ControlRoom::ProcessMetadata(std::unique_ptr<Message> msg,
                             int worker_id,
                             StreamID origin) {
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

  // Handle to 0 sequence number special case.
  // Zero means to start reading from the latest records, so we first need
  // to asynchronously consult the tailer for the latest seqno, and then
  // process the subscription.
  if (topic[0].topic_type == MetadataType::mSubscribe &&
      topic[0].seqno == 0) {
    // Create a callback to enqueue a subscribe command.
    // TODO(pja) 1: When this is passed to FindLatestSeqno, it will allocate
    // when converted to an std::function - could use an alloc pool for this.
    auto callback = [this, request, worker_id, origin] (Status status,
                                                        SequenceNumber seqno) {
      std::unique_ptr<Message> message(request);
      if (!status.ok()) {
        LOG_WARN(control_tower_->GetOptions().info_log,
                 "Failed to find latest sequence number in Topic(%s, %s) (%s)",
                 request->GetTopicInfo()[0].namespace_id.c_str(),
                 request->GetTopicInfo()[0].topic_name.c_str(),
                 status.ToString().c_str());
        return;
      }

      std::vector<TopicPair>& req_topic = request->GetTopicInfo();
      req_topic[0].seqno = seqno;  // update seqno
      LOG_INFO(control_tower_->GetOptions().info_log,
               "Subscribing (%llu) at latest seqno for Topic(%s)@%" PRIu64,
               origin,
               req_topic[0].topic_name.c_str(),
               req_topic[0].seqno);

      // send message back to this Room with the seqno appropriately
      // filled up in the message.
      assert(seqno != 0);
      status = Forward(std::move(message), worker_id, origin);
      if (!status.ok()) {
        // TODO(pja) 1: may need to do some flow control if this is due
        // to receiving too many subscriptions.
        LOG_WARN(control_tower_->GetOptions().info_log,
                 "Failed to enqueue subscription (%s)",
                 status.ToString().c_str());
      }
    };

    TopicUUID uuid(topic[0].namespace_id, topic[0].topic_name);
    st = ct->GetTopicTailer(room_number_)->FindLatestSeqno(uuid, callback);
    if (!st.ok()) {
      // TODO(pja) 1: may need to do some flow control if this is due
      // to receiving too many subscriptions.
      LOG_WARN(options.info_log,
               "Failed to find latest seqno (%s) for %s",
               st.ToString().c_str(),
               uuid.ToString().c_str());
    } else {
      // Succeeded, so ownership of message is in the callback.
      msg.release();
      LOG_INFO(options.info_log,
               "Sent FindLatestSeqno request for (%llu) for %s",
               origin,
               uuid.ToString().c_str());
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


  // Remember this subscription request
  TopicUUID uuid(topic[0].namespace_id, topic[0].topic_name);
  if (topic[0].topic_type == MetadataType::mSubscribe) {
    topic_tailer_->AddSubscriber(uuid, topic[0].seqno, hostnum);
    LOG_INFO(options.info_log,
        "Added subscriber %llu for Topic(%s)@%" PRIu64,
        origin,
        topic[0].topic_name.c_str(),
        topic[0].seqno);
  } else if (topic[0].topic_type == MetadataType::mUnSubscribe) {
    topic_tailer_->RemoveSubscriber(uuid, hostnum);
    LOG_INFO(options.info_log,
        "Removed subscriber %llu from Topic(%s)",
        origin,
        topic[0].topic_name.c_str());
  }

  // change it to a response ack message
  request->SetMetaType(MessageMetadata::MetaType::Response);

  // send response back to copilot
  st = options.msg_loop->SendResponse(*request, origin, worker_id);
  if (!st.ok()) {
    LOG_WARN(options.info_log,
             "Unable to send %s response for Topic(%s)@%" PRIu64
             " to tower for %llu",
             topic[0].topic_type == MetadataType::mSubscribe ? "subscribe"
                                                             : "unsubscribe",
             topic[0].topic_name.c_str(),
             topic[0].seqno,
             origin);
  } else {
    LOG_INFO(options.info_log,
        "Sent %s response for Topic(%s)@%" PRIu64 " to tower for %llu",
        topic[0].topic_type == MetadataType::mSubscribe
          ? "subscribe" : "unsubscribe",
        topic[0].topic_name.c_str(),
        topic[0].seqno,
        origin);
  }
  options.info_log->Flush();
}

Status
ControlRoom::OnTailerMessage(std::unique_ptr<Message> msg,
                             std::vector<HostNumber> hosts) {
  if (room_loop_.Send(std::move(msg), std::move(hosts))) {
    return Status::OK();
  } else {
    return Status::NoBuffer();
  }
}


// Process Data messages that are coming in from Tailer.
void
ControlRoom::ProcessDeliver(std::unique_ptr<Message> msg,
                            const std::vector<HostNumber>& hosts) {
  ControlTower* ct = control_tower_;
  ControlTowerOptions& options = ct->GetOptions();
  Status st;

  // get the request message
  MessageData* request = static_cast<MessageData*>(msg.get());

  const SequenceNumber prev_seqno = request->GetPrevSequenceNumber();
  const SequenceNumber next_seqno = request->GetSequenceNumber();
  LOG_INFO(options.info_log,
      "Received data (%.16s)@%" PRIu64 "-%" PRIu64 " for Topic(%s)",
      request->GetPayload().ToString().c_str(),
      prev_seqno,
      next_seqno,
      request->GetTopicName().ToString().c_str());

  // serialize msg
  std::string serial;

  // For each subscriber on this topic at prev_seqno, deliver the message and
  // advance the subscription to next_seqno.
  TopicUUID uuid(request->GetNamespaceId(), request->GetTopicName());
  for (HostNumber hostnum : hosts) {
    // Convert HostNumber to origin StreamID.
    int worker_id = -1;
    StreamID origin = ct->LookupHost(hostnum, &worker_id);
    assert(worker_id != -1);
    if (worker_id != -1) {
      // Send to correct worker loop.
      st = options.msg_loop->SendResponse(*request, origin, worker_id);

      if (st.ok()) {
        LOG_INFO(options.info_log,
                 "Sent data (%.16s)@%" PRIu64 " for Topic(%s) to %llu",
                 request->GetPayload().ToString().c_str(),
                 request->GetSequenceNumber(),
                 request->GetTopicName().ToString().c_str(),
                 origin);
      } else {
        LOG_WARN(options.info_log,
                 "Unable to forward Data message to subscriber %llu",
                 origin);
      }
    }
  }

  if (hosts.empty()) {
    LOG_WARN(options.info_log,
      "No hosts for record in Topic(%s, %s)@%" PRIu64 ": no message sent.",
      request->GetNamespaceId().ToString().c_str(),
      request->GetTopicName().ToString().c_str(),
      request->GetSequenceNumber());
  }
}

// Process Gap messages that are coming in from Tailer.
void
ControlRoom::ProcessGap(std::unique_ptr<Message> msg,
                        const std::vector<HostNumber>& hosts) {
  MessageGap* gap = static_cast<MessageGap*>(msg.get());

  SequenceNumber prev_seqno = gap->GetStartSequenceNumber();
  SequenceNumber next_seqno = gap->GetEndSequenceNumber();

  ControlTower* ct = control_tower_;
  ControlTowerOptions& options = control_tower_->GetOptions();
  LOG_INFO(options.info_log,
      "Received gap %" PRIu64 "-%" PRIu64 " for Topic(%s)",
      prev_seqno,
      next_seqno,
      gap->GetTopicName().ToString().c_str());

  for (HostNumber hostnum : hosts) {
    // Convert HostNumber to origin StreamID.
    int worker_id = -1;
    StreamID origin = ct->LookupHost(hostnum, &worker_id);
    assert(worker_id != -1);
    if (worker_id != -1) {
      // Send to correct worker loop.
      Status st = options.msg_loop->SendResponse(*gap, origin, worker_id);

      if (st.ok()) {
        LOG_INFO(options.info_log,
                 "Sent gap %" PRIu64 "-%" PRIu64 " for Topic(%s) to %llu",
                 prev_seqno,
                 next_seqno,
                 gap->GetTopicName().ToString().c_str(),
                 origin);
      } else {
        LOG_WARN(options.info_log,
                 "Unable to forward Gap message to subscriber %llu",
                 origin);
      }
    }
  }

  if (hosts.empty()) {
    LOG_WARN(options.info_log, "No hosts for gap: no message sent.");
  }

  // TODO(pja) 1 : Send to copilots.
}

}  // namespace rocketspeed
