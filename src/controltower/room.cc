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

#include "external/folly/move_wrapper.h"

namespace rocketspeed {

ControlRoom::ControlRoom(const ControlTowerOptions& options,
                         ControlTower* control_tower,
                         unsigned int room_number) :
  control_tower_(control_tower),
  room_number_(room_number),
  topic_tailer_(control_tower->GetTopicTailer(room_number)) {
}

ControlRoom::~ControlRoom() {
}

// The Control Tower uses this method to forward a message to this Room.
// The Control Room forwards some messages (those with seqno = 0) to
// itself by using this method.
Status
ControlRoom::Forward(std::unique_ptr<Message> msg,
                     int worker_id,
                     StreamID origin) {
  auto moved_msg = folly::makeMoveWrapper(std::move(msg));
  std::unique_ptr<Command> cmd(
    MakeExecuteCommand([this, moved_msg, worker_id, origin] () mutable {
      std::unique_ptr<Message> message(moved_msg.move());
      if (message->GetMessageType() == MessageType::mMetadata) {
        assert(worker_id != -1);
        ProcessMetadata(std::move(message), worker_id, origin);
      } else if (message->GetMessageType() == MessageType::mGoodbye) {
        assert(worker_id == -1);
        ProcessGoodbye(std::move(message), origin);
      } else {
        LOG_ERROR(control_tower_->GetOptions().info_log,
          "Unexpected message type in room: %d",
          static_cast<int>(message->GetMessageType()));
        assert(false);
      }
    }));
  MsgLoop* msg_loop = control_tower_->GetOptions().msg_loop;
  return msg_loop->SendCommand(std::move(cmd), room_number_);
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
        "Added subscriber %llu for Topic(%s,%s)@%" PRIu64,
        origin,
        topic[0].namespace_id.c_str(),
        topic[0].topic_name.c_str(),
        topic[0].seqno);
  } else if (topic[0].topic_type == MetadataType::mUnSubscribe) {
    topic_tailer_->RemoveSubscriber(uuid, hostnum);
    LOG_INFO(options.info_log,
        "Removed subscriber %llu from Topic(%s,%s)",
        origin,
        topic[0].namespace_id.c_str(),
        topic[0].topic_name.c_str());
  }
}

// Process Goodbye messages that are coming in from ControlTower.
void
ControlRoom::ProcessGoodbye(std::unique_ptr<Message> msg,
                            StreamID origin) {
  ControlTower* ct = control_tower_;
  ControlTowerOptions& options = ct->GetOptions();

  // Map the origin to a HostNumber
  int test_worker_id = -1;
  HostNumber hostnum = ct->LookupHost(origin, &test_worker_id);
  if (hostnum == -1) {
    // Unknown host, ignore.
    LOG_WARN(options.info_log,
      "Received goodbye from unknown origin Stream(%llu)",
      origin);
    return;
  }
  assert(hostnum >= 0);
  LOG_INFO(options.info_log,
    "Received goodbye for Stream(%llu) Hostnum(%d)",
    origin,
    int(hostnum));

  topic_tailer_->RemoveSubscriber(hostnum);
}

void
ControlRoom::OnTailerMessage(std::unique_ptr<Message> msg,
                             std::vector<HostNumber> hosts) {
  MessageType type = msg->GetMessageType();
  if (type == MessageType::mDeliver) {
    ProcessDeliver(std::move(msg), std::move(hosts));
  } else if (type == MessageType::mGap) {
    ProcessGap(std::move(msg), std::move(hosts));
  } else {
    assert(false);
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
  LOG_DEBUG(options.info_log,
      "Received data (%.16s)@%" PRIu64 "-%" PRIu64 " for Topic(%s,%s)",
      request->GetPayload().ToString().c_str(),
      prev_seqno,
      next_seqno,
      request->GetNamespaceId().ToString().c_str(),
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
        LOG_DEBUG(options.info_log,
                 "Sent data (%.16s)@%" PRIu64 " for %s to %llu",
                 request->GetPayload().ToString().c_str(),
                 request->GetSequenceNumber(),
                 uuid.ToString().c_str(),
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
      "No hosts for record in %s@%" PRIu64 ": no message sent.",
      uuid.ToString().c_str(),
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
  LOG_DEBUG(options.info_log,
      "Received gap %" PRIu64 "-%" PRIu64 " for Topic(%s,%s)",
      prev_seqno,
      next_seqno,
      gap->GetNamespaceId().c_str(),
      gap->GetTopicName().c_str());

  for (HostNumber hostnum : hosts) {
    // Convert HostNumber to origin StreamID.
    int worker_id = -1;
    StreamID origin = ct->LookupHost(hostnum, &worker_id);
    assert(worker_id != -1);
    if (worker_id != -1) {
      // Send to correct worker loop.
      Status st = options.msg_loop->SendResponse(*gap, origin, worker_id);

      if (st.ok()) {
        LOG_DEBUG(options.info_log,
                 "Sent gap %" PRIu64 "-%" PRIu64 " for Topic(%s,%s) to %llu",
                 prev_seqno,
                 next_seqno,
                 gap->GetNamespaceId().c_str(),
                 gap->GetTopicName().c_str(),
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
}

}  // namespace rocketspeed
