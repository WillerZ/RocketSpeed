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
#include "src/messages/queues.h"
#include "src/util/common/coding.h"
#include "src/messages/flow_control.h"
#include "src/util/topic_uuid.h"

#include "external/folly/move_wrapper.h"

namespace rocketspeed {

ControlRoom::ControlRoom(const ControlTowerOptions& options,
                         ControlTower* control_tower,
                         unsigned int room_number) :
  control_tower_(control_tower),
  room_number_(room_number),
  topic_tailer_(control_tower->GetTopicTailer(room_number)) {

  room_to_client_queues_ = options.msg_loop->CreateWorkerQueues(
    options.room_to_client_queue_size);
}

ControlRoom::~ControlRoom() {
}

// The Control Tower uses this method to forward a message to this Room.
// The Control Room forwards some messages (those with seqno = 0) to
// itself by using this method.
std::unique_ptr<Command>
ControlRoom::MsgCommand(std::unique_ptr<Message> msg,
                        int worker_id,
                        StreamID origin) {
  auto moved_msg = folly::makeMoveWrapper(std::move(msg));
  std::unique_ptr<Command> cmd(
    MakeExecuteCommand([this, moved_msg, worker_id, origin] () mutable {
      std::unique_ptr<Message> message(moved_msg.move());
      if (message->GetMessageType() == MessageType::mSubscribe) {
        RS_ASSERT(worker_id != -1);
        ProcessSubscribe(std::move(message), worker_id, origin);
      } else if (message->GetMessageType() == MessageType::mUnsubscribe) {
        RS_ASSERT(worker_id != -1);
        ProcessUnsubscribe(std::move(message), worker_id, origin);
      } else if (message->GetMessageType() == MessageType::mGoodbye) {
        RS_ASSERT(worker_id == -1);
        ProcessGoodbye(std::move(message), origin);
      } else {
        LOG_ERROR(control_tower_->GetOptions().info_log,
          "Unexpected message type in room: %d",
          static_cast<int>(message->GetMessageType()));
        RS_ASSERT(false);
      }
    }));
  return cmd;
}

void ControlRoom::ProcessSubscribe(std::unique_ptr<Message> msg,
                                   int worker_id,
                                   StreamID origin) {
  ControlTower* ct = control_tower_;
  ControlTowerOptions& options = ct->GetOptions();

  MessageSubscribe* subscribe = static_cast<MessageSubscribe*>(msg.get());
  CopilotSub id(origin, subscribe->GetSubID());
  TopicUUID uuid(subscribe->GetNamespace(), subscribe->GetTopicName());
  const SequenceNumber seqno = subscribe->GetStartSequenceNumber();

  sub_worker_.Insert(id.stream_id, id.sub_id, worker_id);

  LOG_INFO(options.info_log,
    "Adding subscriber %s for %s@%" PRIu64,
    id.ToString().c_str(),
    uuid.ToString().c_str(),
    seqno);
  topic_tailer_->AddSubscriber(uuid, seqno, id);
}

void ControlRoom::ProcessUnsubscribe(std::unique_ptr<Message> msg,
                                     int worker_id,
                                     StreamID origin) {
  ControlTower* ct = control_tower_;
  ControlTowerOptions& options = ct->GetOptions();
  MessageUnsubscribe* unsubscribe = static_cast<MessageUnsubscribe*>(msg.get());
  CopilotSub id(origin, unsubscribe->GetSubID());

  // Remove this subscription request
  LOG_INFO(options.info_log,
    "Removing subscriber %s",
    id.ToString().c_str());

  topic_tailer_->RemoveSubscriber(id);

  sub_worker_.Remove(id.stream_id, id.sub_id);
}

// Process Goodbye messages that are coming in from ControlTower.
void
ControlRoom::ProcessGoodbye(std::unique_ptr<Message> msg,
                            StreamID origin) {
  ControlTower* ct = control_tower_;
  ControlTowerOptions& options = ct->GetOptions();

  LOG_INFO(options.info_log,
    "Received goodbye for Stream(%llu)",
    origin);

  topic_tailer_->RemoveSubscriber(origin);

  sub_worker_.Remove(origin);
}

void
ControlRoom::OnTailerMessage(Flow* flow,
                             const Message& msg,
                             std::vector<CopilotSub> recipients) {
  MessageType type = msg.GetMessageType();
  if (type == MessageType::mDeliver) {
    ProcessDeliver(flow, msg, std::move(recipients));
  } else if (type == MessageType::mGap) {
    ProcessGap(flow, msg, std::move(recipients));
  } else {
    RS_ASSERT(false);
  }
}


// Process Data messages that are coming in from Tailer.
void
ControlRoom::ProcessDeliver(Flow* flow,
                            const Message& msg,
                            const std::vector<CopilotSub>& recipients) {
  ControlTower* ct = control_tower_;
  ControlTowerOptions& options = ct->GetOptions();
  Status st;

  // get the request message
  const MessageData* request = static_cast<const MessageData*>(&msg);

  const SequenceNumber prev_seqno = request->GetPrevSequenceNumber();
  const SequenceNumber next_seqno = request->GetSequenceNumber();
  LOG_DEBUG(options.info_log,
      "Received data (%.16s)@%" PRIu64 "-%" PRIu64 " for Topic(%s,%s)",
      request->GetPayload().ToString().c_str(),
      prev_seqno,
      next_seqno,
      request->GetNamespaceId().ToString().c_str(),
      request->GetTopicName().ToString().c_str());

  // For each subscriber on this topic at prev_seqno, deliver the message and
  // advance the subscription to next_seqno.
  TopicUUID uuid(request->GetNamespaceId(), request->GetTopicName());
  for (CopilotSub recipient : recipients) {
    // Send to correct worker loop.
    const int worker_id = CopilotWorker(recipient);
    if (worker_id == -1) {
      LOG_WARN(options.info_log,
        "Unknown worker for subscription %s",
        recipient.ToString().c_str());
      continue;
    }

    MessageDeliverData deliver(request->GetTenantID(),
                               recipient.sub_id,
                               request->GetMessageId(),
                               request->GetPayload().ToString());
    deliver.SetSequenceNumbers(prev_seqno, next_seqno);
    auto command = MsgLoop::ResponseCommand(deliver, recipient.stream_id);

    flow->Write(room_to_client_queues_[worker_id].get(), command);
    LOG_DEBUG(options.info_log,
             "Sent data (%.16s)@%" PRIu64 " for %s to %s",
             request->GetPayload().ToString().c_str(),
             request->GetSequenceNumber(),
             uuid.ToString().c_str(),
             recipient.ToString().c_str());
  }

  if (recipients.empty()) {
    LOG_WARN(options.info_log,
      "No recipients for record in %s@%" PRIu64 ": no message sent.",
      uuid.ToString().c_str(),
      request->GetSequenceNumber());
  }
}

// Process Gap messages that are coming in from Tailer.
void
ControlRoom::ProcessGap(Flow* flow,
                        const Message& msg,
                        const std::vector<CopilotSub>& recipients) {
  const MessageGap* gap = static_cast<const MessageGap*>(&msg);

  SequenceNumber prev_seqno = gap->GetStartSequenceNumber();
  SequenceNumber next_seqno = gap->GetEndSequenceNumber();

  ControlTower* ct = control_tower_;
  ControlTowerOptions& options = ct->GetOptions();
  LOG_DEBUG(options.info_log,
      "Received gap %" PRIu64 "-%" PRIu64 " for Topic(%s,%s)",
      prev_seqno,
      next_seqno,
      gap->GetNamespaceId().c_str(),
      gap->GetTopicName().c_str());

  for (CopilotSub recipient : recipients) {
    const int worker_id = CopilotWorker(recipient);
    if (worker_id == -1) {
      LOG_WARN(options.info_log,
        "Unknown worker for subscription %s",
        recipient.ToString().c_str());
      continue;
    }
    MessageDeliverGap deliver(gap->GetTenantID(),
                              recipient.sub_id,
                              gap->GetType());
    deliver.SetSequenceNumbers(prev_seqno, next_seqno);
    auto command = MsgLoop::ResponseCommand(deliver, recipient.stream_id);

    flow->Write(room_to_client_queues_[worker_id].get(), command);
    LOG_DEBUG(options.info_log,
      "Sent gap %" PRIu64 "-%" PRIu64 " for Topic(%s,%s) to %llu",
      prev_seqno,
      next_seqno,
      gap->GetNamespaceId().c_str(),
      gap->GetTopicName().c_str(),
      recipient.stream_id);
  }

  if (recipients.empty()) {
    LOG_WARN(options.info_log, "No recipients for gap: no message sent.");
  }
}

int ControlRoom::CopilotWorker(const CopilotSub& id) {
  int* ptr = sub_worker_.Find(id.stream_id, id.sub_id);
  return ptr ? *ptr : -1;
}


}  // namespace rocketspeed
