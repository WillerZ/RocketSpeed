// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "publisher.h"

#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

#include "src/client/smart_wake_lock.h"
#include "src/messages/event_callback.h"
#include "src/messages/messages.h"
#include "src/messages/msg_loop.h"
#include "src/messages/commands.h"
#include "src/messages/stream.h"
#include "src/port/port.h"
#include "src/util/common/guid_generator.h"
#include "src/util/common/hash.h"
#include "src/util/common/noncopyable.h"
#include "src/util/common/nonmovable.h"
#include "src/util/common/thread_check.h"
#include "src/util/timeout_list.h"

namespace rocketspeed {

/** Publisher uses this class to tell user the status of publish request. */
class ClientResultStatus : public ResultStatus {
 public:
  ClientResultStatus(Status status,
                     MessageData message,
                     SequenceNumber seqno)
  : status_(status)
  , message_(std::move(message))
  , seqno_(seqno) {
  }

  virtual Status GetStatus() const { return status_; }

  virtual MsgId GetMessageId() const {
    return message_.GetMessageId();
  }

  virtual SequenceNumber GetSequenceNumber() const {
    // Sequence number comes from the ack, not the original message.
    return seqno_;
  }

  virtual Slice GetTopicName() const {
    return message_.GetTopicName();
  }

  virtual Slice GetNamespaceId() const {
    return message_.GetNamespaceId();
  }

  virtual Slice GetContents() const {
    return message_.GetPayload();
  }

  ~ClientResultStatus() {}

 private:
  Status status_;
  const MessageData message_;
  SequenceNumber seqno_;
};

////////////////////////////////////////////////////////////////////////////////
/** Describes published message awaiting response. */
class PendingAck {
 public:
  PendingAck(PublishCallback _callback, MessageData _data)
      : callback(std::move(_callback)), data(std::move(_data)) {}

  PublishCallback callback;
  MessageData data;
};

/** State of a single publisher, aligned to avoid false sharing. */
class alignas(CACHE_LINE_SIZE) PublisherWorkerData : public StreamReceiver {
 public:
  PublisherWorkerData(const ClientOptions& options,
                      PublisherImpl* publisher,
                      EventLoop* event_loop)
  : publisher_(publisher)
  , event_loop_(event_loop)
  , publish_timeout_(options.publish_timeout) {
    timer_callback_ = event_loop_->RegisterTimerCallback(
      [this] { CheckTimeouts(); }, options.timer_period);
  }

  /** Publishes message to the Pilot. */
  void Publish(std::unique_ptr<MessageData> message,
               PublishCallback callback);

 private:
  ThreadCheck thread_check_;
  PublisherImpl* const publisher_;
  /** An EventLoop this publisher operates on. */
  EventLoop* const event_loop_;
  /** Timeout for publishes. */
  const std::chrono::milliseconds publish_timeout_;
  /** Stream used by this worker to talk to the pilot. */
  std::unique_ptr<Stream> pilot_stream_;
  /** Messages sent, awaiting ack. */
  std::unordered_map<MsgId, PendingAck, MsgId::Hash> messages_sent_;
  /** List of messages to timeout. */
  TimeoutList<MsgId, MsgId::Hash> timeouts_;
  /** Timer Callback */
  std::unique_ptr<EventCallback> timer_callback_;

  /** Checks for publish timeouts. */
  void CheckTimeouts();

  void ReceiveDataAck(StreamReceiveArg<MessageDataAck>) final override;

  void ReceiveGoodbye(StreamReceiveArg<MessageGoodbye>) final override;
};

void PublisherWorkerData::Publish(std::unique_ptr<MessageData> message,
                                  PublishCallback callback) {
  const MsgId message_id = message->GetMessageId();
  thread_check_.Check();

  // Check if we have a valid socket to the Pilot, recreate it if not.
  if (!pilot_stream_) {
    // Get the pilot's address.
    HostId pilot;
    Status st = publisher_->publisher_->GetPilot(&pilot);
    if (!st.ok()) {
      LOG_WARN(publisher_->info_log_,
               "Failed to obtain Pilot address: %s",
               st.ToString().c_str());
      // We'll try to obtain the address and reconnect on next occasion.
      // TODO(stupaq) make above comment valid.
      std::unique_ptr<ClientResultStatus> result_status(
        new ClientResultStatus(
          Status::IOError("No available RocketSpeed hosts"),
          *message, 0));
      callback(std::move(result_status));
      return;
    }

    // And create socket to it.
    pilot_stream_ = event_loop_->OpenStream(pilot);
    pilot_stream_->SetReceiver(this);

    LOG_INFO(publisher_->info_log_,
             "Reconnected to %s on stream %" PRIu64,
             pilot.ToString().c_str(),
             pilot_stream_->GetLocalID());
  }

  auto result = messages_sent_.emplace(
    message_id, PendingAck(std::move(callback), *message));
  (void)result;
  RS_ASSERT(result.second);

  // Send out the request.
  // Need an L-value unique_ptr<Message> (not MessageData)
  std::unique_ptr<Message> message_to_write(std::move(message));
  pilot_stream_->Write(message_to_write);

  // Add message to the sent list.
  timeouts_.Add(message_id);
}


void PublisherWorkerData::ReceiveDataAck(StreamReceiveArg<MessageDataAck> arg) {
  thread_check_.Check();

  // For each ack'd message, if it was waiting for an ack then remove it
  // from the waiting list and let the application know about the ack.
  for (const auto& ack : arg.message->GetAcks()) {
    LOG_DEBUG(publisher_->info_log_,
              "Received DataAck for message ID %s",
              ack.msgid.ToHexString().c_str());

    auto it = messages_sent_.find(ack.msgid);
    if (it != messages_sent_.end()) {
      if (it->second.callback) {
        Status st;
        SequenceNumber seqno = 0;
        if (ack.status == MessageDataAck::AckStatus::Success) {
          st = Status::OK();
          seqno = ack.seqno;
        } else {
          st = Status::IOError("Server informed that publish failed");
        }

        if (it->second.callback) {
          std::unique_ptr<ClientResultStatus> result_status(
              new ClientResultStatus(st, std::move(it->second.data), seqno));
          it->second.callback(std::move(result_status));
        }
      }

      // Remove sent message from list.
      messages_sent_.erase(it);
    } else {
      // We've received an ack for a message that has already been acked
      // (or was never sent). This is possible if a message was sent twice
      // before the first ack arrived, so just ignore.
    }
    timeouts_.Erase(ack.msgid);
  }
}

void PublisherWorkerData::ReceiveGoodbye(StreamReceiveArg<MessageGoodbye> arg) {
  thread_check_.Check();

  LOG_WARN(publisher_->info_log_,
           "Received goodbye message on stream (%" PRIu64 ")",
           arg.stream_id);

  pilot_stream_.reset();

  // Notify about failed publishes.
  for (auto& entry : messages_sent_) {
    if (entry.second.callback) {
      std::unique_ptr<ClientResultStatus> result_status(
          new ClientResultStatus(Status::InternalError("Disconnected"),
                                 std::move(entry.second.data),
                                 0));
      entry.second.callback(std::move(result_status));
    }
  }
  messages_sent_.clear();
  timeouts_.Clear();
}

void PublisherWorkerData::CheckTimeouts() {
  timeouts_.ProcessExpired(
    publish_timeout_,
    [&] (MsgId msg_id) {
      auto it = messages_sent_.find(msg_id);
      RS_ASSERT(it != messages_sent_.end());
      if (it != messages_sent_.end() && it->second.callback) {
        std::unique_ptr<ClientResultStatus> result_status(
            new ClientResultStatus(Status::TimedOut(),
                                   std::move(it->second.data),
                                   0));
        it->second.callback(std::move(result_status));
        messages_sent_.erase(it);
      }
    },
    -1 /* no batch limit */);
}

////////////////////////////////////////////////////////////////////////////////
PublisherImpl::PublisherImpl(const ClientOptions& options_,
                             MsgLoop* msg_loop,
                             SmartWakeLock* wake_lock)
: publisher_(options_.publisher)
, info_log_(options_.info_log)
, msg_loop_(msg_loop)
, wake_lock_(wake_lock) {
  using namespace std::placeholders;

  // clang complains the private member wake_lock_ is unused, but we will
  // use it in the future. This silences the warning.
  (void)wake_lock_;

  for (int i = 0; i < msg_loop_->GetNumWorkers(); ++i) {
    worker_data_.emplace_back(std::unique_ptr<PublisherWorkerData>(
        new PublisherWorkerData(options_, this, msg_loop_->GetEventLoop(i))));
  }
}

PublisherImpl::~PublisherImpl() {
  // Destructor cannot be inline, otherwise we have to define worker data in
  // header file and pull include dependencies. It sounds like a reasonable
  // tradeoff.
}

PublishStatus PublisherImpl::Publish(TenantID tenant_id,
                                     const NamespaceID& namespace_id,
                                     const Topic& topic_name,
                                     const TopicOptions& options,
                                     const Slice& data,
                                     PublishCallback callback,
                                     const MsgId message_id) {
  // Find the worker ID for this topic.
  const auto worker_id = GetWorkerForTopic(topic_name);

  // Construct message.
  std::unique_ptr<MessageData> message(
    new MessageData(MessageType::mPublish,
                    tenant_id,
                    topic_name,
                    namespace_id,
                    data.ToString()));

  // Take note of message ID before we move into the command.
  const MsgId empty_msgid = MsgId();
  if (!(message_id == empty_msgid)) {
    message->SetMessageId(message_id);
  } else {
    message->SetMessageId(
        GUIDGenerator::ThreadLocalGUIDGenerator()->Generate());
  }
  const MsgId msgid = message->GetMessageId();

  // Schedule command to publish the message.
  Status st = msg_loop_->SendCommand(
      std::unique_ptr<ExecuteCommand>(MakeExecuteCommand(
          [this, worker_id, msg = std::move(message),
           callback = std::move(callback)]() mutable {
            worker_data_[worker_id]->Publish(
                std::move(msg), std::move(callback));
          })),
      worker_id);

  // Return status with the generated message ID.
  return PublishStatus(st, msgid);
}

int PublisherImpl::GetWorkerForTopic(const Topic& name) const {
  return static_cast<int>(MurmurHash2<std::string>()(name) %
                          msg_loop_->GetNumWorkers());
}

}  // namespace rocketspeed
