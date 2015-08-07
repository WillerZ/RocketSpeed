// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "publisher.h"

#include <memory>
#include <unordered_map>

#include "external/folly/move_wrapper.h"

#include "src/client/smart_wake_lock.h"
#include "src/messages/messages.h"
#include "src/messages/msg_loop_base.h"
#include "src/messages/commands.h"
#include "src/port/port.h"
#include "src/util/common/guid_generator.h"
#include "src/util/common/hash.h"
#include "src/util/common/thread_check.h"

namespace rocketspeed {

/** Publisher uses this class to tell user the status of publish request. */
class ClientResultStatus : public ResultStatus {
 public:
  ClientResultStatus(Status status,
                     std::string serialized_message,
                     SequenceNumber seqno)
      : status_(status)
      , serialized_(std::move(serialized_message))
      , seqno_(seqno) {
    Slice in(serialized_);
    if (!message_.DeSerialize(&in).ok()) {
      // Failed to deserialize a message after it has been serialized?
      assert(false);
      status_ = Status::InternalError("Message corrupt.");
    }
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
  MessageData message_;
  std::string serialized_;
  SequenceNumber seqno_;
};

////////////////////////////////////////////////////////////////////////////////
/** Describes published message awaiting response. */
class PendingAck {
 public:
  PendingAck(PublishCallback _callback, std::string _data)
      : callback(std::move(_callback)), data(std::move(_data)) {}

  PublishCallback callback;
  std::string data;
};

/** State of a single publisher, aligned to avoid false sharing. */
class alignas(CACHE_LINE_SIZE) PublisherWorkerData {
 public:
  // Noncopyable
  PublisherWorkerData(const PublisherWorkerData&) = delete;
  PublisherWorkerData& operator=(const PublisherWorkerData&) = delete;
  // Movable
  PublisherWorkerData(PublisherWorkerData&&) = default;
  PublisherWorkerData& operator=(PublisherWorkerData&&) = default;

  PublisherWorkerData(PublisherImpl* publisher, int worker_id)
      : publisher_(publisher)
      , worker_id_(worker_id)
      , pilot_socket_valid_(false) {}

  /** Publishes message to the Pilot. */
  void Publish(MsgId message_id,
               std::string serialized,
               PublishCallback callback);

  /** Handles acknowledgements for published messages. */
  void ProcessDataAck(std::unique_ptr<Message> msg, StreamID origin);

  /** Handles goodbye messages for publisher streams. */
  void ProcessGoodbye(std::unique_ptr<Message> msg, StreamID origin);

 private:
  ThreadCheck thread_check_;
  PublisherImpl* const publisher_;
  /** Index of this worker. */
  const int worker_id_;
  /** Stream socket used by this worker to talk to the pilot. */
  StreamSocket pilot_socket_;
  /** Determines whether pilot socket is valid. */
  bool pilot_socket_valid_;
  /** Messages sent, awaiting ack. Maps message ID -> pre-serialized message. */
  std::unordered_map<MsgId, PendingAck, MsgId::Hash> messages_sent_;

  bool ExpectsMessage(StreamID origin);
};

void PublisherWorkerData::Publish(MsgId message_id,
                                  std::string serialized,
                                  PublishCallback callback) {
  thread_check_.Check();

  // Check if we have a valid socket to the Pilot, recreate it if not.
  if (!pilot_socket_valid_) {
    // Get the pilot's address.
    HostId pilot;
    Status st = publisher_->config_->GetPilot(&pilot);
    if (!st.ok()) {
      LOG_WARN(publisher_->info_log_,
               "Failed to obtain Pilot address: %s",
               st.ToString().c_str());
      // We'll try to obtain the address and reconnect on next occasion.
      // TODO(stupaq) make above comment valid.
      std::unique_ptr<ClientResultStatus> result_status(
        new ClientResultStatus(
          Status::IOError("No available RocketSpeed hosts"),
          std::move(serialized), 0));
      callback(std::move(result_status));
      return;
    }

    // And create socket to it.
    pilot_socket_ =
        publisher_->msg_loop_->CreateOutboundStream(pilot, worker_id_);
    pilot_socket_valid_ = true;

    LOG_INFO(publisher_->info_log_,
             "Reconnected to %s on stream %llu",
             pilot.ToString().c_str(),
             pilot_socket_.GetStreamID());
  }

  // Send to event loop for processing (the loop will free it).
  Status st = publisher_->msg_loop_->SendCommand(
      SerializedSendCommand::Request(serialized, {&pilot_socket_}), worker_id_);
  if (!st.ok()) {
    std::unique_ptr<ClientResultStatus> result_status(
        new ClientResultStatus(Status::NoBuffer(), std::move(serialized), 0));
    callback(std::move(result_status));
    return;
  }

  // Add message to the sent list.
  auto emplace_result = messages_sent_.emplace(
      message_id, PendingAck(std::move(callback), std::move(serialized)));
  ((void)emplace_result);
  assert(emplace_result.second);
}

void PublisherWorkerData::ProcessDataAck(std::unique_ptr<Message> msg,
                                         StreamID origin) {
  thread_check_.Check();

  if (!ExpectsMessage(origin)) {
    return;
  }

  const MessageDataAck* ackMsg = static_cast<const MessageDataAck*>(msg.get());

  // For each ack'd message, if it was waiting for an ack then remove it
  // from the waiting list and let the application know about the ack.
  for (const auto& ack : ackMsg->GetAcks()) {
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
          st = Status::IOError("Publish failed");
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
  }
}

void PublisherWorkerData::ProcessGoodbye(std::unique_ptr<Message> msg,
                                         StreamID origin) {
  thread_check_.Check();

  if (!ExpectsMessage(origin)) {
    return;
  }

  LOG_WARN(publisher_->info_log_,
           "Received goodbye message on stream (%llu)",
           origin);

  pilot_socket_valid_ = false;

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
}

bool PublisherWorkerData::ExpectsMessage(StreamID origin) {
  if (!pilot_socket_valid_) {
    LOG_WARN(publisher_->info_log_,
             "Unexpected message on stream: (%llu), no valid stream",
             origin);
    return false;
  }
  if (pilot_socket_.GetStreamID() != origin) {
    LOG_WARN(publisher_->info_log_,
             "Unexpected message on stream: (%llu), expected: (%llu)",
             origin,
             pilot_socket_.GetStreamID());
    return false;
  }
  return true;
}

////////////////////////////////////////////////////////////////////////////////
PublisherImpl::PublisherImpl(BaseEnv* env,
                             std::shared_ptr<Configuration> config,
                             std::shared_ptr<Logger> info_log,
                             MsgLoopBase* msg_loop,
                             SmartWakeLock* wake_lock)
    : config_(std::move(config))
    , info_log_(std::move(info_log))
    , msg_loop_(msg_loop)
    , wake_lock_(wake_lock) {
  using namespace std::placeholders;

  // clang complains the private member wake_lock_ is unused, but we will
  // use it in the future. This silences the warning.
  (void)wake_lock_;

  for (int i = 0; i < msg_loop_->GetNumWorkers(); ++i) {
    worker_data_.emplace_back(this, i);
  }

  // Register our callbacks.
  std::map<MessageType, MsgCallbackType> callbacks;
  callbacks[MessageType::mDataAck] =
      std::bind(&PublisherImpl::ProcessDataAck, this, _1, _2);
  msg_loop_->RegisterCallbacks(std::move(callbacks));
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
  MessageData message(
      MessageType::mPublish, tenant_id, Slice(topic_name), namespace_id, data);

  // Take note of message ID before we move into the command.
  const MsgId empty_msgid = MsgId();
  if (!(message_id == empty_msgid)) {
    message.SetMessageId(message_id);
  } else {
    message.SetMessageId(GUIDGenerator::ThreadLocalGUIDGenerator()->Generate());
  }
  const MsgId msgid = message.GetMessageId();

  std::string serialized;
  message.SerializeToString(&serialized);

  // Schedule command to publish the message.
  auto moved_serialized = folly::makeMoveWrapper(std::move(serialized));
  auto moved_callback = folly::makeMoveWrapper(std::move(callback));
  Status st = msg_loop_->SendCommand(
      std::unique_ptr<ExecuteCommand>(MakeExecuteCommand(
          [this, worker_id, msgid, moved_serialized, moved_callback]() mutable {
            worker_data_[worker_id].Publish(
                msgid, moved_serialized.move(), moved_callback.move());
          })),
      worker_id);

  // Return status with the generated message ID.
  return PublishStatus(st, msgid);
}

// TODO(stupaq) remove these once we get thread-unsafe outgoing loops
void PublisherImpl::ProcessDataAck(std::unique_ptr<Message> msg,
                                   StreamID origin) {
  const auto worker_id = msg_loop_->GetThreadWorkerIndex();
  worker_data_[worker_id].ProcessDataAck(std::move(msg), origin);
}

void PublisherImpl::ProcessGoodbye(std::unique_ptr<Message> msg,
                                   StreamID origin) {
  const auto worker_id = msg_loop_->GetThreadWorkerIndex();
  worker_data_[worker_id].ProcessGoodbye(std::move(msg), origin);
}

int PublisherImpl::GetWorkerForTopic(const Topic& name) const {
  return static_cast<int>(MurmurHash2<std::string>()(name) %
                          msg_loop_->GetNumWorkers());
}

}  // namespace rocketspeed
