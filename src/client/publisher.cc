// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/client/publisher.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "src/client/message_received.h"
#include "src/client/smart_wake_lock.h"
#include "src/client/topic_id.h"
#include "src/messages/msg_loop_base.h"
#include "src/port/port.h"
#include "src/util/common/hash.h"

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

  virtual Status GetStatus() const {
    return status_;
  }

  virtual MsgId GetMessageId() const {
    assert(status_.ok());
    return message_.GetMessageId();
  }

  virtual SequenceNumber GetSequenceNumber() const {
    // Sequence number comes from the ack, not the original message.
    assert(status_.ok());
    return seqno_;
  }

  virtual Slice GetTopicName() const {
    assert(status_.ok());
    return message_.GetTopicName();
  }

  virtual Slice GetNamespaceId() const {
    assert(status_.ok());
    return message_.GetNamespaceId();
  }

  virtual Slice GetContents() const {
    assert(status_.ok());
    return message_.GetPayload();
  }

  ~ClientResultStatus() {
  }

 private:
  Status status_;
  MessageData message_;
  std::string serialized_;
  SequenceNumber seqno_;
};

/** Describes published message awaiting response. */
class PendingAck {
 public:
  PendingAck(PublishCallback _callback, std::string _data)
      : callback(std::move(_callback)), data(std::move(_data)) {
  }

  PublishCallback callback;
  std::string data;
};

/**
 * State of a publisher. We have one such structure per worker thread, all
 * messages on a single topic are handled by a single thread to avoid
 * contention. Aligned to avoid false sharing.
 */
class alignas(CACHE_LINE_SIZE) PublisherWorkerData {
 public:
  /** Stream socket used by this worker to talk to the pilot. */
  StreamSocket pilot_socket;

  /**
   * Map a subscribed topic name to the last sequence number received for this
   * topic.
   */
  std::unordered_map<TopicID, SequenceNumber> topic_map;

  /** Messages sent, awaiting ack. Maps message ID -> pre-serialized message. */
  std::unordered_map<MsgId, PendingAck, MsgId::Hash> messages_sent;
  std::mutex message_sent_mutex;  // mutex for operators on messages_sent_
};

PublisherImpl::PublisherImpl(BaseEnv* env,
                             std::shared_ptr<Logger> info_log,
                             MsgLoopBase* msg_loop,
                             SmartWakeLock* wake_lock,
                             HostId pilot_host)
    : info_log_(std::move(info_log))
    , msg_loop_(msg_loop)
    , wake_lock_(wake_lock)
    , pilot_host_(std::move(pilot_host)) {
  // Prepare sharded state.
  worker_data_.reset(new PublisherWorkerData[msg_loop_->GetNumWorkers()]);

  // Initialise stream socket for each worker, each of them is independent.
  for (int i = 0; i < msg_loop_->GetNumWorkers(); ++i) {
    worker_data_[i].pilot_socket =
        msg_loop_->CreateOutboundStream(pilot_host_.ToClientId(), i);
  }

  // Register our callbacks.
  std::map<MessageType, MsgCallbackType> callbacks;
  callbacks[MessageType::mDataAck] =
      std::bind(&PublisherImpl::ProcessDataAck, this, std::placeholders::_1);
  msg_loop_->RegisterCallbacks(std::move(callbacks));
}

PublisherImpl::~PublisherImpl() {
  // Destructor cannot be inline, otherwise we have to define worker data in
  // header file and pull include dependencies. It sounds like a reasonable
  // tradeoff.
}

PublishStatus PublisherImpl::Publish(TenantID tenant_id,
                                     const NamespaceID& namespace_id,
                                     const Topic& name,
                                     const TopicOptions& options,
                                     const Slice& data,
                                     PublishCallback callback,
                                     const MsgId message_id) {
  // Find the worker ID for this topic.
  int worker_id = GetWorkerForTopic(name);
  auto& worker_data = worker_data_[worker_id];

  // Construct message.
  MessageData message(MessageType::mPublish,
                      tenant_id,
                      Slice(name),
                      namespace_id,
                      data);

  // Take note of message ID before we move into the command.
  const MsgId empty_msgid = MsgId();
  if (!(message_id == empty_msgid)) {
    message.SetMessageId(message_id);
  }
  const MsgId msgid = message.GetMessageId();

  // Get a serialized version of the message
  // TODO(pja) 1 : Figure out what to do with shared strings.
  std::string serialized;
  message.SerializeToString(&serialized);

  // Add message to the sent list.
  PendingAck pending_ack(std::move(callback), std::move(serialized));
  std::unique_lock<std::mutex> lock(worker_data.message_sent_mutex);
  bool added =
      worker_data.messages_sent.emplace(msgid, std::move(pending_ack)).second;
  lock.unlock();

  assert(added);

  // Send to event loop for processing (the loop will free it).
  wake_lock_->AcquireForSending();
  Status status =
      msg_loop_->SendRequest(message, &worker_data.pilot_socket, worker_id);
  if (!status.ok() && added) {
    std::unique_lock<std::mutex> lock1(worker_data.message_sent_mutex);
    worker_data.messages_sent.erase(msgid);
    lock1.unlock();
  }

  // Return status with the generated message ID.
  return PublishStatus(status, msgid);
}

void PublisherImpl::ProcessDataAck(std::unique_ptr<Message> msg) {
  wake_lock_->AcquireForReceiving();
  msg_loop_->ThreadCheck();

  int worker_id = msg_loop_->GetThreadWorkerIndex();
  auto& worker_data = worker_data_[worker_id];

  // Check that message arrived on correct stream.
  if (worker_data.pilot_socket.GetStreamID() != msg->GetOrigin()) {
    LOG_WARN(info_log_,
             "Incorrect message stream: (%s) expected: (%s)",
             msg->GetOrigin().c_str(),
             worker_data.pilot_socket.GetStreamID().c_str());
    return;
  }

  const MessageDataAck* ackMsg = static_cast<const MessageDataAck*>(msg.get());

  // For each ack'd message, if it was waiting for an ack then remove it
  // from the waiting list and let the application know about the ack.
  for (const auto& ack : ackMsg->GetAcks()) {
    // Find the message ID from the messages_sent list.
    std::unique_lock<std::mutex> lock1(worker_data.message_sent_mutex);
    auto it = worker_data.messages_sent.find(ack.msgid);
    bool successful_ack = it != worker_data.messages_sent.end();
    lock1.unlock();

    // If successful, invoke callback.
    if (successful_ack) {
      if (it->second.callback) {
        Status st;
        SequenceNumber seqno = 0;
        if (ack.status == MessageDataAck::AckStatus::Success) {
          st = Status::OK();
          seqno = ack.seqno;
        } else {
          st = Status::IOError("Publish failed");
        }

        std::unique_ptr<ClientResultStatus> result_status(
            new ClientResultStatus(st, std::move(it->second.data), seqno));
        it->second.callback(std::move(result_status));
      }

      // Remove sent message from list.
      std::unique_lock<std::mutex> lock2(worker_data.message_sent_mutex);
      worker_data.messages_sent.erase(it);
      lock2.unlock();
    } else {
      // We've received an ack for a message that has already been acked
      // (or was never sent). This is possible if a message was sent twice
      // before the first ack arrived, so just ignore.
    }
  }
}

int PublisherImpl::GetWorkerForTopic(const Topic& name) const {
  return static_cast<int>(MurmurHash2<std::string>()(name) %
                          msg_loop_->GetNumWorkers());
}

}  // namespace rocketspeed
