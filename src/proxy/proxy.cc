// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "src/proxy/proxy.h"

#include <climits>
#include <algorithm>
#include <functional>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

#include "external/folly/move_wrapper.h"

#include "src/messages/msg_loop.h"
#include "src/messages/stream_socket.h"
#include "src/proxy/wrapped_message.h"
#include "src/util/common/autovector.h"
#include "src/util/common/ordered_processor.h"
#include "src/util/worker_loop.h"

namespace rocketspeed {

struct OrderedEventType {
  MessageType type;
  std::string message;
  StreamID local;
};

typedef OrderedProcessor<OrderedEventType> SessionProcessor;

struct SessionInfo {
  explicit SessionInfo(SessionProcessor processor)
      : next_seqno_(0), ordered_processor_(std::move(processor)) {
  }

  /** Next sequence number for messages sent via OnMessageCallback. */
  MessageSequenceNumber next_seqno_;
  /** Ordering processor for messages received via Forward. */
  SessionProcessor ordered_processor_;
};

/** Represents per message loop worker data. */
struct alignas(CACHE_LINE_SIZE) ProxyWorkerData {
  ProxyWorkerData(StreamAllocator allocator)
      : open_streams_(std::move(allocator)) {
  }

  ProxyWorkerData(const ProxyWorkerData&) = delete;
  ProxyWorkerData& operator=(const ProxyWorkerData&) = delete;

  /** The data can only be accessed from a single and the same thread. */
  ThreadCheck thread_check_;
  /** Stores session metdata for all open sessions. */
  std::unordered_map<int64_t, SessionInfo> open_sessions_;
  /** Stores map: (session, session local stream ID) <-> global stream ID. */
  UniqueStreamMap<int64_t> open_streams_;
};

Status Proxy::CreateNewInstance(ProxyOptions options,
                                std::unique_ptr<Proxy>* proxy) {
  // Sanitize / Validate options.
  if (options.info_log == nullptr) {
    options.info_log = std::make_shared<NullLogger>();
  }

  if (options.conf == nullptr) {
    return Status::InvalidArgument("Configuration required");
  }

  if (options.num_workers <= 0) {
    return Status::InvalidArgument("Invalid number of workers");
  }

  // Create the proxy object.
  proxy->reset(new Proxy(std::move(options)));
  return Status::OK();
}

Proxy::Proxy(ProxyOptions options)
    : info_log_(std::move(options.info_log))
    , env_(options.env)
    , config_(std::move(options.conf))
    , ordering_buffer_size_(options.ordering_buffer_size)
    , msg_thread_(0) {
  using std::placeholders::_1;
  using std::placeholders::_2;

  msg_loop_.reset(new MsgLoop(env_, options.env_options,
                              0,  // port
                              options.num_workers, info_log_, "proxy"));

  auto callback = std::bind(&Proxy::HandleMessageReceived, this, _1, _2);
  auto goodbye_callback = std::bind(&Proxy::HandleGoodbyeMessage, this, _1, _2);

  // Use same callback for all server-generated messages.
  std::map<MessageType, MsgCallbackType> callbacks;
  callbacks[MessageType::mMetadata] = callback;
  callbacks[MessageType::mDataAck] = callback;
  callbacks[MessageType::mGap] = callback;
  callbacks[MessageType::mDeliver] = callback;
  callbacks[MessageType::mPing] = callback;

  // Except goodbye. Goodbye needs to be handled separately.
  callbacks[MessageType::mGoodbye] = goodbye_callback;
  msg_loop_->RegisterCallbacks(callbacks);

  // We own the message loop, so we can just steal stream ID space for outbound
  // streams from it.
  for (int i = 0; i < msg_loop_->GetNumWorkers(); ++i) {
    worker_data_.emplace_back(new ProxyWorkerData(
        std::move(*msg_loop_->GetOutboundStreamAllocator(i))));
  }
}

Status Proxy::Start(OnMessageCallback on_message,
                    OnDisconnectCallback on_disconnect) {
  on_message_ = std::move(on_message);
  on_disconnect_ = on_disconnect ? std::move(on_disconnect)
                                 : [](const std::vector<int64_t>&) {};

  msg_thread_ = env_->StartThread([this]() { msg_loop_->Run(); }, "proxy");

  return msg_loop_->WaitUntilRunning();
}

Status Proxy::Forward(std::string data, int64_t session) {
  // Deserialize metadata.
  StreamID origin = 0;
  MessageSequenceNumber sequence = 0;
  std::string msg;
  Status st = UnwrapMessage(std::move(data), &msg, &origin, &sequence);
  if (!st.ok()) {
    LOG_ERROR(info_log_,
              "Faled unwrapping message on session %" PRIi64
              ", speculative stream (%llu) and seqno %d. Problems: %s",
              session,
              origin,
              sequence,
              st.ToString().c_str());
    return st;
  }
  // Forward message to responsible worker.
  int worker_id = WorkerForSession(session);
  auto moved_msg = folly::makeMoveWrapper(std::move(msg));
  std::unique_ptr<Command> command(new ExecuteCommand(
      [this, moved_msg, session, sequence, origin]() mutable {
        HandleMessageForwarded(moved_msg.move(), session, sequence, origin);
      }));
  return msg_loop_->SendCommand(std::move(command), worker_id);
}

void Proxy::DestroySession(int64_t session) {
  int worker_id = WorkerForSession(session);
  std::unique_ptr<Command> command(new ExecuteCommand(
      std::bind(&Proxy::HandleDestroySession, this, session)));
  auto st = msg_loop_->SendCommand(std::move(command), worker_id);
  if (!st.ok()) {
    LOG_ERROR(info_log_,
              "Could not schedule session deletion: %s, leaking resources.",
              st.ToString().c_str());
    // Inactive clients will be removed by GC mechanism, Proxy will receive
    // Goodbye message for each stream this session opened, and after closing
    // the last stream, the session will be disposed as well (see
    // Proxy::HandleGoodbyeMessage).
  }
}

Proxy::~Proxy() {
  if (msg_loop_->IsRunning()) {
    msg_loop_->Stop();
    env_->WaitForJoin(msg_thread_);
  }
}

int Proxy::WorkerForSession(int64_t session) const {
  return static_cast<int>(session % msg_loop_->GetNumWorkers());
}

ProxyWorkerData& Proxy::GetWorkerDataForSession(int64_t session) {
  const auto worker_id = WorkerForSession(session);
  // This way we do not reach into the thread local in production code.
  assert(worker_id == msg_loop_->GetThreadWorkerIndex());
  worker_data_[worker_id]->thread_check_.Check();
  return *worker_data_[worker_id];
}

void Proxy::HandleGoodbyeMessage(std::unique_ptr<Message> msg,
                                 StreamID origin) {
  MessageGoodbye* goodbye = static_cast<MessageGoodbye*>(msg.get());
  if (goodbye->GetOriginType() == MessageGoodbye::OriginType::Server) {
    LOG_INFO(info_log_, "Received goodbye for stream (%llu).", origin);

    auto& data = *worker_data_[msg_loop_->GetThreadWorkerIndex()];
    data.thread_check_.Check();
    // Remove entry from streams map.
    int64_t session;
    StreamID local;
    const auto status =
        data.open_streams_.RemoveGlobal(origin, &session, &local);
    if (status == decltype(status)::kNotRemoved) {
      // Session might have been closed while connection to pilot/copilot died.
      LOG_INFO(info_log_,
               "Proxy received goodbye on non-existent stream (%llu)", origin);
      return;
    }

    // If that was the last stream on the session, remove the session.
    if (status == decltype(status)::kRemovedLast) {
      LOG_INFO(info_log_,
               "Removed last stream on session %" PRIi64 ", closing session",
               session);
      data.open_sessions_.erase(session);
      on_disconnect_({session});
    }
  } else {
    LOG_WARN(info_log_,
             "Proxy received client goodbye from %llu, but has no clients.",
             origin);
  }
}

void Proxy::HandleDestroySession(int64_t session) {
  auto& data = GetWorkerDataForSession(session);
  data.thread_check_.Check();

  // Remove session information.
  data.open_sessions_.erase(session);
  // Remove all streams for the session.
  auto removed = data.open_streams_.RemoveContext(session);

  // Prepare list of streams that we will be sending goodbye to.
  SendCommand::StreamList recipients;
  for (const auto& pair : removed) {
    recipients.push_back(pair.second);
  }

  // Prepare goodbye message.
  MessageGoodbye goodbye(Tenant::GuestTenant, MessageGoodbye::Code::Graceful,
                         MessageGoodbye::OriginType::Client);
  std::string serialized;
  goodbye.SerializeToString(&serialized);

  // Send goodbye to all removed streams as a response, because we don't want to
  // open stream if it wasn't opened before.
  msg_loop_->SendCommandToSelf(SerializedSendCommand::Response(
      std::move(serialized), std::move(recipients)));
}

void Proxy::HandleMessageReceived(std::unique_ptr<Message> msg,
                                  StreamID global) {
  if (!on_message_) {
    return;
  }

  LOG_INFO(info_log_, "Received message from RocketSpeed, type %d",
           static_cast<int>(msg->GetMessageType()));

  auto& data = *worker_data_[msg_loop_->GetThreadWorkerIndex()];
  data.thread_check_.Check();

  // Find corresponding session and translate stream ID back, drop if stream was
  // not open.
  int64_t session;
  StreamID local;
  bool found = data.open_streams_.FindLocalAndContext(global, &session, &local);
  if (!found) {
    LOG_ERROR(info_log_,
              "Could not find session for global stream ID (%llu)",
              global);
    stats_.bad_origins->Add(1);
    return;
  }

  MessageSequenceNumber seqno;
  {  // Assign sequence number, drop if session is not open.
    auto it = data.open_sessions_.find(session);
    if (it == data.open_sessions_.end()) {
      LOG_ERROR(info_log_,
                "Could not find open session %" PRIi64 ", stream (%llu) exists",
                session,
                global);
      stats_.bad_origins->Add(1);
      // This shall never happen.
      assert(false);
      return;
    }
    seqno = it->second.next_seqno_++;
  }

  // Include sequence number and origin stream in the message.
  std::string serialized;
  // TODO(pja) 1 : ideally we wouldn't reserialize here.
  msg->SerializeToString(&serialized);
  serialized = WrapMessage(std::move(serialized), local, seqno);

  // Deliver message.
  on_message_(session, std::move(serialized));
  stats_.on_message_calls->Add(1);
}

void Proxy::HandleMessageForwarded(std::string msg,
                                   int64_t session,
                                   MessageSequenceNumber sequence,
                                   StreamID local) {
  stats_.forwards->Add(1);

  // TODO(pja) 1 : Really inefficient. Only need to deserialize header,
  // not entire message, and don't need to copy entire message.
  std::unique_ptr<char[]> buffer = Slice(msg).ToUniqueChars();
  std::unique_ptr<Message> message =
      Message::CreateNewInstance(std::move(buffer), msg.size());

  // Find message type.
  if (!message) {
    LOG_ERROR(info_log_,
              "Failed deserializing message forwarded to proxy, "
              "session (%" PRIi64 ") seqno (%d) local stream (%llu)",
              session,
              sequence,
              local);
    stats_.forward_errors->Add(1);
    // Kill the session.
    HandleDestroySession(session);
    on_disconnect_({session});
    return;
  }

  // Filter out message by type.
  switch (message->GetMessageType()) {
    case MessageType::mPing:
    case MessageType::mPublish:
    case MessageType::mMetadata:
    case MessageType::mGoodbye:
      break;

    default:
      LOG_ERROR(info_log_,
                "Session %" PRIi64
                " attempting to send invalid message type through proxy (%d)",
                session, static_cast<int>(message->GetMessageType()));
      stats_.forward_errors->Add(1);
      // Kill session.
      HandleDestroySession(session);
      on_disconnect_({session});
      return;
  }

  // Find or create session info.
  auto& data = GetWorkerDataForSession(session);
  // Handle reordering.
  auto it = data.open_sessions_.find(session);
  if (it == data.open_sessions_.end()) {
    // Not there, so create it.
    SessionProcessor processor(
        ordering_buffer_size_,
        [this, session, &data](SessionProcessor::EventType event) {
          // It's safe to capture data reference.
          // Need to check if session is still there. Previous command
          // processed may have caused it to drop.
          if (data.open_sessions_.find(session) == data.open_sessions_.end()) {
            return;
          }
          HandleMessageForwardedInorder(event.type, std::move(event.message),
                                        session, event.local);
        });

    auto result =
        data.open_sessions_.emplace(session, SessionInfo(std::move(processor)));
    assert(result.second);
    it = result.first;
  }

  if (sequence == -1) {
    HandleMessageForwardedInorder(message->GetMessageType(), std::move(msg),
                                  session, local);
  } else {
    Status st = it->second.ordered_processor_.Process(
        {message->GetMessageType(), std::move(msg), local}, sequence);
    if (!st.ok()) {
      LOG_ERROR(info_log_,
                "Failed to insert message (%d) into processor"
                " for session %" PRIi64 ", reason: %s",
                sequence,
                session,
                st.ToString().c_str());
      // Kill the session.
      HandleDestroySession(session);
      on_disconnect_({session});
      return;
    }
  }
}

void Proxy::HandleMessageForwardedInorder(MessageType message_type,
                                          std::string msg,
                                          int64_t session,
                                          StreamID local) {
  auto& data = GetWorkerDataForSession(session);

  // Get unique stream ID for session and local stream ID pair.
  StreamID global;
  auto status = data.open_streams_.GetGlobal(session, local, true, &global);
  assert(status != decltype(status)::kNotInserted);

  // We're cheating a bit here by creating a socket on-the-fly, but the
  // information whether the stream shall be opened is stored in the unique
  // stream map.
  StreamSocket socket(global);
  assert(socket.IsOpen());

  if (status == decltype(status)::kInserted) {
    HostId host;
    // Select destination based on message type.
    switch (message_type) {
      case MessageType::mPing:  // could go to either
      case MessageType::mPublish: {
        Status st = config_->GetPilot(&host);
        if (!st.ok()) {
          LOG_ERROR(info_log_, "Failed to find pilot");
          return;
        }
        break;
      }
      case MessageType::mMetadata: {
        Status st = config_->GetCopilot(&host);
        if (!st.ok()) {
          LOG_ERROR(info_log_, "Failed to find copilot");
          return;
        }
        break;
      }
      default:
        LOG_ERROR(info_log_, "Invalid message type cannot be forwarded.");
        assert(false);
        // Note that we cannot kill session here, as it would remove ordered
        // processor and corrupt memory.
        return;
    }
    socket = StreamSocket(host.ToClientId(), global);
    assert(!socket.IsOpen());
  }

  // Send directly to loop.
  msg_loop_->SendCommandToSelf(
      SerializedSendCommand::Request(std::move(msg), {&socket}));
}

}  // namespace rocketspeed
