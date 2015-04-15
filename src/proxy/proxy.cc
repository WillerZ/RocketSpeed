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

#include "src/messages/stream_socket.h"
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

/** Represents per message loop worker data. */
struct alignas(CACHE_LINE_SIZE) ProxyWorkerData {
  ProxyWorkerData(StreamAllocator allocator)
      : open_streams_(std::move(allocator)) {}

  ProxyWorkerData(const ProxyWorkerData&) = delete;
  ProxyWorkerData& operator=(const ProxyWorkerData&) = delete;

  /** The data can only be accessed from a single and the same thread. */
  ThreadCheck thread_check_;
  /** Stores ordering processor per session. */
  std::unordered_map<int64_t, SessionProcessor> session_processors_;
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

  msg_loop_.reset(new MsgLoop(env_,
                              options.env_options,
                              0,  // port
                              options.num_workers,
                              info_log_,
                              "proxy"));

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
                    Proxy::OnDisconnectCallback on_disconnect) {
  on_message_ = std::move(on_message);
  on_disconnect_ = on_disconnect ? std::move(on_disconnect)
                                 : [](const std::vector<int64_t>&) {};

  msg_thread_ = env_->StartThread([this] () { msg_loop_->Run(); },
                                  "proxy");

  return msg_loop_->WaitUntilRunning();
}

Status Proxy::Forward(std::string data, int64_t session, int32_t sequence) {
  int worker_id = WorkerForSession(session);
  StreamID local;
  {  // Deserialize origin of the message.
    Slice in(data.data(), data.size());
    Status st = DecodeOrigin(&in, &local);
    if (!st.ok()) {
      return st;
    }
    data.erase(0, data.size() - in.size());
  }
  std::unique_ptr<Command> command(
      new ExecuteCommand(std::bind(&Proxy::HandleMessageForwarded,
                                   this,
                                   std::move(data),
                                   session,
                                   sequence,
                                   local)));
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
               "Proxy received goodbye on non-existent stream (%llu)",
               origin);
      return;
    }

    // If that was the last stream on the session, remove the session.
    if (status == decltype(status)::kRemovedLast) {
      data.session_processors_.erase(session);
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

  // Remove session processor.
  data.session_processors_.erase(session);
  // Remove all streams for the session.
  auto removed = data.open_streams_.RemoveContext(session);

  // Send goodbye to all removed streams.
  MessageGoodbye goodbye(Tenant::GuestTenant,
                         MessageGoodbye::Code::Graceful,
                         MessageGoodbye::OriginType::Client);
  for (const auto& pair : removed) {
    StreamID global = pair.second;
    // Send goodbye message as response, because we don't want to open stream if
    // it wasn't opened before.
    const auto worker = WorkerForSession(session);
    msg_loop_->SendResponse(goodbye, global, worker);
    // OK if above fails. Server will garbage collect client.
  }
}

void Proxy::HandleMessageReceived(std::unique_ptr<Message> msg,
                                  StreamID origin) {
  if (!on_message_) {
    return;
  }

  LOG_INFO(info_log_,
           "Received message from RocketSpeed, type %d",
           static_cast<int>(msg->GetMessageType()));

  const auto& data = *worker_data_[msg_loop_->GetThreadWorkerIndex()];
  data.thread_check_.Check();

  // Find corresponding session and translate stream ID back.
  int64_t session;
  StreamID local;
  bool found = data.open_streams_.FindLocalAndContext(origin, &session, &local);
  if (!found) {
    LOG_ERROR(info_log_,
              "Could not find session for global stream ID (%llu)",
              origin);
    stats_.bad_origins->Add(1);
    return;
  }

  // TODO(pja) 1 : ideally we wouldn't reserialize here.
  std::string serial;
  msg->SerializeToString(&serial);
  serial = EncodeOrigin(local).append(serial);
  // Deliver message.
  on_message_(session, std::move(serial));
  stats_.on_message_calls->Add(1);
}

void Proxy::HandleMessageForwarded(std::string msg,
                                   int64_t session,
                                   int32_t sequence,
                                   StreamID local) {
  stats_.forwards->Add(1);

  // TODO(pja) 1 : Really inefficient. Only need to deserialize header,
  // not entire message, and don't need to copy entire message.
  std::unique_ptr<char[]> buffer = Slice(msg).ToUniqueChars();
  std::unique_ptr<Message> message =
      Message::CreateNewInstance(std::move(buffer), msg.size());

  // Find message type.
  if (!message) {
    LOG_ERROR(info_log_, "Failed to deserialize message forwarded to proxy.");
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
                session,
                static_cast<int>(message->GetMessageType()));
      stats_.forward_errors->Add(1);
      // Kill session.
      HandleDestroySession(session);
      on_disconnect_({session});
      return;
  }

  if (sequence == -1) {
    HandleMessageForwardedInorder(
        message->GetMessageType(), std::move(msg), session, local);
  } else {
    auto& data = GetWorkerDataForSession(session);
    // Handle reordering.
    auto it = data.session_processors_.find(session);
    if (it == data.session_processors_.end()) {
      // Not there, so create it.
      SessionProcessor processor(
          ordering_buffer_size_,
          [this, session, &data](SessionProcessor::EventType event) {
            // It's safe to capture data reference.
            // Need to check if session is still there. Previous command
            // processed may have caused it to drop.
            if (data.session_processors_.find(session) ==
                data.session_processors_.end()) {
              return;
            }
            HandleMessageForwardedInorder(
                event.type, std::move(event.message), session, event.local);
          });

      auto result =
          data.session_processors_.emplace(session, std::move(processor));
      assert(result.second);
      it = result.first;
    }

    Status st = it->second.Process(
        {message->GetMessageType(), std::move(msg), local}, sequence);
    if (!st.ok()) {
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
      case MessageType::mPublish:
        host = config_->GetPilotHostIds().front();
        break;
      case MessageType::mMetadata:
        host = config_->GetCopilotHostIds().front();
        break;
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
