// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS
#include "src/proxy/proxy.h"

#include <climits>
#include <functional>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "src/util/common/ordered_processor.h"
#include "src/util/worker_loop.h"

namespace rocketspeed {

/**
 * Bidirectional map between hosts and sessions, i.e. what pilots and
 * copilots a session is communicating with.
 * This is used to inform clients when the host goes down.
 */
struct HostSessionMatrix {
  /**
   * Add a (session, host) into the matrix. Has no effect if the pair
   * has already been inserted.
   *
   * @param session The session communicating with a host.
   * @param host The host the session is communicating with.
   */
  void Add(int64_t session, const ClientID& host) {
    host_to_sessions_[host].insert(session);
    auto& hosts = session_to_hosts_[session];
    if (std::find(hosts.begin(), hosts.end(), host) == hosts.end()) {
      hosts.push_back(host);
    }
  }

  /**
   * Remove a host column from matrix.
   *
   * @param host The host to remove.
   * @return The sessions for that host.
   */
  std::unordered_set<int64_t> RemoveHost(const ClientID& host) {
    auto sessions = std::move(host_to_sessions_[host]);
    host_to_sessions_.erase(host);
    for (int64_t session : sessions) {
      auto& hosts = session_to_hosts_[session];
      auto it = std::find(hosts.begin(), hosts.end(), host);
      if (it != hosts.end()) {
        hosts.erase(it);
      }
    }
    return sessions;
  }

  /**
   * Remove a session row from matrix.
   *
   * @param session The session to remove.
   * @return The hosts for the removed session.
   */
  std::vector<ClientID> RemoveSession(int64_t session) {
    auto hosts = std::move(session_to_hosts_[session]);
    for (const ClientID& host : hosts) {
      host_to_sessions_[host].erase(session);
    }
    session_to_hosts_.erase(session);
    return hosts;
  }

 private:
  // Maps host to sessions communicating with this host.
  std::unordered_map<ClientID, std::unordered_set<int64_t>> host_to_sessions_;

  // Maps sessions to the hosts they are communicating with.
  // Each session should only communicate with up to two hosts (pilot, copilot).
  std::unordered_map<int64_t, std::vector<ClientID>> session_to_hosts_;
};

typedef OrderedProcessor<std::unique_ptr<Command>> SessionProcessor;

/** Represents per message loop worker data. */
struct alignas(CACHE_LINE_SIZE) ProxyWorkerData {
  ProxyWorkerData() = default;
  ProxyWorkerData(const ProxyWorkerData&) = delete;
  ProxyWorkerData& operator=(const ProxyWorkerData&) = delete;

  /** The data can only be accessed from a single and the same thread. */
  ThreadCheck thread_check_;

  /**
   * Mapping from internal client IDs, which happen to be sessions, into
   * client IDs presented by the clients connected to the proxy.
   */
  std::unordered_map<int64_t, ClientID> session_to_client_;

  std::unordered_map<int64_t, SessionProcessor> sessions_;

  HostSessionMatrix host_session_matrix_;
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

  msg_loop_.reset(new MsgLoop(env_,
                              options.env_options,
                              0,  // port
                              options.num_workers,
                              info_log_,
                              "proxy"));

  auto callback = std::bind(&Proxy::HandleMessageReceived, this, _1);
  auto goodbye_callback = std::bind(&Proxy::HandleGoodbyeMessage, this, _1);

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

  worker_data_.reset(new ProxyWorkerData[options.num_workers]);
}

Status Proxy::Start(OnMessageCallback on_message,
                    OnDisconnectCallback on_disconnect) {
  on_message_ = std::move(on_message);
  on_disconnect_ = on_disconnect ? std::move(on_disconnect)
                                 : [](const std::vector<int64_t>&) {};

  msg_thread_ = env_->StartThread([this] () { msg_loop_->Run(); },
                                  "proxy");

  return msg_loop_->WaitUntilRunning();
}

Status Proxy::Forward(std::string msg, int64_t session, int32_t sequence) {
  int worker_id = WorkerForSession(session);
  std::unique_ptr<Command> command(
      new ExecuteCommand(std::bind(&Proxy::HandleMessageForwarded,
                                   this,
                                   std::move(msg),
                                   session,
                                   sequence)));
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
  worker_data_[worker_id].thread_check_.Check();
  return worker_data_[worker_id];
}

void Proxy::HandleGoodbyeMessage(std::unique_ptr<Message> msg) {
  MessageGoodbye* goodbye = static_cast<MessageGoodbye*>(msg.get());
  if (goodbye->GetOriginType() == MessageGoodbye::OriginType::Server) {
    LOG_WARN(info_log_,
             "Received goodbye from server %s.",
             goodbye->GetOrigin().c_str());
    // Arbitrary subset of sessions might have been mapped to this host, we have
    // to fan out to all workers.
    for (int worker_id = 0; worker_id < msg_loop_->GetNumWorkers();
         ++worker_id) {
      std::unique_ptr<Command> command(new ExecuteCommand(
          std::bind(&Proxy::HandleRemoveHost, this, goodbye->GetOrigin())));
      msg_loop_->SendCommand(std::move(command), worker_id);
      // TODO(stupaq) scary things can happen if we fail to append command to
      // all queues
    }
  } else {
    LOG_WARN(info_log_,
             "Proxy received client goodbye from %s, but has no clients.",
             goodbye->GetOrigin().c_str());
  }
}

void Proxy::HandleDestroySession(int64_t session) {
  auto& data = GetWorkerDataForSession(session);

  // Find session in map.
  auto it = data.sessions_.find(session);
  // Remove session.
  if (it != data.sessions_.end()) {
    data.sessions_.erase(it);
  }
  auto hosts = data.host_session_matrix_.RemoveSession(session);

  // Send goodbye to all hosts.
  MessageGoodbye goodbye(Tenant::GuestTenant,
                         std::to_string(session),
                         MessageGoodbye::Code::Graceful,
                         MessageGoodbye::OriginType::Client);

  // OK if this fails. Server will garbage collect client.
  int worker = WorkerForSession(session);
  for (const ClientID& host : hosts) {
    msg_loop_->SendRequest(goodbye, host, worker);
  }

  // Remove the session to client ID mapping.
  data.session_to_client_.erase(session);
}

void Proxy::HandleRemoveHost(ClientID host) {
  auto& data = worker_data_[msg_loop_->GetThreadWorkerIndex()];
  data.thread_check_.Check();
  // Remove host.
  auto sessions = data.host_session_matrix_.RemoveHost(host);
  std::vector<int64_t> sessions_vec(sessions.begin(), sessions.end());
  on_disconnect_(std::move(sessions_vec));
}

void Proxy::HandleMessageReceived(std::unique_ptr<Message> msg) {
  if (!on_message_) {
    return;
  }

  LOG_INFO(info_log_,
           "Received message from RocketSpeed, type %d",
           static_cast<int>(msg->GetMessageType()));

  // Parse origin as session.
  const char* origin = msg->GetOrigin().c_str();
  int64_t session = strtoll(origin, nullptr, 10);
  // strtoll failure modes are:
  // return 0LL if could not convert.
  // return LLONG_MIN/MAX if out of range, with errno set to ERANGE.
  if ((session == 0 && strcmp(origin, "0")) ||
      (session == LLONG_MIN && errno == ERANGE) ||
      (session == LLONG_MAX && errno == ERANGE)) {
    LOG_ERROR(info_log_,
              "Could not parse message origin '%s' into a session ID.",
              origin);
    stats_.bad_origins->Add(1);
    return;
  }

  // Translate origin back.
  const auto& data = GetWorkerDataForSession(session);
  auto it = data.session_to_client_.find(session);
  if (it == data.session_to_client_.end()) {
    LOG_ERROR(info_log_,
              "Could find client ID for session '%" PRIi64 "'.",
              session);
    stats_.bad_origins->Add(1);
    return;
  }
  msg->SetOrigin(it->second);

  // TODO(pja) 1 : ideally we wouldn't reserialize here.
  std::string serial;
  msg->SerializeToString(&serial);
  on_message_(session, std::move(serial));
  stats_.on_message_calls->Add(1);
}

void Proxy::HandleMessageForwarded(std::string msg,
                                   int64_t session,
                                   int32_t sequence) {
  stats_.forwards->Add(1);
  auto& data = GetWorkerDataForSession(session);

  // TODO(pja) 1 : Really inefficient. Only need to deserialize header,
  // not entire message, and don't need to copy entire message.
  std::unique_ptr<char[]> buffer = Slice(msg).ToUniqueChars();
  std::unique_ptr<Message> message =
    Message::CreateNewInstance(std::move(buffer), msg.size());

  {  // Save origin presented by client.
    auto it = data.session_to_client_.find(session);
    // We save unnecessary copy.
    if (it == data.session_to_client_.end()) {
      data.session_to_client_.emplace_hint(it, session, message->GetOrigin());
    }
  }

  // Internally the session is out client ID.
  message->SetOrigin(std::to_string(session));
  message->SerializeToString(&msg);

  if (!message) {
    LOG_ERROR(info_log_,
      "Failed to deserialize message forwarded to proxy.");
    stats_.forward_errors->Add(1);
    on_disconnect_({session});
    return;
  }

  // Select destination based on message type.
  SendCommand::Recipients hosts;
  switch (message->GetMessageType()) {
    case MessageType::mPing:  // could go to either
    case MessageType::mPublish:
      hosts.push_back(config_->GetPilotHostIds().front().ToClientId());
      break;

    case MessageType::mMetadata:
      hosts.push_back(config_->GetCopilotHostIds().front().ToClientId());
      break;

    case MessageType::mGoodbye:
      // Goodbye messages need to be sent to both.
      hosts.push_back(config_->GetPilotHostIds().front().ToClientId());
      hosts.push_back(config_->GetCopilotHostIds().front().ToClientId());
      break;

    case MessageType::mDataAck:
    case MessageType::mGap:
    case MessageType::mDeliver:
    case MessageType::NotInitialized:
    default:
      // Client shouldn't be sending us these kinds of messages.
      LOG_ERROR(info_log_,
        "Client %s attempting to send invalid message type through proxy (%d)",
        message->GetOrigin().c_str(),
        static_cast<int>(message->GetMessageType()));
      stats_.forward_errors->Add(1);
      on_disconnect_({session});
      return;
  }

  // Create command.
  const bool is_new_request = true;
  std::unique_ptr<Command> cmd(
    new SerializedSendCommand(std::move(msg),
                              hosts,
                              is_new_request));

  if (sequence == -1) {
    // Send directly to loop.
    msg_loop_->SendCommandToSelf(std::move(cmd));
  } else {
    // Handle reordering.
    auto it = data.sessions_.find(session);
    if (it == data.sessions_.end()) {
      // Not there, so create it.
      SessionProcessor processor(
          ordering_buffer_size_,
          [this, session, &data](std::unique_ptr<Command> command) {
            // It's safe to capture data reference.
            // Process command by sending it to the event loop.
            // Need to check if session is still there. Previous command
            // processed may have caused it to drop.
            if (data.sessions_.find(session) != data.sessions_.end()) {
              msg_loop_->SendCommandToSelf(std::move(command));
            }
          });

      auto result = data.sessions_.emplace(session, std::move(processor));
      assert(result.second);
      it = result.first;
    }

    for (const ClientID& host : hosts) {
      data.host_session_matrix_.Add(session, host);
    }

    Status st = it->second.Process(std::move(cmd), sequence);
    if (!st.ok()) {
      on_disconnect_({session});
      data.sessions_.erase(it);
      return;
    }
  }
}

}  // namespace rocketspeed
