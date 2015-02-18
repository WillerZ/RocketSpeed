// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/proxy/proxy.h"
#include <thread>

namespace rocketspeed {

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
, config_(std::move(options.conf)) {
  msg_loop_.reset(new MsgLoop(env_,
                              options.env_options,
                              0,  // port
                              options.num_workers,
                              info_log_,
                              "proxy"));

  // Message callback.
  // Regardless of type, just forward back to the host.
  auto callback = [this] (std::unique_ptr<Message> msg) {
    if (on_message_) {
      LOG_INFO(info_log_,
        "Received message from RocketSpeed, type %d",
        static_cast<int>(msg->GetMessageType()));

      // TODO(pja) 1 : ideally we wouldn't reserialize here.
      std::string serial;
      msg->SerializeToString(&serial);
      on_message_(msg->GetOrigin(), std::move(serial));
      stats_.on_message_calls->Add(1);
    }
  };

  // TODO(pja) 1 : on_disconnect need hooking in.

  // Use same callback for all server-generated messages.
  std::map<MessageType, MsgCallbackType> callbacks;
  callbacks[MessageType::mMetadata] = callback;
  callbacks[MessageType::mDataAck] = callback;
  callbacks[MessageType::mGap] = callback;
  callbacks[MessageType::mDeliver] = callback;
  callbacks[MessageType::mPing] = callback;
  msg_loop_->RegisterCallbacks(callbacks);
}

void Proxy::Start(OnMessageCallback on_message,
                  OnDisconnectCallback on_disconnect) {
  on_message_ = std::move(on_message);
  on_disconnect_ = std::move(on_disconnect);
  msg_thread_ = env_->StartThread([this] () { msg_loop_->Run(); },
                                          "proxy");
  while (!msg_loop_->IsRunning()) {
    std::this_thread::yield();
  }
}

Status Proxy::Forward(std::string msg) {
  stats_.forwards->Add(1);

  // TODO(pja) 1 : Really inefficient. Only need to deserialize header,
  // not entire message, and don't need to copy entire message.
  std::unique_ptr<char[]> buffer = Slice(msg).ToUniqueChars();
  std::unique_ptr<Message> message =
    Message::CreateNewInstance(std::move(buffer), msg.size());
  if (!message) {
    LOG_ERROR(info_log_,
      "Failed to deserialize message forwarded to proxy.");
    stats_.forward_errors->Add(1);
    return Status::InvalidArgument("Message failed to deserialize");
  }

  // Select destination based on message type.
  HostId const* host_id = nullptr;
  switch (message->GetMessageType()) {
    case MessageType::mPing:  // could go to either
    case MessageType::mPublish:
      host_id = &config_->GetPilotHostIds().front();
      break;

    case MessageType::mMetadata:
      host_id = &config_->GetCopilotHostIds().front();
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
      return Status::InvalidArgument("Invalid message type");
  }

  // Create command.
  const bool is_new_request = true;
  std::unique_ptr<Command> cmd(
    new SerializedSendCommand(std::move(msg),
                              host_id->ToClientId(),
                              env_->  NowMicros(),
                              is_new_request));

  // Send to loop.
  // TODO(pja) 1 : Use other threads based on client ID.
  return msg_loop_->SendCommand(std::move(cmd), 0);
}

Proxy::~Proxy() {
  if (msg_loop_->IsRunning()) {
    msg_loop_->Stop();
    env_->WaitForJoin(msg_thread_);
  }
}

}  // namespace rocketspeed
