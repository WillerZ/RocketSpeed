// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "proxy_wrapper.h"

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "src/djinni/conversions.h"
#include "src/djinni/jvm_env.h"
#include "src/proxy/proxy.h"

#include "src-gen/djinni/cpp/DisconnectCallback.hpp"
#include "src-gen/djinni/cpp/MessageCallback.hpp"

namespace rocketspeed {
namespace djinni {

std::shared_ptr<ProxyImpl> ProxyImpl::Create(LogLevel log_level,
                                             ConfigurationImpl config,
                                             int32_t num_workers) {
  rocketspeed::Status status;
  auto config1 = ToConfiguration(std::move(config));

  ProxyOptions options;
  options.conf = std::move(config1);
  if (num_workers < 0) {
    throw std::runtime_error("Number of workers out of range.");
  }
  options.num_workers = num_workers;

  auto jvm_env = JvmEnv::Default();
  options.env = jvm_env;

  options.info_log = jvm_env->CreatePlatformLogger(ToInfoLogLevel(log_level));
  LOG_DEBUG(options.info_log, "Created logger for RocketSpeed Proxy.");

  std::unique_ptr<rocketspeed::Proxy> proxy;
  status = rocketspeed::Proxy::CreateNewInstance(std::move(options), &proxy);
  if (!status.ok()) {
    throw std::runtime_error(status.ToString());
  }
  return std::make_shared<ProxyWrapper>(std::move(proxy));
}

Status ProxyWrapper::Start(
    std::shared_ptr<MessageCallback> message_callback,
    std::shared_ptr<DisconnectCallback> disconnect_callback) {
  auto message_callback1 =
      [message_callback](int64_t session_id, std::string message) {
    auto first = reinterpret_cast<const uint8_t*>(message.data());
    std::vector<uint8_t> message1(first, first + message.size());
    message_callback->Call(session_id, std::move(message1));
  };
  auto disconnect_callback1 =
      [disconnect_callback](const std::vector<int64_t>& session_ids) {
    disconnect_callback->Call(session_ids);
  };
  return FromStatus(proxy_->Start(message_callback1, disconnect_callback1));
}

Status ProxyWrapper::Forward(std::vector<uint8_t> message,
                             int64_t session) {
  std::string message1(message.begin(), message.end());
  return FromStatus(proxy_->Forward(std::move(message1), session));
}

void ProxyWrapper::DestroySession(int64_t session) {
  proxy_->DestroySession(session);
}

void ProxyWrapper::Close() {
  proxy_.reset();
}

}  // namespace djinni
}  // namespace rocketspeed
