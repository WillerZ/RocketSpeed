//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "rocketeer_wrapper.h"

#include <memory>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "include/Status.h"
#include "include/Types.h"
#include "include/RocketSpeed.h"
#include "src/djinni/jvm_env.h"
#include "src/djinni/type_conversions.h"
#include "src/engine/rocketeer.h"
#include "src/engine/rocketeer_server.h"
#include "src/messages/stream_socket.h"
#include "src/util/common/coding.h"

#include "src-gen/djinni/cpp/InboundID.hpp"
#include "src-gen/djinni/cpp/LogLevel.hpp"
#include "src-gen/djinni/cpp/Rocketeer.hpp"
#include "src-gen/djinni/cpp/RocketeerServerImpl.hpp"
#include "src-gen/djinni/cpp/Status.hpp"
#include "src-gen/djinni/cpp/SubscriptionParameters.hpp"

namespace rocketspeed {
namespace djinni {

namespace {

rs::Status ToInboundID(const jni::InboundID& inbound_id, rs::InboundID* out) {
  auto data = reinterpret_cast<const char*>(inbound_id.serialised.data());
  Slice in(data, inbound_id.serialised.size());
  rs::InboundID result;
  if (!DecodeOrigin(&in, &result.stream_id)) {
    return rs::Status::IOError("Bad origin");
  }
  if (!GetFixed64(&in, &result.sub_id)) {
    return rs::Status::IOError("Bad subscription ID");
  }
  *out = result;
  return rs::Status::OK();
}

jni::InboundID FromInboundID(const rs::InboundID& in) {
  std::string serial;
  EncodeOrigin(&serial, in.stream_id);
  PutFixed64(&serial, in.sub_id);
  auto data = reinterpret_cast<const uint8_t*>(serial.data());
  return jni::InboundID(std::vector<uint8_t>(data, data + serial.size()));
}

jni::SubscriptionParameters FromSubscriptionParameters(
    rs::SubscriptionParameters in) {
  return jni::SubscriptionParameters(in.tenant_id,
                                     std::move(in.namespace_id),
                                     std::move(in.topic_name),
                                     FromSequenceNumber(in.start_seqno));
}

class ForwardingRocketeer : public rs::Rocketeer {
 public:
  ForwardingRocketeer(std::shared_ptr<jni::Rocketeer> rocketeer,
                      std::shared_ptr<Logger> info_log)
  : rocketeer_(std::move(rocketeer)), info_log_(std::move(info_log)) {}

  void HandleNewSubscription(rs::InboundID inbound_id,
                             rs::SubscriptionParameters params) override {
    try {
      rocketeer_->HandleNewSubscription(FromInboundID(inbound_id),
                                        FromSubscriptionParameters(params));
    } catch (const std::exception& e) {
      LOG_WARN(
          info_log_, "HandleNewSubscription caught exception: %s", e.what());
    };
  }

  void HandleTermination(rs::InboundID inbound_id,
                         TerminationSource source) override {
    try {
      rocketeer_->HandleTermination(FromInboundID(inbound_id),
                                    source == TerminationSource::Subscriber);
    } catch (const std::exception& e) {
      LOG_WARN(info_log_, "HandleTermination caught exception: %s", e.what());
    };
  }

 private:
  std::shared_ptr<jni::Rocketeer> rocketeer_;
  std::shared_ptr<Logger> info_log_;
};

}  // namespace

RocketeerServerWrapper::RocketeerServerWrapper(
  std::shared_ptr<Logger> info_log,
  std::unique_ptr<rocketspeed::RocketeerServer> server)
: info_log_(std::move(info_log)), server_(std::move(server)) {}

RocketeerServerWrapper::~RocketeerServerWrapper() {
}

std::shared_ptr<RocketeerServerImpl> RocketeerServerImpl::Create(
    LogLevel log_level,
    int32_t listener_port) {
  auto listener_port1 = static_cast<uint16_t>(listener_port);
  if (listener_port1 != listener_port) {
    throw std::runtime_error("Invalid port.");
  }

  auto jvm_env = JvmEnv::Default();
  auto log_level1 = ToInfoLogLevel(log_level);
  auto info_log = jvm_env->CreatePlatformLogger(log_level1);
  LOG_VITAL(info_log,
            "Created JVM logger for Rocketeer, log level: %s",
            rs::LogLevelToString(log_level1));

  rs::RocketeerOptions options;
  options.env = jvm_env;
  options.info_log = info_log;
  options.port = listener_port1;

  std::unique_ptr<rs::RocketeerServer> server(
      new rs::RocketeerServer(std::move(options)));
  return std::make_shared<RocketeerServerWrapper>(info_log, std::move(server));
}

void RocketeerServerWrapper::Register(std::shared_ptr<Rocketeer> rocketeer) {
  rocketeers_.emplace_back(
      new ForwardingRocketeer(std::move(rocketeer), info_log_));
  server_->Register(rocketeers_.back().get());
}

jni::Status RocketeerServerWrapper::Start() {
  return FromStatus(server_->Start());
}

bool RocketeerServerWrapper::Deliver(jni::InboundID inbound_id,
                                     int64_t seqno,
                                     std::vector<uint8_t> payload) {
  rs::InboundID inbound_id1;
  auto st = ToInboundID(inbound_id, &inbound_id1);
  if (!st.ok()) {
    LOG_ERROR(
        info_log_, "Failed to parse InboundID: %s", st.ToString().c_str());
    return false;
  }
  return server_->Deliver(
      inbound_id1, ToSequenceNumber(seqno), ToSlice(payload).ToString());
}

bool RocketeerServerWrapper::Terminate(jni::InboundID inbound_id) {
  rs::InboundID inbound_id1;
  auto st = ToInboundID(inbound_id, &inbound_id1);
  if (!st.ok()) {
    LOG_ERROR(
        info_log_, "Failed to parse InboundID: %s", st.ToString().c_str());
    return false;
  }
  return server_->Terminate(inbound_id1,
                            rs::Rocketeer::UnsubscribeReason::Requested);
}

void RocketeerServerWrapper::Close() {
  server_.reset();
  rocketeers_.clear();
}

}  // namespace djinni
}  // namespace rocketspeed
