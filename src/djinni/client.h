// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>

#include "src-gen/djinni/ClientImpl.hpp"
#include "src-gen/djinni/MsgIdImpl.hpp"
#include "src-gen/djinni/PublishStatus.hpp"
#include "src-gen/djinni/RetentionBase.hpp"
#include "src-gen/djinni/SubscriptionRequestImpl.hpp"

namespace rocketspeed {

class Client;

namespace djinni {

class PublishCallbackImpl;

class Client : public ClientImpl {
 public:
  explicit Client(rocketspeed::Client* client) : client_(client) {}

  PublishStatus Publish(
      int32_t namespace_id,
      std::string topic_name,
      RetentionBase retention,
      std::vector<uint8_t> data,
      std::experimental::optional<MsgIdImpl> message_id,
      std::shared_ptr<PublishCallbackImpl> publish_callback) override;

  void ListenTopics(std::vector<SubscriptionRequestImpl> names) override;

  void Acknowledge(int32_t namespace_id,
                   std::string topic_name,
                   int64_t sequence_number) override;

  void Close() override;

 private:
  std::unique_ptr<rocketspeed::Client> client_;
};

}  // namespace djinni
}  // namespace rocketspeed
