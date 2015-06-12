//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>

#include "src-gen/djinni/cpp/ClientImpl.hpp"

namespace rocketspeed {

class Client;
class Logger;

namespace djinni {

class ClientWrapper : public ClientImpl {
 public:
  ClientWrapper(std::unique_ptr<rocketspeed::Client> client,
                std::shared_ptr<Logger> info_log)
      : client_(std::move(client)), info_log_(std::move(info_log)) {}

  MsgId Publish(int32_t tenant_id,
                std::string namespace_id,
                std::string topic_name,
                std::vector<uint8_t> data,
                std::shared_ptr<PublishCallback> publish_cb,
                MsgId message_id) override;

  int64_t Subscribe(int32_t tenant_id,
                    std::string namespace_id,
                    std::string topic_name,
                    int64_t start_seqno,
                    std::shared_ptr<MessageReceivedCallback> deliver_cb,
                    std::shared_ptr<SubscribeCallback> subscribe_cb) override;

  int64_t Resubscribe(SubscriptionParameters params,
                      std::shared_ptr<MessageReceivedCallback> deliver_cb,
                      std::shared_ptr<SubscribeCallback> subscribe_cb) override;

  void Unsubscribe(int64_t sub_handle) override;

  void Acknowledge(int64_t sub_handle, int64_t seqno) override;

  void SaveSubscriptions(
      std::shared_ptr<SnapshotCallback> snapshot_cb) override;

  std::vector<SubscriptionParameters> RestoreSubscriptions() override;

  void Close() override;

 private:
  std::unique_ptr<rocketspeed::Client> client_;
  std::shared_ptr<Logger> info_log_;
};

}  // namespace djinni
}  // namespace rocketspeed
