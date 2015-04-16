// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>

#include "src-gen/djinni/cpp/ProxyImpl.hpp"
#include "src-gen/djinni/cpp/Status.hpp"

namespace rocketspeed {

class Proxy;

namespace djinni {

class MessageCallback;
class DisconnectCallback;

class ProxyWrapper final : public ProxyImpl {
 public:
  explicit ProxyWrapper(std::unique_ptr<rocketspeed::Proxy> proxy)
      : proxy_(std::move(proxy)) {
  }

  Status Start(
      std::shared_ptr<MessageCallback> message_callback,
      std::shared_ptr<DisconnectCallback> disconnect_callback) override;

  Status Forward(std::vector<uint8_t> message,
                 int64_t session) override;

  void DestroySession(int64_t session) override;

  void Close() override;

 private:
  std::unique_ptr<rocketspeed::Proxy> proxy_;
};

}  // namespace djinni
}  // namespace rocketspeed
