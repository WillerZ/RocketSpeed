//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "src-gen/djinni/cpp/InboundID.hpp"
#include "src-gen/djinni/cpp/RocketeerServerImpl.hpp"

namespace rocketspeed {

class Logger;
class Rocketeer;
class RocketeerServer;
class Status;

namespace djinni {

class RocketeerServerWrapper : public RocketeerServerImpl {
 public:
  RocketeerServerWrapper(std::shared_ptr<Logger> info_log,
                         std::unique_ptr<rocketspeed::RocketeerServer> server);

  ~RocketeerServerWrapper();

  void Register(std::shared_ptr<Rocketeer> rocketeer) override;

  Status Start() override;

  bool Deliver(InboundID inbound_id,
               int64_t seqno,
               std::vector<uint8_t> payload) override;

  bool Terminate(InboundID inbound_id) override;

  void Close() override;

 private:
  std::shared_ptr<Logger> info_log_;
  std::vector<std::unique_ptr<rocketspeed::Rocketeer>> rocketeers_;
  std::unique_ptr<rocketspeed::RocketeerServer> server_;
};

}  // namespace djinni
}  // namespace rocketspeed
