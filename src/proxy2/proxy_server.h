/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <memory>
#include <vector>

#include "include/ProxyServer.h"
#include "src/messages/event_loop.h"
#include "src/messages/queues.h"

namespace rocketspeed {

class DownstreamWorker;
class MsgLoop;
class MsgLoopThread;
class Statistics;
class UpstreamWorker;

class ProxyServerImpl : public ProxyServer {
 public:
  explicit ProxyServerImpl(ProxyServerOptions options);

  Status Start();

  const HostId& GetListenerAddress() const override;

  void ExportStatistics(StatisticsVisitor* visitor) const override;

  ~ProxyServerImpl() override;

 private:
  const ProxyServerOptions options_;

  std::unique_ptr<MsgLoop> upstream_loop_;
  std::unique_ptr<MsgLoop> downstream_loop_;
  std::vector<std::unique_ptr<UpstreamWorker>> upstream_workers_;
  std::vector<std::unique_ptr<DownstreamWorker>> downstream_workers_;
  std::unique_ptr<MsgLoopThread> upstream_thread_;
  std::unique_ptr<MsgLoopThread> downstream_thread_;

  std::function<void(Flow*, std::unique_ptr<Message>, StreamID)>
  CreateDownstreamCallback();
};

}  // namespace rocketspeed
