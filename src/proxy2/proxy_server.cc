/// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
/// This source code is licensed under the BSD-style license found in the
/// LICENSE file in the root directory of this source tree. An additional grant
/// of patent rights can be found in the PATENTS file in the same directory.
#include "src/proxy2/proxy_server.h"

#include <chrono>
#include <memory>
#include <utility>

#include "include/Assert.h"
#include "include/Env.h"
#include "include/Logger.h"
#include "include/RocketSpeed.h"
#include "include/Status.h"
#include "include/Types.h"
#include "src/messages/event_loop.h"
#include "src/messages/msg_loop.h"
#include "src/proxy2/downstream_worker.h"
#include "src/proxy2/upstream_worker.h"
#include "src/util/common/client_env.h"
#include "src/util/common/statistics.h"

namespace rocketspeed {

ProxyServerOptions::~ProxyServerOptions() = default;

Status ProxyServer::Create(ProxyServerOptions options,
                           std::unique_ptr<ProxyServer>* out) {
  RS_ASSERT(out);

  if (!options.backoff_strategy) {
    options.backoff_strategy = backoff::RandomizedTruncatedExponential(
        std::chrono::milliseconds(100), std::chrono::seconds(1), 2.0);
  }

  auto proxy = std::make_unique<ProxyServerImpl>(std::move(options));
  auto st = proxy->Start();
  if (!st.ok()) {
    return st;
  }
  *out = std::move(proxy);
  return Status::OK();
}

ProxyServer::~ProxyServer() = default;

ProxyServerImpl::ProxyServerImpl(ProxyServerOptions options)
: options_(std::move(options)) {}

Status ProxyServerImpl::Start() {
  MsgLoop::Options loop_options;
  loop_options.event_loop.connection_without_streams_keepalive =
      std::chrono::seconds(3600);

  upstream_loop_.reset(
      new MsgLoop(ClientEnv::Default(),
                  EnvOptions(),
                  -1,  // No listener.
                  static_cast<int>(options_.num_upstream_threads),
                  options_.info_log,
                  "proxy2-upstream",
                  loop_options));
  auto st = upstream_loop_->Initialize();
  if (!st.ok()) {
    return st;
  }
  downstream_loop_.reset(
      new MsgLoop(ClientEnv::Default(),
                  EnvOptions(),
                  options_.port,
                  static_cast<int>(options_.num_downstream_threads),
                  options_.info_log,
                  "proxy2-downstream",
                  loop_options));
  st = downstream_loop_->Initialize();
  if (!st.ok()) {
    return st;
  }

  for (size_t i = 0; i < options_.num_upstream_threads; ++i) {
    upstream_workers_.emplace_back(std::make_unique<UpstreamWorker>(
        options_,
        upstream_loop_->GetEventLoop(static_cast<int>(i)),
        downstream_loop_->GetStreamMapping()));
  }

  for (size_t i = 0; i < options_.num_downstream_threads; ++i) {
    downstream_workers_.emplace_back(std::make_unique<DownstreamWorker>(
        options_, downstream_loop_->GetEventLoop(static_cast<int>(i))));
  }

  size_t downstream_id = 0;
  for (const auto& downstream : downstream_workers_) {
    size_t upstream_id = 0;
    for (const auto& upstream : upstream_workers_) {
      upstream->ConnectOutboundQueue(
          downstream_id, downstream->CreateInboundQueue(upstream_id));
      downstream->ConnectOutboundQueue(
          upstream_id, upstream->CreateInboundQueue(downstream_id));
      ++upstream_id;
    }
    ++downstream_id;
  }

  downstream_loop_->RegisterCallbacks({
      {MessageType::mIntroduction, CreateDownstreamCallback()},
      {MessageType::mSubscribe, CreateDownstreamCallback()},
      {MessageType::mUnsubscribe, CreateDownstreamCallback()},
      {MessageType::mGoodbye, CreateDownstreamCallback()},
  });

  auto env = Env::Default();
  upstream_thread_.reset(
      new MsgLoopThread(env, upstream_loop_.get(), "proxy2-upstream"));
  downstream_thread_.reset(
      new MsgLoopThread(env, downstream_loop_.get(), "proxy2-downstream"));

  upstream_loop_->ReliableGather(
      [&](size_t i) -> int {
        upstream_workers_[i]->Start();
        return 0;
      },
      [](std::vector<int>) -> int { return 0; });

  LOG_VITAL(options_.info_log,
            "Started ProxyServer listening at: %s",
            downstream_loop_->GetHostId().ToString().c_str());
  return Status::OK();
}

const HostId& ProxyServerImpl::GetListenerAddress() const {
  return downstream_loop_->GetHostId();
}

void ProxyServerImpl::ExportStatistics(StatisticsVisitor* visitor) const {
  Statistics stats;
  stats.Aggregate(upstream_loop_->GetStatisticsSync());
  stats.Aggregate(downstream_loop_->GetStatisticsSync());
  stats.Aggregate(
      upstream_loop_->AggregateStatsSync([this](size_t i) -> Statistics {
        return *upstream_workers_[i]->GetStatistics();
      }));
  stats.Aggregate(
      downstream_loop_->AggregateStatsSync([this](size_t i) -> Statistics {
        return *downstream_workers_[i]->GetStatistics();
      }));

  stats.Export(visitor);
}

ProxyServerImpl::~ProxyServerImpl() = default;

MsgCallbackType ProxyServerImpl::CreateDownstreamCallback() {
  return [this](Flow* flow, std::unique_ptr<Message> message, StreamID origin) {
    StreamReceiveArg<Message> arg = {flow, origin, std::move(message)};
    auto worker_id = downstream_loop_->GetThreadWorkerIndex();
    downstream_workers_[worker_id]->operator()(std::move(arg));
  };
}

}  // namespace rocketspeed
