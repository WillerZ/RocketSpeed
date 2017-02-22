#include "src/client/backlog_query_store.h"

#include "include/Logger.h"
#include "src/messages/flow_control.h"
#include <xxhash.h>

namespace rocketspeed {

BacklogQueryStore::BacklogQueryStore(
    std::shared_ptr<Logger> info_log,
    std::function<void(Flow*, std::unique_ptr<Message>)> message_handler,
    EventLoop* event_loop)
: info_log_(std::move(info_log))
, event_loop_(event_loop)
, pending_send_(event_loop, "pending_send_backlog_requests")
, message_handler_(std::move(message_handler)) {
  // Wire the source of queries pending send.
  auto flow_control = event_loop_->GetFlowControl();
  flow_control->Register<Query>(
      &pending_send_,
      [this](Flow* flow, Query req) {
        HandlePending(flow, req.first, req.second);
      });
  pending_send_.SetReadEnabled(event_loop_, false);
}

void BacklogQueryStore::Insert(
    Mode mode,
    SubscriptionID sub_id,
    NamespaceID namespace_id,
    Topic topic,
    DataSource source,
    SequenceNumber seqno,
    std::function<void(HasMessageSinceResult, std::string)> callback) {
  // Add a pending request.
  // This will be sent to the server later (see HandlePending).
  TopicUUID uuid(namespace_id, topic);
  LOG_DEBUG(info_log_,
      "BacklogQueryStore::Insert(%s, %s)",
      uuid.ToString().c_str(),
      mode == Mode::kPendingSend ? "pending_send" : "awaiting_sync");
  Key key { std::move(namespace_id), std::move(topic), std::move(source) };
  Value value { sub_id, seqno, std::move(callback) };

  switch (mode) {
    case Mode::kPendingSend:
      pending_send_.Modify([&](std::deque<Query>& pending) {
        pending.emplace_back(std::move(key), std::move(value));
      });
      break;
    case Mode::kAwaitingSync:
      awaiting_sync_[uuid].emplace_back(std::move(key), std::move(value));
      break;
  }
}

void BacklogQueryStore::MarkSynced(const TopicUUID& uuid) {
  auto it = awaiting_sync_.find(uuid);
  if (it != awaiting_sync_.end()) {
    LOG_DEBUG(info_log_,
              "BacklogQueryStore::MarkSynced(%s)",
              uuid.ToString().c_str());
    pending_send_.Modify([&](std::deque<Query>& pending) {
      for (auto& query : it->second) {
        pending.emplace_back(std::move(query));
      }
    });
    awaiting_sync_.erase(it);
  }
}

void BacklogQueryStore::ProcessBacklogFill(const MessageBacklogFill& msg) {
  // Check if it matches any outstanding request.
  // If it does, invoke the callback and remove the request.
  TopicUUID uuid(msg.GetNamespace(), msg.GetTopicName());
  Key key { msg.GetNamespace(), msg.GetTopicName(), msg.GetDataSource() };
  bool processed = false;
  auto it = sent_.find(key);
  if (it != sent_.end()) {
    if (!it->second.empty()) {
      it->second.front().callback(msg.GetResult(), msg.GetInfo());
      it->second.pop_front();
      if (it->second.empty()) {
        sent_.erase(it);
        processed = true;
        LOG_DEBUG(info_log_,
            "BacklogQueryStore::ProcessBacklogFill(%s) processed",
            uuid.ToString().c_str());
      }
    }
  }
  if (!processed) {
    LOG_WARN(info_log_,
        "BacklogQueryStore::ProcessBacklogFill(%s) didn't match a request",
        uuid.ToString().c_str());
  }
}

void BacklogQueryStore::HandlePending(Flow* flow, Key key, Value value) {
  // Send the pending query to the server and mark as sent.
  std::unique_ptr<Message> message(
      new MessageBacklogQuery(GuestTenant,  // TODO
                              value.sub_id,
                              key.namespace_id,
                              key.topic,
                              key.source,
                              value.seqno));
  sent_[std::move(key)].emplace_back(std::move(value));

  // Note: for tests, message_handler_ might invoke ProcessBacklogFill, so we
  // ensure all state (i.e. sent_) is reflects the update before calling.
  // This wouldn't happen in a real client-server interaction, but makes
  // testing easier.
  message_handler_(flow, std::move(message));
}

void BacklogQueryStore::StartSync() {
  // Start sending and pending requests now that we have a sink.
  pending_send_.SetReadEnabled(event_loop_, true);
}

void BacklogQueryStore::StopSync() {
  // Disconnected, so stop sending requests and move all unresponded queries
  // back to the awaiting sync state.
  for (auto& entry : sent_) {
    TopicUUID uuid(entry.first.namespace_id, entry.first.topic);
    for (auto& value : entry.second) {
      awaiting_sync_[uuid].emplace_back(entry.first, std::move(value));
    }
  }
  sent_.clear();
  pending_send_.Modify([&](std::deque<Query>& pending) {
    for (auto& entry : pending) {
      TopicUUID uuid(entry.first.namespace_id, entry.first.topic);
      awaiting_sync_[uuid].push_back(entry);
    }
    pending.clear();
  });
  pending_send_.SetReadEnabled(event_loop_, false);
}

size_t BacklogQueryStore::Key::Hash::operator()(const Key& key) const {
  const uint64_t seed = 0xd6c5552724cda88bULL;
  XXH64_stateSpace_t state;
  XXH64_resetState(&state, seed);
  XXH64_update(&state, key.namespace_id.data(), key.namespace_id.size());
  XXH64_update(&state, key.topic.data(), key.topic.size());
  XXH64_update(&state, key.source.data(), key.source.size());
  return XXH64_intermediateDigest(&state);
}

} // namespace rocketspeed
