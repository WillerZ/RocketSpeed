#include "src/client/backlog_query_store.h"

#include "include/Logger.h"
#include "src/messages/flow_control.h"
#include "external/xxhash/xxhash.h"

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
    Epoch epoch,
    SequenceNumber seqno,
    std::function<void(HasMessageSinceResult)> callback) {
  // Add a pending request.
  // This will be sent to the server later (see HandlePending).
  Key key { std::move(namespace_id), std::move(topic), std::move(epoch) };
  Value value { sub_id, seqno, std::move(callback) };

  switch (mode) {
    case Mode::kPendingSend:
      pending_send_.Modify([&](std::deque<Query>& pending) {
        pending.emplace_back(std::move(key), std::move(value));
      });
      break;
    case Mode::kAwaitingSync:
      awaiting_sync_[sub_id].emplace_back(std::move(key), std::move(value));
      break;
  }
}

void BacklogQueryStore::MarkSynced(SubscriptionID sub_id) {
  auto it = awaiting_sync_.find(sub_id);
  if (it != awaiting_sync_.end()) {
    LOG_DEBUG(info_log_, "BacklogQueryStore::MarkSynced(%" PRIu64 ")",
        sub_id.ForLogging());
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
  Key key { msg.GetNamespace(), msg.GetTopicName(), msg.GetEpoch() };
  auto it = sent_.find(key);
  if (it != sent_.end()) {
    for (auto it2 = it->second.begin(); it2 != it->second.end(); ) {
      auto seqno = it2->seqno;
      // TODO(pja): can be smarter about matching here, e.g. if the answer is
      // kNo and the sequence number range covers a greater range than the query
      // then the fill can answer multiple queries at once.
      if (msg.GetPrevSequenceNumber() == seqno) {
        it2->callback(msg.GetResult());
        it2 = it->second.erase(it2);
      } else {
        ++it2;
      }
    }
    if (it->second.empty()) {
      sent_.erase(it);
    }
  }
}

void BacklogQueryStore::HandlePending(Flow* flow, Key key, Value value) {
  // Send the pending query to the server and mark as sent.
  std::unique_ptr<Message> message(
      new MessageBacklogQuery(GuestTenant,  // TODO
                              value.sub_id,
                              key.namespace_id,
                              key.topic,
                              key.epoch,
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
    for (auto& value : entry.second) {
      awaiting_sync_[value.sub_id].emplace_back(entry.first, std::move(value));
    }
  }
  sent_.clear();
  pending_send_.Modify([&](std::deque<Query>& pending) {
    for (auto& entry : pending) {
      awaiting_sync_[entry.second.sub_id].push_back(entry);
    }
    pending.clear();
  });
  pending_send_.SetReadEnabled(event_loop_, false);
}

size_t BacklogQueryStore::Key::Hash::operator()(const Key& key) const {
  const uint64_t seed = 0xd6c5552724cda88bULL;
  XXH64_state_t state;
  XXH64_reset(&state, seed);
  XXH64_update(&state, key.namespace_id.data(), key.namespace_id.size());
  XXH64_update(&state, key.topic.data(), key.topic.size());
  XXH64_update(&state, key.epoch.data(), key.epoch.size());
  return XXH64_digest(&state);
}

} // namespace rocketspeed
