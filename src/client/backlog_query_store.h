#pragma once

#include <deque>
#include <unordered_map>
#include "include/Types.h"
#include "src/util/common/observable_container.h"

namespace rocketspeed {

class Logger;

/**
 * The responsibility of the BacklogQueryStore is to provide storage and
 * retrieval interfaces for outstanding backlog queries (created from
 * HasMessageSince calls).
 */
class BacklogQueryStore {
 public:
  explicit BacklogQueryStore(
      std::shared_ptr<Logger> info_log,
      std::function<void(Flow*, std::unique_ptr<Message>)> message_handler,
      EventLoop* event_loop);

  enum class Mode {
    /// Query is ready to be sent to server, but not yet sent.
    kPendingSend,

    /// Query is awaiting the associated subscripiton to be synced.
    kAwaitingSync,
  };

  /**
   * Adds a query request to the store. The query can either be in the
   * kPendingSend mode, where it will be sent to the server as soon as the
   * connection is available; or the kAwaitingSync mode where it will sit
   * idly until MarkSynced is invoked for its subscription ID.
   */
  void Insert(Mode mode,
              SubscriptionID sub_id,
              NamespaceID namespace_id,
              Topic topic,
              Epoch epoch,
              SequenceNumber seqno,
              std::function<void(HasMessageSinceResult)> callback);

  /**
   * If any queries are in the kAwaitingSync mode for this subscription ID
   * then they will be queued up ready to be sent to the server.
   */
  void MarkSynced(SubscriptionID sub_id);

  /**
   * Processes a backlog fill message from the server, potentially invoking
   * a callback for a matching outstanding query.
   */
  void ProcessBacklogFill(const MessageBacklogFill& msg);

  /**
   * Starts syncing any pending requests to the server.
   */
  void StartSync();

  /**
   * Stops sending requests to the server.
   */
  void StopSync();

 private:
  struct Key {
    NamespaceID namespace_id;
    Topic topic;
    Epoch epoch;

    bool operator==(const Key& rhs) const {
      return std::tie(namespace_id, topic, epoch) ==
          std::tie(rhs.namespace_id, rhs.topic, rhs.epoch);
    }
    struct Hash {
      size_t operator()(const Key& key) const;
    };
  };

  struct Value {
    SubscriptionID sub_id;
    SequenceNumber seqno;
    std::function<void(HasMessageSinceResult)> callback;
  };

  void HandlePending(Flow* flow, Key key, Value value);


  using Query = std::pair<Key, Value>;

  std::shared_ptr<Logger> info_log_;
  EventLoop* event_loop_;
  std::unordered_map<SubscriptionID, std::vector<Query>> awaiting_sync_;
  ObservableContainer<std::deque<Query>> pending_send_;
  std::unordered_map<Key, std::vector<Value>, Key::Hash> sent_;
  std::function<void(Flow*, std::unique_ptr<Message>)> message_handler_;
};

}
