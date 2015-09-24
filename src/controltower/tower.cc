//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#include "src/controltower/tower.h"

#include <map>
#include <numeric>
#include <sstream>
#include <thread>
#include <vector>

#include "src/util/auto_roll_logger.h"
#include "src/util/logging.h"
#include "src/util/log_buffer.h"
#include "src/util/storage.h"
#include "src/messages/queues.h"

#include "src/controltower/log_tailer.h"
#include "src/controltower/room.h"
#include "src/controltower/topic_tailer.h"

#include "external/folly/move_wrapper.h"

namespace rocketspeed {

/**
 * Sanitize user-specified options
 */
ControlTowerOptions
ControlTower::SanitizeOptions(const ControlTowerOptions& src) {
  ControlTowerOptions result = src;

  if (result.info_log == nullptr) {
    Status s = CreateLoggerFromOptions(src.env,
                                       result.log_dir,
                                       "LOG.controltower",
                                       result.log_file_time_to_roll,
                                       result.max_log_file_size,
                                       result.info_log_level,
                                       &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = std::make_shared<NullLogger>();
    }
  }
  return result;
}

/**
 * Private constructor for a Control Tower
 */
ControlTower::ControlTower(const ControlTowerOptions& options):
  options_(SanitizeOptions(options))  {
  // The rooms and that tailers are not initialized here.
  // The reason being that those initializations could fail and
  // return error Status.
  options_.msg_loop->RegisterCallbacks(InitializeCallbacks());

  for (int i = 0; i < options_.msg_loop->GetNumWorkers(); ++i) {
    tower_to_room_queues_.emplace_back(
      options_.msg_loop->CreateWorkerQueues());
    find_latest_seqno_response_queues_.emplace_back(
      options_.msg_loop->CreateThreadLocalQueues(i));
  }

  sub_to_room_.resize(options_.msg_loop->GetNumWorkers());
}

ControlTower::~ControlTower() {
}

void ControlTower::Stop() {
  // MsgLoop must be stopped first.
  assert(!options_.msg_loop->IsRunning());

  // Stop log tailer from communicating with log storage.
  log_tailer_->Stop();

  // Release reference to log storage.
  options_.storage.reset();
}

/**
 * This is a static method to create a ControlTower
 */
Status
ControlTower::CreateNewInstance(const ControlTowerOptions& options,
                                ControlTower** ct) {
  if (!options.storage) {
    assert(false);
    return Status::InvalidArgument("Log storage must be provided");
  }

  if (!options.log_router) {
    assert(false);
    return Status::InvalidArgument("Log router must be provided");
  }

  std::unique_ptr<ControlTower> tower(new ControlTower(options));
  Status st = tower->Initialize();
  if (!st.ok()) {
    tower.reset();
  }
  *ct = tower.release();
  return st;
}

Status ControlTower::Initialize() {
  const ControlTowerOptions opt = GetOptions();

  // Create the LogTailer first.
  LogTailer* log_tailer;
  Status st = LogTailer::CreateNewInstance(opt.env,
                                           opt.storage,
                                           opt.info_log,
                                           &log_tailer);
  if (!st.ok()) {
    return st;
  }
  log_tailer_.reset(log_tailer);

  // Initialize the LogTailer.
  auto on_record = [this] (std::unique_ptr<MessageData>& msg,
                           LogID log_id,
                           size_t reader_id) {
    // Process message from the log tailer.
    const int room_number = LogIDToRoom(log_id);
    Status status = topic_tailer_[room_number]->SendLogRecord(
      msg,
      log_id,
      reader_id);
    if (!status.ok()) {
      LOG_ERROR(options_.info_log,
        "Failed to SendLogRecord to topic tailer %d (%s)",
        room_number,
        status.ToString().c_str());
      assert(msg);
      return false;
    }
    return true;
  };

  auto on_gap = [this] (LogID log_id,
                        GapType type,
                        SequenceNumber from,
                        SequenceNumber to,
                        size_t reader_id) {
    // Process message from the log tailer.
    const int room_number = LogIDToRoom(log_id);
    Status status = topic_tailer_[room_number]->SendGapRecord(
      log_id,
      type,
      from,
      to,
      reader_id);
    if (!status.ok()) {
      LOG_ERROR(options_.info_log,
        "Failed to SendGapRecord to topic tailer %d (%s)",
        room_number,
        status.ToString().c_str());
      return false;
    }
    return true;
  };

  const size_t num_rooms = opt.msg_loop->GetNumWorkers();
  const size_t num_readers = num_rooms * opt.readers_per_room;
  st = log_tailer_->Initialize(std::move(on_record),
                               std::move(on_gap),
                               num_readers);
  if (!st.ok()) {
    return st;
  }

  // equally distribute the cache among the workers
  size_t cache_size_per_room = 0;
  if (opt.cache_size > 0) {
    cache_size_per_room = std::max(opt.cache_size / num_rooms, 1024UL);
  }

  // Now create the TopicTailer.
  // One per room with one reader each.
  size_t reader_id = 0;

  for (size_t i = 0; i < num_rooms; ++i) {
    auto on_message =
      [this, i] (const Message& msg,
                 std::vector<CopilotSub> recipients) {
        rooms_[i]->OnTailerMessage(msg, std::move(recipients));
      };
    TopicTailer* topic_tailer;
    st = TopicTailer::CreateNewInstance(opt.env,
                                        options_.msg_loop,
                                        int(i),
                                        log_tailer_.get(),
                                        opt.log_router,
                                        opt.info_log,
                                        cache_size_per_room,
                                        opt.cache_data_from_system_namespaces,
                                        std::move(on_message),
                                        opt.topic_tailer,
                                        &topic_tailer);
    if (st.ok()) {
      // Topic tailer i uses reader i in log tailer.
      std::vector<size_t> reader_ids;
      for (size_t j = 0; j < opt.readers_per_room; ++j) {
        reader_ids.push_back(reader_id++);
      }
      st = topic_tailer->Initialize(reader_ids, opt.max_subscription_lag);
    }
    if (!st.ok()) {
      return st;
    }
    topic_tailer_.emplace_back(topic_tailer);
  }

  for (unsigned int i = 0; i < num_rooms; i++) {
    rooms_.emplace_back(new ControlRoom(opt, this, i));
  }
  return Status::OK();
}

void ControlTower::ProcessSubscribe(std::unique_ptr<Message> msg,
                                    StreamID origin) {
  options_.msg_loop->ThreadCheck();

  MessageSubscribe* subscribe = static_cast<MessageSubscribe*>(msg.get());
  auto sub_id = subscribe->GetSubID();
  if (subscribe->GetNamespace() == InvalidNamespace) {
    // Invalid namespace, so respond with forced unsubscription.
    MessageUnsubscribe message(subscribe->GetTenantID(),
                               sub_id,
                               MessageUnsubscribe::Reason::kInvalid);
    auto command = options_.msg_loop->ResponseCommand(message, origin);
    options_.msg_loop->SendCommandToSelf(std::move(command));
    return;
  }

  LogID log_id;
  Status st = options_.log_router->GetLogID(subscribe->GetNamespace().c_str(),
                                            subscribe->GetTopicName().c_str(),
                                            &log_id);
  if (!st.ok()) {
    LOG_WARN(options_.info_log,
        "Unable to map Topic(%s,%s) to logid %s",
        subscribe->GetNamespace().c_str(),
        subscribe->GetTopicName().c_str(),
        st.ToString().c_str());
    return;
  }
  const int room_number = LogIDToRoom(log_id);
  ControlRoom* room = rooms_[room_number].get();
  int worker_id = options_.msg_loop->GetThreadWorkerIndex();

  auto command = room->MsgCommand(std::move(msg), worker_id, origin);
  auto& queue = tower_to_room_queues_[worker_id][room_number];
  if (!queue->Write(command)) {
    LOG_WARN(options_.info_log,
        "Unable to forward subscription for Topic(%s,%s)@%" PRIu64
        " to rooms-%u",
        subscribe->GetNamespace().c_str(),
        subscribe->GetTopicName().c_str(),
        subscribe->GetStartSequenceNumber(),
        room_number);
  } else {
    LOG_DEBUG(options_.info_log,
        "Forwarded subscription for Topic(%s,%s)@%" PRIu64 " to rooms-%u",
        subscribe->GetNamespace().c_str(),
        subscribe->GetTopicName().c_str(),
        subscribe->GetStartSequenceNumber(),
        room_number);
  }

  auto& room_map = sub_to_room_[worker_id];
  room_map.Insert(origin, sub_id, room_number);
}

void ControlTower::ProcessUnsubscribe(std::unique_ptr<Message> msg,
                                      StreamID origin) {
  options_.msg_loop->ThreadCheck();
  int worker_id = options_.msg_loop->GetThreadWorkerIndex();

  MessageUnsubscribe* unsubscribe = static_cast<MessageUnsubscribe*>(msg.get());
  auto& room_map = sub_to_room_[worker_id];
  int room_number;
  if (!room_map.MoveOut(origin, unsubscribe->GetSubID(), &room_number)) {
    return;
  }
  ControlRoom* room = rooms_[room_number].get();

  auto command = room->MsgCommand(std::move(msg), worker_id, origin);
  auto& queue = tower_to_room_queues_[worker_id][room_number];
  if (!queue->Write(command)) {
    LOG_WARN(options_.info_log,
        "Unable to forward unsubscription to rooms-%u",
        room_number);
  } else {
    LOG_DEBUG(options_.info_log,
        "Forwarded unsubscription to rooms-%u",
        room_number);
  }
}

void ControlTower::ProcessFindTailSeqno(std::unique_ptr<Message> msg,
                                        StreamID origin) {
  options_.msg_loop->ThreadCheck();
  int worker_id = options_.msg_loop->GetThreadWorkerIndex();
  MessageFindTailSeqno* request = static_cast<MessageFindTailSeqno*>(msg.get());

  // Find log ID for topic.
  LogID log_id;
  Status st = options_.log_router->GetLogID(Slice(request->GetNamespace()),
                                            Slice(request->GetTopicName()),
                                            &log_id);
  if (!st.ok()) {
    LOG_WARN(options_.info_log,
        "Unable to map Topic(%s,%s) to logid %s",
        request->GetNamespace().c_str(),
        request->GetTopicName().c_str(),
        st.ToString().c_str());
    return;
  }

  // Send FindLatestSeqno request to log tailer.
  auto msg_moved = folly::makeMoveWrapper(std::move(msg));
  auto callback =
    [this, log_id, msg_moved, origin, worker_id]
    (Status status, SequenceNumber seqno) mutable {
      MessageFindTailSeqno* req =
        static_cast<MessageFindTailSeqno*>(msg_moved->get());
      if (status.ok()) {
        // Sequence number found, so send gap back to clinet.
        MessageTailSeqno response(req->GetTenantID(),
                                  req->GetNamespace(),
                                  req->GetTopicName(),
                                  seqno);
        auto command = options_.msg_loop->ResponseCommand(response, origin);
        auto& queues = find_latest_seqno_response_queues_[worker_id];
        if (queues->GetThreadLocal()->Write(command)) {
          LOG_DEBUG(options_.info_log,
            "Sent latest seqno %" PRIu64 " to %llu for Topic(%s,%s)",
            seqno,
            origin,
            req->GetNamespace().c_str(),
            req->GetTopicName().c_str());
        } else {
          LOG_WARN(options_.info_log,
            "Failed to send latest seqno to %llu for Topic(%s,%s)",
            origin,
            req->GetNamespace().c_str(),
            req->GetTopicName().c_str());
        }
      } else {
        LOG_ERROR(options_.info_log,
          "FindLatestSeqno for Log(%" PRIu64 ") failed with: %s",
          log_id,
          status.ToString().c_str());
      }
    };

  const int room = LogIDToRoom(log_id);
  std::unique_ptr<Command> cmd(
    MakeExecuteCommand([this, room, log_id, callback] () mutable {
      SequenceNumber seqno = topic_tailer_[room]->GetTailSeqnoEstimate(log_id);
      if (seqno) {
        callback(Status::OK(), seqno);
      } else {
        Status status = log_tailer_->FindLatestSeqno(log_id, callback);
        if (status.ok()) {
          LOG_DEBUG(options_.info_log,
            "Sent FindLatestSeqno for Log(%" PRIu64 ")",
            log_id);
        } else {
          LOG_ERROR(options_.info_log,
            "FindLatestSeqno for Log(%" PRIu64 ") failed with: %s",
            log_id,
            status.ToString().c_str());
        }
      }
    }));
  auto& queue = tower_to_room_queues_[worker_id][room];
  if (!queue->Write(cmd)) {
    LOG_ERROR(options_.info_log,
      "Failed to enqueue command to find latest seqno on Log(%" PRIu64 ")",
      log_id);
  }
}

// The message is forwarded to the appropriate ControlRoom
void
ControlTower::ProcessGoodbye(std::unique_ptr<Message> msg, StreamID origin) {
  options_.msg_loop->ThreadCheck();
  int worker_id = options_.msg_loop->GetThreadWorkerIndex();

  // get the request message
  MessageGoodbye* goodbye = static_cast<MessageGoodbye*>(msg.get());

  for (int i = 0; i < options_.msg_loop->GetNumWorkers(); ++i) {
    // forward message to the destination room
    std::unique_ptr<Message> new_msg(
      new MessageGoodbye(goodbye->GetTenantID(),
                         goodbye->GetCode(),
                         goodbye->GetOriginType()));
    auto command = rooms_[i]->MsgCommand(std::move(new_msg), -1, origin);
    auto& queue = tower_to_room_queues_[worker_id][i];
    if (!queue->Write(command)) {
      LOG_WARN(options_.info_log,
          "Unable to forward goodbye to rooms-%d",
          i);
    } else {
      LOG_DEBUG(options_.info_log,
          "Forwarded goodbye to rooms-%d",
          i);
    }
  }
  sub_to_room_[worker_id].Remove(origin);
}

// A static method to initialize the callback map
std::map<MessageType, MsgCallbackType>
ControlTower::InitializeCallbacks() {
  // create a temporary map and initialize it
  std::map<MessageType, MsgCallbackType> cb;
  cb[MessageType::mSubscribe] = [this] (std::unique_ptr<Message> msg,
                                        StreamID origin) {
    ProcessSubscribe(std::move(msg), origin);
  };
  cb[MessageType::mUnsubscribe] = [this] (std::unique_ptr<Message> msg,
                                          StreamID origin) {
    ProcessUnsubscribe(std::move(msg), origin);
  };
  cb[MessageType::mFindTailSeqno] = [this] (std::unique_ptr<Message> msg,
                                            StreamID origin) {
    ProcessFindTailSeqno(std::move(msg), origin);
  };
  cb[MessageType::mGoodbye] = [this] (std::unique_ptr<Message> msg,
                                      StreamID origin) {
    ProcessGoodbye(std::move(msg), origin);
  };

  // return the updated map
  return cb;
}

Statistics ControlTower::GetStatisticsSync() {
  return options_.msg_loop->AggregateStatsSync(
    [this] (int i) { return topic_tailer_[i]->GetStatistics(); });
}

std::string ControlTower::GetInfoSync(std::vector<std::string> args) {
  if (args.size() >= 1) {
    if (args[0] == "log" && args.size() == 2 && !args[1].empty()) {
      // log n  -- information about a single log.
      char* end = nullptr;
      const LogID log_id { strtoul(&*args[1].begin(), &end, 10) };
      const int room = LogIDToRoom(log_id);
      std::string result;
      Status st =
        options_.msg_loop->WorkerRequestSync(
          [this, room, log_id] () {
            return topic_tailer_[room]->GetLogInfo(log_id);
          },
          room,
          &result);
      return st.ok() ? result : st.ToString();
    } else if (args[0] == "logs") {
      // logs  -- information about all logs.
      std::string result;
      Status st =
        options_.msg_loop->MapReduceSync(
          [this] (int room) {
            return topic_tailer_[room]->GetAllLogsInfo();
          },
          [] (std::vector<std::string> infos) {
            return std::accumulate(infos.begin(), infos.end(), std::string());
          },
          &result);
      return st.ok() ? result : st.ToString();
    } else if (args[0] == "tail_seqno" && args.size() == 2) {
      // tail_seqno n  -- find tail seqno for log n.
      char* end = nullptr;
      const LogID log_id { strtoul(&*args[1].begin(), &end, 10) };
      auto done = std::make_shared<port::Semaphore>();
      auto result = std::make_shared<SequenceNumber>();
      auto callback = [&done, &result](Status status, SequenceNumber seqno) {
        *result = seqno;
        done->Post();
      };
      Status st = log_tailer_->FindLatestSeqno(log_id, callback);
      if (st.ok()) {
        if (done->TimedWait(std::chrono::seconds(5))) {
          return std::to_string(*result);
        }
        st = Status::TimedOut();
      }
      return st.ToString();
    } else if (args[0] == "cache" && args.size() == 2) {
      // returns cache configured capacity and current usage
      bool get_capacity = false;
      if (args[1] == "capacity") {
        get_capacity = true;
      } else if (args[1] == "usage") {
        // do nothing
      } else {
        return "Unknown options for cache {capacity | usage}";
      }
      size_t sum = 0;
      for (unsigned int room = 0; room < rooms_.size(); room++) {
        std::string result;
        Status st =
          (get_capacity ?
            options_.msg_loop->WorkerRequestSync(
              [this, room] () {
                return topic_tailer_[room]->GetCacheCapacity();
              },
              room,
              &result) :
            options_.msg_loop->WorkerRequestSync(
              [this, room] () {
                return topic_tailer_[room]->GetCacheUsage();
              },
              room,
              &result)
          );
        if (!st.ok()) {
          return st.ToString();
        }
        sum += std::stol(result); // add up the per-room caches
      }
      return std::to_string(sum);
    }
  }
  return "Unknown info for control tower";
}

std::string ControlTower::SetInfoSync(std::vector<std::string> args) {
  if (args.size() >= 1) {
    if (args[0] == "cache") {
      std::string value = "";
      if (args.size() >= 2 && args[1] == "clear") {
        // clear the cache
        for (unsigned int room = 0; room < rooms_.size(); room++) {
          std::string result;
          Status st =
            options_.msg_loop->WorkerRequestSync(
            [this, room] () {
              return topic_tailer_[room]->ClearCache();
            },
            room,
            &result);
          if (!st.ok()) {
            value.append(result);
          }
        }
      } else if (args.size() >= 3 && args[1] == "capacity") {
        // set new cache with global cache size
        size_t newsize = std::stol(args[2]);

        // Equally distribute the cache among the workers. Also check to
        // see that the new size is not above some resonable limit, e.g. 1TB
        size_t cache_size_per_room = 0;
        if (newsize <= 1024L * 1024L * 1024L * 1024L) {
          if (newsize > 0) {
            cache_size_per_room = std::max(newsize / rooms_.size(), 1024UL);
          }
          for (unsigned int room = 0; room < rooms_.size(); room++) {
            std::string result;
            Status st =
              options_.msg_loop->WorkerRequestSync(
              [this, room, cache_size_per_room] () {
                return
                  topic_tailer_[room]->SetCacheCapacity(cache_size_per_room);
              },
              room,
              &result);
            if (!st.ok()) {
              value.append(result);
            }
          }
        } else {
          value = "Specified cache size is too large";
        }
      } else {
        value = "Unknown command. Use set tower cache { clear | capacity }";
      }
      return value;
    }
  }
  return "Unknown command for control tower";
}

int ControlTower::LogIDToRoom(LogID log_id) const {
  return static_cast<int>(log_id % rooms_.size());
}

std::string CopilotSub::ToString() const {
  std::ostringstream ss;
  ss << "CopilotSub(" << stream_id << ", " << sub_id << ")";
  return ss.str();
}

}  // namespace rocketspeed
