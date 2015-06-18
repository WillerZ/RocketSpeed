//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#include "src/controltower/tower.h"

#include <map>
#include <numeric>
#include <thread>
#include <vector>

#include "src/util/auto_roll_logger.h"
#include "src/util/logging.h"
#include "src/util/log_buffer.h"
#include "src/util/storage.h"

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
  options_(SanitizeOptions(options)),
  hostmap_(options.max_number_of_hosts),
  hostworker_(new std::atomic<int>[options.max_number_of_hosts]),
  tower_id_(options_.msg_loop->GetHostId().ToClientId()) {
  // The rooms and that tailers are not initialized here.
  // The reason being that those initializations could fail and
  // return error Status.
  for (unsigned int i = 0; i < options.max_number_of_hosts; ++i) {
    hostworker_[i].store(-1, std::memory_order_release);
  }
  options_.msg_loop->RegisterCallbacks(InitializeCallbacks());
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

  // Now create the TopicTailer.
  // One per room with one reader each.
  size_t reader_id = 0;
  for (size_t i = 0; i < num_rooms; ++i) {
    auto on_message =
      [this, i] (std::unique_ptr<Message> msg,
                 std::vector<HostNumber> hosts) {
        Status status = rooms_[i]->OnTailerMessage(std::move(msg),
                                                   std::move(hosts));
        if (!status.ok()) {
          LOG_WARN(options_.info_log,
            "OnTailerMessage failed for room %zu (%s)",
            i, status.ToString().c_str());
        }
      };
    TopicTailer* topic_tailer;
    st = TopicTailer::CreateNewInstance(opt.env,
                                        options_.msg_loop,
                                        int(i),
                                        log_tailer_.get(),
                                        opt.log_router,
                                        opt.info_log,
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

// A callback method to process MessageMetadata
// The message is forwarded to the appropriate ControlRoom
void
ControlTower::ProcessMetadata(std::unique_ptr<Message> msg, StreamID origin) {
  options_.msg_loop->ThreadCheck();

  // get the request message
  MessageMetadata* request = static_cast<MessageMetadata*>(msg.get());
  if (request->GetMetaType() != MessageMetadata::MetaType::Request) {
    LOG_WARN(options_.info_log,
        "MessageMetadata with bad type %d received, ignoring...",
        request->GetMetaType());
  }

  // Process each topic
  for (size_t i = 0; i < request->GetTopicInfo().size(); i++) {
    // map the topic to a logid
    TopicPair topic = request->GetTopicInfo()[i];
    LogID logid;
    Status st = options_.log_router->GetLogID(topic.namespace_id,
                                              topic.topic_name,
                                              &logid);
    if (!st.ok()) {
      LOG_WARN(options_.info_log,
          "Unable to map Topic(%s,%s) to logid %s",
          topic.namespace_id.c_str(),
          topic.topic_name.c_str(),
          st.ToString().c_str());
      continue;
    }
    // calculate the destination room number
    int room_number = LogIDToRoom(logid);

    // Copy out only the ith topic into a new message.
    MessageMetadata* newmsg = new MessageMetadata(
                            request->GetTenantID(),
                            request->GetMetaType(),
                            std::vector<TopicPair> {topic});
    std::unique_ptr<Message> newmessage(newmsg);

    // forward message to the destination room
    ControlRoom* room = rooms_[room_number].get();
    int worker_id = options_.msg_loop->GetThreadWorkerIndex();
    st = room->Forward(std::move(newmessage), worker_id, origin);
    if (!st.ok()) {
      LOG_WARN(options_.info_log,
          "Unable to forward %ssubscription for Topic(%s,%s)@%" PRIu64
          " to rooms-%u (%s)",
          topic.topic_type == MetadataType::mSubscribe ? "" : "un",
          topic.namespace_id.c_str(),
          topic.topic_name.c_str(),
          topic.seqno,
          room_number,
          st.ToString().c_str());
    } else {
      LOG_DEBUG(options_.info_log,
          "Forwarded %ssubscription for Topic(%s,%s)@%" PRIu64 " to rooms-%u",
          topic.topic_type == MetadataType::mSubscribe ? "" : "un",
          topic.namespace_id.c_str(),
          topic.topic_name.c_str(),
          topic.seqno,
          room_number);
    }
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
        MessageGap gap(req->GetTenantID(),
                       req->GetNamespace(),
                       req->GetTopicName(),
                       GapType::kBenign,
                       0,
                       seqno - 1);
        status = options_.msg_loop->SendResponse(gap, origin, worker_id);
        if (status.ok()) {
          LOG_DEBUG(options_.info_log,
            "Sent latest seqno gap 0-%" PRIu64 " to %llu for Topic(%s,%s)",
            seqno - 1,
            origin,
            req->GetNamespace().c_str(),
            req->GetTopicName().c_str());
        } else {
          LOG_WARN(options_.info_log,
            "Failed to send latest seqno gap to %llu for Topic(%s,%s)",
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
    new ExecuteCommand([this, room, log_id, callback] () mutable {
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
  st = options_.msg_loop->SendCommand(std::move(cmd), room);
  if (!st.ok()) {
    LOG_ERROR(options_.info_log,
      "Failed to enqueue command to find latest seqno on Log(%" PRIu64 "): %s",
      log_id, st.ToString().c_str());
  }
}

// A callback method to process MessageMetadata
// The message is forwarded to the appropriate ControlRoom
void
ControlTower::ProcessGoodbye(std::unique_ptr<Message> msg, StreamID origin) {
  options_.msg_loop->ThreadCheck();

  // get the request message
  MessageGoodbye* goodbye = static_cast<MessageGoodbye*>(msg.get());

  for (int i = 0; i < options_.msg_loop->GetNumWorkers(); ++i) {
    // forward message to the destination room
    std::unique_ptr<Message> new_msg(
      new MessageGoodbye(goodbye->GetTenantID(),
                         goodbye->GetCode(),
                         goodbye->GetOriginType()));
    Status st = rooms_[i]->Forward(std::move(new_msg), -1, origin);
    if (!st.ok()) {
      LOG_WARN(options_.info_log,
          "Unable to forward goodbye to rooms-%d (%s)",
          i, st.ToString().c_str());
    } else {
      LOG_DEBUG(options_.info_log,
          "Forwarded goodbye to rooms-%d",
          i);
    }
  }
}

// A static method to initialize the callback map
std::map<MessageType, MsgCallbackType>
ControlTower::InitializeCallbacks() {
  // create a temporary map and initialize it
  std::map<MessageType, MsgCallbackType> cb;
  cb[MessageType::mMetadata] = [this] (std::unique_ptr<Message> msg,
                                       StreamID origin) {
    ProcessMetadata(std::move(msg), origin);
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

HostNumber ControlTower::LookupHost(StreamID origin, int* out_worker_id) const {
  HostNumber hostnum = hostmap_.Lookup(origin);
  if (hostnum != -1) {
    assert(static_cast<unsigned int>(hostnum) < options_.max_number_of_hosts);
    *out_worker_id = hostworker_[hostnum].load(std::memory_order_acquire);
  }
  return hostnum;
}

StreamID ControlTower::LookupHost(HostNumber hostnum,
                                  int* out_worker_id) const {
  StreamID origin = hostmap_.Lookup(hostnum);
  if (hostnum != -1) {
    assert(static_cast<unsigned int>(hostnum) < options_.max_number_of_hosts);
    *out_worker_id = hostworker_[hostnum].load(std::memory_order_acquire);
  }
  return origin;
}

HostNumber ControlTower::InsertHost(StreamID origin, int worker_id) {
  HostNumber hostnum = hostmap_.Insert(origin, hostworker_.get(), worker_id);
  if (hostnum != -1) {
    assert(static_cast<unsigned int>(hostnum) < options_.max_number_of_hosts);
  }
  return hostnum;
}

Statistics ControlTower::GetStatisticsSync() {
  Statistics stats = options_.msg_loop->AggregateStatsSync(
    [this] (int i) { return topic_tailer_[i]->GetStatistics(); });
  stats.Aggregate(options_.msg_loop->GetStatisticsSync());
  return stats;
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
    }
  }
  return "Unknown info for control tower";
}

int ControlTower::LogIDToRoom(LogID log_id) const {
  return static_cast<int>(log_id % rooms_.size());
}

}  // namespace rocketspeed
