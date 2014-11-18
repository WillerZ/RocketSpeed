//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#include "src/controltower/tower.h"

#include <map>
#include <thread>
#include <vector>

#include "src/util/auto_roll_logger.h"
#include "src/util/logging.h"
#include "src/util/log_buffer.h"
#include "src/util/storage.h"

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
      result.info_log = nullptr;
    }
  }
  return result;
}

/**
 * Private constructor for a Control Tower
 */
ControlTower::ControlTower(const ControlTowerOptions& options):
  options_(SanitizeOptions(options)),
  log_router_(options.log_range.first, options.log_range.second),
  hostmap_(options.max_number_of_hosts),
  tower_id_(options_.msg_loop->GetHostId().ToClientId()) {
  // The rooms and that tailers are not initialized here.
  // The reason being that those initializations could fail and
  // return error Status.

  options_.msg_loop->RegisterCallbacks(InitializeCallbacks());
}

ControlTower::~ControlTower() {
  // The ControlRooms use the msg_loop.MsgClient to send messages.
  // Shutdown all the ControlRooms before shutting down msg_loop.
  rooms_.clear();
  options_.info_log->Flush();
}

/**
 * This is a static method to create a ControlTower
 */
Status
ControlTower::CreateNewInstance(const ControlTowerOptions& options,
                                ControlTower** ct) {
  *ct = new ControlTower(options);
  const ControlTowerOptions opt = (*ct)->GetOptions();

  // Start the LogTailer first.
  Tailer* tailer;
  Status st = Tailer::CreateNewInstance(opt.env, (*ct)->rooms_,
                                        options.storage,
                                        options.storage_url,
                                        opt.info_log,
                                        &tailer);
  if (!st.ok()) {
    delete *ct;
    return st;
  }
  (*ct)->tailer_.reset(tailer);

  // The ControlRooms keep a pointer to the Tailer, so create
  // these after the Tailer is created. The port numbers for
  // the rooms are adjacent to the port number of the ControlTower.
  for (unsigned int i = 0; i < opt.number_of_rooms; i++) {
    unique_ptr<ControlRoom> newroom(new ControlRoom(opt, *ct, i));
    (*ct)->rooms_.push_back(std::move(newroom));
  }

  // Initialize the tailer
  // Needs to be done after constructing rooms.
  st = (*ct)->tailer_->Initialize();
  if (!st.ok()) {
    return st;
  }

  // Start background threads for Room msg loops
  unsigned int numrooms = opt.number_of_rooms;
  for (unsigned int i = 0; i < numrooms; i++) {
    ControlRoom* room = (*ct)->rooms_[i].get();
    opt.env->StartThread(ControlRoom::Run, room,
                  "rooms-" + std::to_string(room->GetRoomNumber()));
  }
  // Wait for all the Rooms to be ready to process events
  for (unsigned int i = 0; i < numrooms; i++) {
    while (!(*ct)->rooms_[i].get()->IsRunning()) {
      std::this_thread::yield();
    }
  }
  return Status::OK();
}

// A callback method to process MessageMetadata
// The message is forwarded to the appropriate ControlRoom
void
ControlTower::ProcessMetadata(std::unique_ptr<Message> msg) {
  options_.msg_loop->ThreadCheck();

  // get the request message
  MessageMetadata* request = static_cast<MessageMetadata*>(msg.get());
  if (request->GetMetaType() != MessageMetadata::MetaType::Request) {
    LOG_WARN(options_.info_log,
        "MessageMetadata with bad type %d received, ignoring...",
        request->GetMetaType());
  }

  // Process each topic
  for (unsigned int i = 0; i < request->GetTopicInfo().size(); i++) {
    // map the topic to a logid
    TopicPair topic = request->GetTopicInfo()[i];
    LogID logid;
    Status st = log_router_.GetLogID(topic.topic_name, &logid);
    if (!st.ok()) {
      LOG_INFO(options_.info_log,
          "Unable to map msg to logid %s", st.ToString().c_str());
      continue;
    }
    // calculate the destination room number
    int room_number = logid % options_.number_of_rooms;

    // Copy out only the ith topic into a new message.
    MessageMetadata* newmsg = new MessageMetadata(
                            request->GetTenantID(),
                            request->GetMetaType(),
                            request->GetOrigin(),
                            std::vector<TopicPair> {topic});
    std::unique_ptr<Message> newmessage(newmsg);

    // forward message to the destination room
    ControlRoom* room = rooms_[room_number].get();
    st = room->Forward(std::move(newmessage), logid);
    if (!st.ok()) {
      LOG_INFO(options_.info_log,
          "Unable to forward subscription for Topic(%s)@%lu to room %u (%s)",
          topic.topic_name.c_str(),
          topic.seqno,
          room_number,
          st.ToString().c_str());
    } else {
      LOG_INFO(options_.info_log,
          "Forwarded subscription for Topic(%s)@%lu to room %u",
          topic.topic_name.c_str(),
          topic.seqno,
          room_number);
    }
  }
}

// A static method to initialize the callback map
std::map<MessageType, MsgCallbackType>
ControlTower::InitializeCallbacks() {
  // create a temporary map and initialize it
  std::map<MessageType, MsgCallbackType> cb;
  cb[MessageType::mMetadata] = [this] (std::unique_ptr<Message> msg) {
    ProcessMetadata(std::move(msg));
  };

  // return the updated map
  return cb;
}

}  // namespace rocketspeed
