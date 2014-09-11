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

void
ControlTower::Run(void) {
  Log(InfoLogLevel::INFO_LEVEL, options_.info_log,
      "Starting a new Control Tower with %d rooms",
      options_.number_of_rooms);
  msg_loop_.Run();
}

/**
 * Private constructor for a Control Tower
 */
ControlTower::ControlTower(const ControlTowerOptions& options,
                           const Configuration* conf):
  options_(SanitizeOptions(options)),
  conf_(conf),
  callbacks_(InitializeCallbacks()),
  log_router_(options.log_range.first, options.log_range.second),
  hostmap_(options.max_number_of_hosts),
  msg_loop_(options_.env,
            options_.env_options,
            HostId(options_.hostname, options_.port_number),
            options_.info_log,
            static_cast<ApplicationCallbackContext>(this),
            callbacks_) {
  // The rooms and that tailers are not initialized here.
  // The reason being that those initializations could fail and
  // return error Status.
}

ControlTower::~ControlTower() {
  // The ControlRooms use the msg_loop.MsgClient to send messages.
  // Shutdown all the ControlRooms before shutting down msg_loop.
  rooms_.clear();
}

/**
 * This is a static method to create a ControlTower
 */
Status
ControlTower::CreateNewInstance(const ControlTowerOptions& options,
                                const Configuration* conf,
                                ControlTower** ct) {
  *ct = new ControlTower(options, conf);
  const ControlTowerOptions opt = (*ct)->GetOptions();

  // Start the LogTailer first.
  Tailer* tailer;
  Status st = Tailer::CreateNewInstance(conf, opt.env,
                                        (*ct)->rooms_, &tailer);
  if (!st.ok()) {
    delete *ct;
    return st;
  }
  (*ct)->tailer_.reset(tailer);

  // The ControlRooms keep a pointer to the Tailer, so create
  // these after the Tailer is created. The port numbers for
  // the rooms are adjacent to the port number of the ControlTower.
  for (unsigned int i = 0; i < opt.number_of_rooms; i++) {
    unique_ptr<ControlRoom> newroom(new ControlRoom(opt, *ct, i,
                                    opt.port_number + i + 1));
    (*ct)->rooms_.push_back(std::move(newroom));
  }

  // Start background threads for Room msg loops
  unsigned int numrooms = opt.number_of_rooms;
  for (unsigned int i = 0; i < numrooms; i++) {
    ControlRoom* room = (*ct)->rooms_[i].get();
    opt.env->StartThread(ControlRoom::Run, room,
                  "rooms-" + std::to_string(room->GetRoomId().port));
  }
  // Wait for all the Rooms to be ready to process events
  for (unsigned int i = 0; i < numrooms; i++) {
    while (!(*ct)->rooms_[i].get()->IsRunning()) {
      std::this_thread::yield();
    }
  }
  return Status::OK();
}

// A static callback method to process MessageData
void
ControlTower::ProcessData(const ApplicationCallbackContext ctx,
                          std::unique_ptr<Message> msg) {
  ControlTower* ct = static_cast<ControlTower*>(ctx);
  fprintf(stdout, "Received data message %d\n", ct->IsRunning());
}

// A static callback method to process MessageMetadata
// The message is forwarded to the appropriate ControlRoom
void
ControlTower::ProcessMetadata(const ApplicationCallbackContext ctx,
                              std::unique_ptr<Message> msg) {
  ControlTower* ct = static_cast<ControlTower*>(ctx);

  // get the request message
  MessageMetadata* request = static_cast<MessageMetadata*>(msg.get());
  if (request->GetMetaType() != MessageMetadata::MetaType::Request) {
    Log(InfoLogLevel::WARN_LEVEL, ct->options_.info_log,
        "MessageMetadata with bad type %d received, ignoring...",
        request->GetMetaType());
  }

  // Process each topic
  for (unsigned int i = 0; i < request->GetTopicInfo().size(); i++) {
    // map the topic to a logid
    TopicPair topic = request->GetTopicInfo()[i];
    LogID logid;
    Status st = ct->log_router_.GetLogID(topic.topic_name, &logid);
    if (!st.ok()) {
      Log(InfoLogLevel::INFO_LEVEL, ct->options_.info_log,
          "Unable to map msg to logid %s", st.ToString().c_str());
      continue;
    }
    // calculate the destination room number
    int room_number = logid % ct->options_.number_of_rooms;

    // Copy out only the ith topic into a new message.
    MessageMetadata* newmsg = new MessageMetadata(
                           request->GetTenantID(),
                            request->GetSequenceNumber(),
                            request->GetMetaType(),
                            request->GetOrigin(),
                            std::vector<TopicPair> {topic});
    std::unique_ptr<Message> newmessage(newmsg);

    // forward message to the destination room
    ControlRoom* room = ct->rooms_[room_number].get();
    st =  room->Forward(std::move(newmessage), logid);
    if (!st.ok()) {
      Log(InfoLogLevel::INFO_LEVEL, ct->options_.info_log,
          "Unable to forward Metadata msg to %s:%d %s",
          room->GetRoomId().hostname.c_str(),
          room->GetRoomId().port,
          st.ToString().c_str());
    } else {
      Log(InfoLogLevel::INFO_LEVEL, ct->options_.info_log,
          "Forwarded Metadata to %s:%d",
          room->GetRoomId().hostname.c_str(),
          room->GetRoomId().port);
    }
  }
}

// A static method to initialize the callback map
std::map<MessageType, MsgCallbackType>
ControlTower::InitializeCallbacks() {
  // create a temporary map and initialize it
  std::map<MessageType, MsgCallbackType> cb;
  cb[MessageType::mData] = MsgCallbackType(ProcessData);
  cb[MessageType::mMetadata] = MsgCallbackType(ProcessMetadata);

  // return the updated map
  return cb;
}

}  // namespace rocketspeed
