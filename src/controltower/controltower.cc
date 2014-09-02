//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#include "src/controltower/controltower.h"
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
  // Start background threads for Room msg loops
  for (unsigned int i = 0; i < options_.number_of_rooms; i++) {
    ControlRoom* room = rooms_[i].get();
    options_.env->StartThread(ControlRoom::Run, room,
                  "rooms-" + std::to_string(room->GetRoomId().port));
  }
  // wait for all the Rooms to be ready to process events
  for (unsigned int i = 0; i < options_.number_of_rooms; i++) {
    while (!rooms_[i].get()->IsRunning()) {
      std::this_thread::yield();
    }
  }
  Log(InfoLogLevel::INFO_LEVEL, options_.info_log,
      "Starting a new Control Tower with %d rooms",
      options_.number_of_rooms);
  msg_loop_.Run();
}

/**
 * Private constructor for a Control Tower
 */
ControlTower::ControlTower(const ControlTowerOptions& options,
                           const Configuration& conf):
  options_(SanitizeOptions(options)),
  conf_(conf),
  callbacks_(InitializeCallbacks()),
  log_router_(options.log_count),
  msg_loop_(options_.env,
            options_.env_options,
            HostId(options_.hostname, options_.port_number),
            options_.info_log,
            static_cast<ApplicationCallbackContext>(this),
            callbacks_) {
  //
  // Create the ControlRooms. The port numbers for the rooms
  // are adjacent to the port number of the ControlTower.
  for (unsigned int i = 0; i < options_.number_of_rooms; i++) {
    unique_ptr<ControlRoom> newroom(new ControlRoom(options_, this, i,
                                    options_.port_number + i + 1));
    rooms_.push_back(std::move(newroom));
  }
  options_.info_log->Flush();
}

ControlTower::~ControlTower() {
}

/**
 * This is a static method to create a ControlTower
 */
Status
ControlTower::CreateNewInstance(const ControlTowerOptions& options,
                                const Configuration& conf,
                                ControlTower** ct) {
  *ct = new ControlTower(options, conf);
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
    MessageMetadata newmsg(request->GetTenantID(),
                           request->GetSequenceNumber(),
                           request->GetMetaType(),
                           request->GetOrigin(),
                           std::vector<TopicPair> {topic});


    // forward message to the destination room
    ControlRoom* room = ct->rooms_[room_number].get();
    st =  room->Forward(&newmsg);
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
