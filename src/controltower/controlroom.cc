//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/controltower/controlroom.h"
#include <map>
#include "src/controltower/controltower.h"

namespace rocketspeed {

ControlRoom::ControlRoom(const ControlTowerOptions& options,
                         ControlTower* control_tower,
                         int room_number,
                         int port_number) :
  control_tower_(control_tower),
  room_number_(room_number),
  room_id_(HostId(options.hostname, port_number)),
  callbacks_(InitializeCallbacks()),
  room_loop_(options.env,
            options.env_options,
            room_id_,
            options.info_log,
            static_cast<ApplicationCallbackContext>(this),
            callbacks_) {
}

ControlRoom::~ControlRoom() {
}

// static method to start the loop for processing Room events
void ControlRoom::Run(void* arg) {
  ControlRoom* room = static_cast<ControlRoom*>(arg);
  Log(InfoLogLevel::INFO_LEVEL,
      room->control_tower_->GetOptions().info_log,
      "Starting ControlRoom Loop at port %d", room->room_id_.port);
  room->room_loop_.Run();
}

// The Control Tower uses this method to forward a message to this Room.
Status
ControlRoom::Forward(Message* msg) {
  // Use the client from the msg loop of the control tower because that
  // client has a pool of connections to different hostids.
  return control_tower_->GetClient().Send(room_id_, msg);
}

// A static callback method to process MessageData
void
ControlRoom::ProcessData(const ApplicationCallbackContext ctx,
                          std::unique_ptr<Message> msg) {
  ControlRoom* ct = static_cast<ControlRoom*>(ctx);
  fprintf(stdout, "Received data message %d\n", ct->IsRunning());
}

// A static callback method to process MessageMetadata
void
ControlRoom::ProcessMetadata(const ApplicationCallbackContext ctx,
                              std::unique_ptr<Message> msg) {
  ControlRoom* room = static_cast<ControlRoom*>(ctx);
  ControlTower* ct = room->control_tower_;

  // get the request message
  MessageMetadata* request = static_cast<MessageMetadata*>(msg.get());
  if (request->GetMetaType() != MessageMetadata::MetaType::Request) {
    Log(InfoLogLevel::WARN_LEVEL, ct->GetOptions().info_log,
        "MessageMetadata with bad type %d received, ignoring...",
        request->GetMetaType());
  }
  const HostId origin = request->GetOrigin();

  // change it to a response ack message
  request->SetMetaType(MessageMetadata::MetaType::Response);

  // send reponse back to client
  Status st = ct->GetClient().Send(origin, std::move(msg));
  if (!st.ok()) {
    Log(InfoLogLevel::INFO_LEVEL, ct->GetOptions().info_log,
        "Unable to send Metadata response to %s:%d",
        origin.hostname.c_str(), origin.port);
  } else {
    Log(InfoLogLevel::INFO_LEVEL, ct->GetOptions().info_log,
        "Send Metadata response to %s:%d",
        origin.hostname.c_str(), origin.port);
  }
  ct->GetOptions().info_log->Flush();
}

// A static method to initialize the callback map
std::map<MessageType, MsgCallbackType>
ControlRoom::InitializeCallbacks() {
  // create a temporary map and initialize it
  std::map<MessageType, MsgCallbackType> cb;
  cb[MessageType::mData] = MsgCallbackType(ProcessData);
  cb[MessageType::mMetadata] = MsgCallbackType(ProcessMetadata);

  // return the updated map
  return cb;
}
}  // namespace rocketspeed
