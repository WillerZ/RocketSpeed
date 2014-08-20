//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#include "src/controltower/controltower.h"
#include <map>

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
  msg_loop_(options_.env,
            options_.env_options,
            HostId(options_.hostname, options_.port_number),
            options_.info_log,
            static_cast<ApplicationCallbackContext>(this),
            callbacks_) {
  Log(InfoLogLevel::INFO_LEVEL, options_.info_log,
      "Created a new Control Tower");
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
  const HostId origin = request->GetOrigin();

  // change it to a response ack message
  request->SetMetaType(MessageMetadata::MetaType::Response);

  // send reponse back to client
  Status st = ct->msg_loop_.GetClient().Send(origin, std::move(msg));
  if (!st.ok()) {
    Log(InfoLogLevel::INFO_LEVEL, ct->options_.info_log,
        "Unable to send Metadata response to %s:%d",
        origin.hostname.c_str(), origin.port);
  } else {
    Log(InfoLogLevel::INFO_LEVEL, ct->options_.info_log,
        "Send Metadata response to %s:%d",
        origin.hostname.c_str(), origin.port);
  }
  ct->options_.info_log->Flush();
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
