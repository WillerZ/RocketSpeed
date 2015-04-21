// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>

#include "include/RocketSpeed.h"
#include "src/rollcall/RollCall.h"
#include "src/client/client.h"
#include "src/util/common/coding.h"

/**
 * This is the Rollcall interface. Applications can use this interface
 * to list all the topics that all clients are subscribed to.
 */
namespace rocketspeed {

//
// The states of the Rollcall Reader
//
enum ReaderState : char {
  Invalid                 = 0x00,
  SubscriptionRequestSent = 0x01,
  SubscriptionConfirmed   = 0x02,
};

/*
 * An implementation for the Rollcall stream.
 * For reading the rollcall, register a callback via the constructor
 * For writing to the rollcall, use RollcallImpl::WriteEntry.
 */
class RollcallImpl : public RollcallStream {
 public:
  // Constructor
  RollcallImpl(std::unique_ptr<ClientImpl> client,
               const TenantID tenant_id,
               const NamespaceID& nsid,
               const SubscriptionStart& start_point,
               RollCallback callback);

  // Write an entry to the rollcall topic.
  Status WriteEntry(const TenantID tenant_id,
                    const Topic& topic_name,
                    const NamespaceID& nsid,
                    bool isSubscription,
                    PublishCallback publish_callback);

   // Closes resources associated with this rollcall stream reader
  virtual ~RollcallImpl() = default;

  // Create the rollcall topic name for a namespace
  static Topic GetRollcallTopicName(const NamespaceID& nsid) {
    std::string topic = "r";
    PutLengthPrefixedSlice(&topic, Slice(nsid));
    topic.append(".");
    return topic;
  }

 private:
  const std::unique_ptr<ClientImpl> rs_client_; // rocket speed client
  const NamespaceID nsid_;             // namespace
  const SubscriptionStart start_point_;// start seqno of rollcall topic
  RollCallback callback_;             // callback specified by application
  ReaderState state_;                 // the current state of this reader
  const Topic rollcall_topic_;        // name of the rollcall topic
  const TopicOptions rollcall_topic_options_;
  const MsgId msgid_;

  // The rollcall topic resides in namespace '_rollcall'.
  static const NamespaceID rollcall_namespace_;

  void Serialize(std::string* buffer);
  Status DeSerialize(const Slice& in);

};

}  // namespace rocketspeed
