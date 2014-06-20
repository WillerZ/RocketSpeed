/**
 * @file
 * @version 1.0
 *
 * @section LICENSE
 *
 * Copyright Facebook 2014
 *
 * @section DESCRIPTION
 *
 * This file defines the various data structures needed to access
 * the RocketSpeed pub-sub service.
 */
#pragma once

#include "rocketspeed/include/Status.h"

namespace facebook { namespace rocketspeed {

/**
 * Each Topic is a string
 */
typedef std::string Topic;

/**
 * Each message has a sequence number associated with it.
 * A newly produced message has a higher sequence number than
 * a message produced earlier.
 */
typedef uint64_t SequenceNumber;

/**
 * This is the status returned when a new message is published.
 */
class ResultStatus {
 public:
  Status status;
  SequenceNumber seqno;

  ResultStatus(): seqno(0) {
  };
};

/**
 *  Type of callback that is called when a message arrives
 *
 *  @param data.status OK if this message is received successfully
 *  @param data.seqno Sequence Number of message
 *  @param data.name Name of topic
 *  @param data.payload  The message data itself.
 */
class MessageReceived {
 public:
  Status status;
  SequenceNumber seqno;
  Topic name;
  std::string payload;
};
typedef std::function<void(std::vector<MessageReceived>& data)> ReceiveCallback;


}}
