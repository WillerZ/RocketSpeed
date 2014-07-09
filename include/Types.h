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

#include <vector>
#include <functional>

#include "include/Status.h"

namespace rocketspeed {

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
 *  @param status OK if this message is received successfully
 *  @param seqno Sequence Number of message
 *  @param name Name of topic
 *  @param payload  The message data itself.
 */
class MessageReceived {
 public:
  Status status;
  SequenceNumber seqno;
  Topic name;
  std::string payload;
};

typedef std::function<void(std::vector<MessageReceived>& data)> ReceiveCallback;

/**
 * An object of the form scheme://host:port/path
 */
typedef std::string URL;

/**
 *  A Configuration that specifies how a client can describe a RocketSpeed Cloud.
 *
 *  @param url The name of the RocketSpeed Cloud Service
 */
class Configuration {
 public:
  URL url; 
};

enum Retention : char {
  OneHour = 0x01,         // keep messages for 1 hour
  OneDay = 0x02,          // keep messages for 1 day
  OneWeek = 0x03,         // keep messages for 1 week
};

/**
 * These are the options associated with a Topic
 *
 *  @param retention The amount of time a message would remain in RocketSpeed
 */
class TopicOptions {
 public:
  Retention retention;
};


}
