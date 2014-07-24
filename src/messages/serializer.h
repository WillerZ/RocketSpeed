// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <string>
#include "include/Slice.h"
#include "include/Status.h"

namespace rocketspeed {

class MessageHeader {
 public:
  /**
   * The Serializer and DeSerialier can use the 'version' field to determine
   * how best to serialize/deserialize the object
   */
  char version_;

  /**
   *  The total size of data + header of this message
   */
  uint32_t msgsize_;

  /**
   * Returns the serialized size of the message header
   */
  static unsigned int GetSize() {
    return (sizeof(version_) + sizeof(msgsize_));
  }

  MessageHeader() {}

  /**
   * Given a serialized header, convert it to a real object
   */
  explicit MessageHeader(Slice* in);
};

class Serializer {
 public:
  /**
   * Serializes an object. The lifecycle of the returned slice is tied to the
   * lifecycle of this object.
   *
   * @return Returns the serialized version of this object
   *
   */
  virtual Slice Serialize() const = 0;

  /**
   * Deserializes an object. Populates the current object with the
   * contents from the serialized data.
   *
   * @param in The serialized version of an object
   * @return Returns OK() on success, otherwise returns failure
   *
   */
  virtual Status DeSerialize(Slice* in) = 0;

  virtual ~Serializer() {}

 protected:
  /**
   * The Message Header
   */
  mutable MessageHeader msghdr_;

  // This buffer is here to avoid malloc/free of tmp space at
  // every serialization/deserialization
  mutable std::string serialize_buffer__;
};

}  // namespace rocketspeed
