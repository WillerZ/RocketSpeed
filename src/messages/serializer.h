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

class Serializer {
 public:
  /**
   * Serializes an object.
   *
   * @return Returns OK() on success, otherwise returns failure
   *
   */
  virtual Status Serialize(std::string* str) const = 0;

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
};

}  // namespace rocketspeed
