// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <limits>

#include "src/messages/stream_socket.h"
#include "src/util/id_allocator.h"

namespace rocketspeed {

/** An allocator for stream IDs. */
class StreamAllocator : public IDAllocator<StreamID, StreamAllocator> {
  using Base = IDAllocator<StreamID, StreamAllocator>;

 public:
  using Base::Base;
};

}  // namespace rocketspeed
