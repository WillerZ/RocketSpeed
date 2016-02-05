// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/proxy/options.h"

namespace rocketspeed {

ProxyOptions::ProxyOptions()
: env(Env::Default())
, conf(nullptr)
, info_log(nullptr)
, num_workers(1)
, ordering_buffer_size(16) {
}

}  // namespace rocketspeed
