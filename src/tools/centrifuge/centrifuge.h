// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include "include/Logger.h"
#include <memory>

namespace rocketspeed {

/** Logger used by CentrifugeError/CentrifugeFatal. */
extern std::shared_ptr<Logger> centrifuge_logger;

}  // namespace rocketspeed
