//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

// Include the appropriate platform specific file below.
#if defined(ROCKETSPEED_PLATFORM_POSIX)
#include "src/port/port_posix.h"
#else
#error No platform defined. Did you forget to pass -DROCKETSPEED_PLATFORM_*?
#endif
