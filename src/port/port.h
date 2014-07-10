//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#ifndef STORAGE_ROCKETSPEED_PORT_PORT_H_
#define STORAGE_ROCKETSPEED_PORT_PORT_H_

#include <string.h>

// Include the appropriate platform specific file below.  If you are
// porting to a new platform, see "port_example.h" for documentation
// of what the new port_<platform>.h file must provide.
#if defined(ROCKETSPEED_PLATFORM_POSIX)
#  include "src/port/port_posix.h"
#endif

#endif  // STORAGE_ROCKETSPEED_PORT_PORT_H_
