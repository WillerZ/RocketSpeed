//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#if defined(OS_MACOSX)
#include "src/port/port.h"

/*
** We are using libevent 2.0.* on android which does not support
** the method libevent_global_shutdown.
** https://raw.githubusercontent.com/libevent/libevent/master/whatsnew-2.1.txt
*/
void ld_libevent_global_shutdown(void) {
}

#endif /* OS_ANDROID */
