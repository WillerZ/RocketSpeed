// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "include/Centrifuge.h"
#include "src/tools/centrifuge/centrifuge.h"
#include <cstdlib>

namespace rocketspeed {

// This is initialised in RunCentrifugeClient.
std::shared_ptr<Logger> centrifuge_logger;

CentrifugeOptions::CentrifugeOptions() {
}

void CentrifugeError(Status st) {
  RS_ASSERT(centrifuge_logger);
  LOG_ERROR(centrifuge_logger, "%s", st.ToString().c_str());
}

void CentrifugeFatal(Status st) {
  RS_ASSERT(centrifuge_logger);
  LOG_FATAL(centrifuge_logger, "%s", st.ToString().c_str());
  std::_Exit(1);
}

}  // namespace rocketspeed
