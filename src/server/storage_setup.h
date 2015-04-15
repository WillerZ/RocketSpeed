// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>

namespace rocketspeed {

class Env;
class Logger;
class LogRouter;
class LogStorage;

/**
 * Creates the storage LogStorage.
 * Should be implemented by the storage library.
 */
extern std::shared_ptr<LogStorage> CreateLogStorage(
  Env* env,
  std::shared_ptr<Logger> info_log);

/**
 * Create the storage LogRouter.
 * Should be implemented by the storage library.
 */
extern std::shared_ptr<LogRouter> CreateLogRouter();

}  // namespace rocketspeed
