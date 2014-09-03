// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <errno.h>
#include <string.h>
#include <string>
#include <map>
#include <memory>
#include "include/Env.h"
#include "include/Status.h"
#include "include/Types.h"
#include "src/port/port.h"
#include "src/messages/messages.h"
#include "src/util/logging.h"
#include "src/util/log_buffer.h"
#include "src/util/auto_roll_logger.h"
#include "src/util/mutexlock.h"
/**
 * This is the client-side api to interact with the MsgLoop
 * This class is thread-safe and its methods can be invoked
 * from multiple threads simultaneously.
 */
namespace rocketspeed {

class MsgClient {
 public:
  //
  // Client to communicate with MsgLoop
  MsgClient(const Env* env,
            const EnvOptions& env_options,
            const std::shared_ptr<Logger>& info_log);

  // Send a message to the destination. The first version is
  // used for messages allocated on the stack while the second
  // version is used for message allocated on the stack.
  Status Send(const HostId& destination, Message* msg);
  Status Send(const HostId& destination, unique_ptr<Message> msg);

  // Each entry in the connection cache has two things. The first is the
  // connection object that is connected to the specified HostId. The
  // entry_lock protects access to this entry.
  struct Entry {
    unique_ptr<Connection> connection;
    port::Mutex entry_lock;
  };

 private:
  const Env* env_;
  const EnvOptions env_options_;
  const std::shared_ptr<Logger> info_log_;

  // a cache of HostId to connections
  std::map<HostId, unique_ptr<Entry>> connections_cache_;

  // a lock to protect insertion and deletion from the connection_cache
  port::Mutex cache_lock_;

  // Lookup and lock an entry from the connection cache if it exists
  // Otherwise return null.
  Entry* lookup(const HostId& remote);

  // Removes an entry from the cache, e.g. if the connection closed.
  void remove(const HostId& remote);

  // Release the lock on an entry that was earlier
  // retrieved via a call to lookup()
  void release(Entry* entry);

  // Insert an new entry in the lookup cache. If the entry already
  // exists, then return error.
  Status insert(const HostId& host, unique_ptr<Entry> entry);

  // Fetches a connection entry from cache. On sucess, the entry is
  // locked. On error, returns nullptr.
  Entry* GetConnection(const HostId& host);
};
}  // namespace rocketspeed
