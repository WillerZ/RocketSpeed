//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#include "src/messages/msg_client.h"
#include <utility>

namespace rocketspeed {
/**
 * Private constructor for a MsgClient
 */
MsgClient::MsgClient(const Env* env,
                     const EnvOptions& env_options,
                     const std::shared_ptr<Logger>& info_log) :
  env_(env),
  env_options_(env_options),
  info_log_(info_log) {
  Log(InfoLogLevel::INFO_LEVEL, info_log,
      "Created a new Client ");
}

// Lookup and lock an entry from the connection cache if it exists
MsgClient::Entry*
MsgClient::lookup(const HostId& host) {
  MutexLock lock(&cache_lock_);
  std::map<HostId, unique_ptr<MsgClient::Entry>>::iterator iter =
                                      connections_cache_.find(host);

  // if item is not in cache, return nullptr
  if (iter == connections_cache_.end()) {
    return nullptr;
  }
  MsgClient::Entry* e = iter->second.get();

  // lock this entry
  e->entry_lock.Lock();
  return e;
}

// remove an entry from the connection cache if it exists
void
MsgClient::remove(const HostId& host) {
  MutexLock lock(&cache_lock_);
  auto iter = connections_cache_.find(host);
  if (iter != connections_cache_.end()) {
    // Lock the entry so that other threads cannot use it.
    iter->second.get()->entry_lock.Lock();

    // Move it out of the map so we can unlock the mutex after removing it.
    std::unique_ptr<Entry> e = std::move(iter->second);

    // Remove (the now null) entry from map.
    connections_cache_.erase(host);

    // And unlock.
    e->entry_lock.Unlock();
  }
}

// release the lock on an entry previously acquired via lookup
void
MsgClient::release(MsgClient::Entry* entry) {
  entry->entry_lock.Unlock();
}

// Insert an new entry in the lookup cache. If the entry already
// exists return error.
Status
MsgClient::insert(const HostId& host, unique_ptr<MsgClient::Entry> entry) {
  MutexLock lock(&cache_lock_);

  // Try to insert into cache
  auto ret = connections_cache_.insert(
               std::pair<HostId, unique_ptr<MsgClient::Entry>>
                         (host, std::move(entry)));

  if (ret.second) {  // inserted successfully
    return Status::OK();
  }
  return Status::NotFound();
}

// Fetches a new connection
MsgClient::Entry*
MsgClient::GetConnection(const HostId& host) {
  // if entry already exists, return
  Entry* f = lookup(host);
  if (f != nullptr) {
    return f;
  }

  // create and initialize new entry
  unique_ptr<MsgClient::Entry> e(new MsgClient::Entry());
  Status st =  env_->NewConnection(host.hostname, host.port, false,
                                   &e->connection,
                                   env_options_);
  if (!st.ok()) {
    return nullptr;
  }

  // Try to insert it back into the cache.
  // If insertion is successful, then there is nothing more to do
  insert(host, std::move(e));

  // Do another lookup.
  return lookup(host);
}

Status
MsgClient::Send(const HostId& host, Message* msg) {
  return Send(host, msg->Serialize());
}

Status
MsgClient::Send(const HostId& host, unique_ptr<Message> msg) {
  return Send(host, msg.get());
}

Status
MsgClient::Send(const HostId& host, Slice msg) {
  // We retry once because the first attempt may have failed due to having
  // a stale connection in the cache. In that case, we remove the connection
  // and retry.
  int retries = 1;
  Status st;
  do {
    MsgClient::Entry* entry = GetConnection(host);
    if (entry == nullptr) {
      st = Status::IOError("Unable to connect to host ", host.hostname);
      continue;
    }
    st = entry->connection->Send(msg);
    release(entry);

    if (!st.ok()) {
      // Send failed, remove the connection.
      // This closes the fd, and GetConnection will re-open.
      if (retries) {
        Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "Send to %s:%d failed (%s) -- reconnecting",
          host.hostname.c_str(), host.port, st.ToString().c_str());
      }
      remove(host);
    }
  } while (!st.ok() && retries--);

  if (st.ok()) {
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "MsgClient sent msg ok");
  } else {
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "MsgClient sent msg fail");
  }
  info_log_->Flush();
  return st;
}

}  // namespace rocketspeed
