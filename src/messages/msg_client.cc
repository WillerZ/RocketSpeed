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
  Status st =  env_->NewConnection(host.hostname, host.port,
                                   &e->connection,
                                   env_options_);
  if (!st.ok()) {
    return nullptr;
  }

  e->entry_lock.Lock();  // lock new entry
  MsgClient::Entry* entry = e.get();

  // Try to insert it back into the cache.
  // If insertion is successful, then there is nothing more to do
  st = insert(host, std::move(e));
  if (st.ok()) {
    return entry;
  }

  // insertion was not successful because a racing thread might have
  // already inserted this entry. Do another lookup.
  return lookup(host);
}

Status
MsgClient::Send(const HostId& host, Message* msg) {
  MsgClient::Entry* entry = GetConnection(host);
  if (entry == nullptr) {
    return Status::IOError("Unable to connect to host ", host.hostname);
  }
  Status st =  entry->connection->Send(msg->Serialize());
  release(entry);
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

Status
MsgClient::Send(const HostId& host, unique_ptr<Message> msg) {
  MsgClient::Entry* entry = GetConnection(host);
  if (entry == nullptr) {
    return Status::IOError("Unable to connect to host ", host.hostname);
  }
  Status st =  entry->connection->Send(msg->Serialize());
  release(entry);
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
