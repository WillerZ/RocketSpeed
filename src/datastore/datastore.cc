//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#include "include/Logger.h"
#include "datastore_impl.h"
#include "src/port/port.h"
#include "src/util/common/coding.h"
#include "src/util/common/fixed_configuration.h"

namespace rocketspeed {

DataStoreImpl::DataStoreImpl(
  std::unique_ptr<ClientImpl> client,
  bool create_new,
  std::shared_ptr<Logger> info_log) :
  info_log_(info_log),
  datastore_namespace_(SytemNamespacePermanent),
  datastore_topic_(DefaultDataStoreTopicName()),
  create_new_(create_new),
  start_point_(create_new ? 0 : 1),
  msgid_(MsgId()),
  num_fetched_(0),
  timeout_(1),
  wait_at_startup_(true),
  rawdata_version_(0),
  rs_client_(std::move(client)) {

  // If the subscription request was unsuccessful, then invoke
  // user-specified callback with error state.
  auto subscribe_callback = [this] (const SubscriptionStatus& ss) {
    MutexLock lock(&mutex_);
    if (!ss.GetStatus().ok()) {
      // mark datastore as bad
      db_status_ = Status::IOError(ss.GetStatus().ToString());
    }
  };

  // Receive all messages from this topic and update our
  // in-memory data base
  auto receive_callback = [this] (std::unique_ptr<MessageReceived>& msg) {
    KeyValue rmsg;
    rmsg.DeSerialize(msg->GetContents());
    {
      MutexLock lock(&mutex_);
      if (rmsg.GetKey() == "_create_new") {
        LOG_INFO(info_log_, "DB received delete-all request.");
        rawdata_.clear(); // forget all earlier records
      } else {
        LOG_INFO(info_log_, "DB received key %s value %s",
                 rmsg.GetKey().c_str(), rmsg.GetValue().c_str());
        rawdata_[rmsg.GetKey()] = rmsg.GetValue();
      }
      rawdata_version_++; // new version of database
    }
    num_fetched_++;
    sem_.Post();
  };

  // start the client
  rs_client_->SetDefaultCallbacks(subscribe_callback, receive_callback);

  // send a subscription request for rollcall topic
  auto handle = rs_client_->Client::Subscribe(SystemTenant,
                                              datastore_namespace_,
                                              datastore_topic_,
                                              start_point_);
  if (!handle) {
    // mark datastore as bad
    db_status_ = Status::IOError("Failed to subscribe");
  }
  // Handle will be lost, but the only way we unsubscribe is by shutting down
  // the client.

  // If we are creating a new database, then insert a metadata record
  // to indicate that all previous entries are invalid.
  if (create_new) {
    PutInternal("_create_new", "", true);
  }
}

Status
DataStoreImpl::Get(const std::string& key, std::string* value) {
  // wait till we are reasonably sure that we have received all the records
  WaitForData();

  MutexLock lock(&mutex_);

  // If we were not able to subscribe successfully, then it is an error.
  if (!db_status_.ok()) {
    return db_status_;
  }
  std::map<std::string, std::string>::iterator iter =
                                      rawdata_.find(key);
  if (iter == rawdata_.end()) {
    return Status::NotFound();
  }
  value->assign(iter->second);
  return Status::OK();
}

Status
DataStoreImpl::Put(const std::string& key, const std::string& value) {
  // Wait till we are reasonably sure that we have received all the records
  // This is paranoia, it is best if the first interaction with the
  // database via Get/Put block indefinitely if new datastore records
  // are contunuously flowing in.
  WaitForData();

  return PutInternal(key, value, false);
}

Status
DataStoreImpl::PutInternal(const std::string& key, const std::string& value,
                           bool is_internal) {

  if (!is_internal && key[0] == DataStoreImpl::METAKEY) {
    return Status::InvalidArgument("Key name cannot start with '_'");
  }

  // Serialize the entry
  KeyValue impl(key, value);
  std::string serial;
  impl.Serialize(&serial);
  Slice sl(serial);

  port::Semaphore waitfor;
  Status return_status;

  auto publish_callback = [this, &waitfor, &return_status]
                          (std::unique_ptr<ResultStatus> status) {
    if (!status->GetStatus().ok()) {
      return_status = status->GetStatus();
    }
    waitfor.Post();
  };

  // write it out to rollcall topic
  Status status =  rs_client_->Publish(SystemTenant,
                             datastore_topic_,
                             datastore_namespace_,
                             datastore_topic_options_,
                             sl,
                             publish_callback,
                             msgid_).status;

  if (!status.ok()) {
    return status;           // return error state
  } else {
    waitfor.Wait();          // wait for confirmation
  }
  return return_status;
}

size_t
DataStoreImpl::NumRecords() {
  MutexLock lock(&mutex_);
  return rawdata_.size();
}

// Wait till database is initialized
void
DataStoreImpl::WaitForData() {
  long long current;
  bool success;

  // Wait till initial subscription request has been acknowledged
  {
    MutexLock lock(&mutex_);

    //  do a quick check without lock
    if (!wait_at_startup_) {
      return;
    }
  }
  // Wait till most data has been read in from topic. We do this
  // by waiting to see if there were no new meesages trickling in
  // for the last 'timeout' period.
  do {
    current = num_fetched_;
    success = sem_.TimedWait(timeout_);
    info_log_->Flush();
  } while (current != num_fetched_ && success);

  // Set wait_at_startup_ to false so that we never have to wait again
  {
    MutexLock lock(&mutex_);
    wait_at_startup_ = false;
  }
}

// write this object into a serialized string
void
KeyValue::Serialize(std::string* buffer) {
  PutFixed8(buffer, version_);
  PutFixed8(buffer, entry_type_);
  PutLengthPrefixedSlice(buffer, Slice(key_));
  PutLengthPrefixedSlice(buffer, Slice(value_));
}

// initialize this object from a serialized string
Status
KeyValue::DeSerialize(Slice in) {
  Slice sl;
  // extract the version
  if (!GetFixed8(&in, reinterpret_cast<uint8_t *>(&version_))) {
   return Status::InvalidArgument("DataStore:Bad version");
  }

  // Is this a subscription or unsubscription request?
  if (!GetFixedEnum8(&in, &entry_type_)) {
   return Status::InvalidArgument("DataStore:Bad subscription type");
  }

  // update key
  if (!GetLengthPrefixedSlice(&in, &sl)) {
   return Status::InvalidArgument("DataStore:Bad key");
  }
  key_.clear();
  key_.append(sl.data(), sl.size());

  // update value
  if (!GetLengthPrefixedSlice(&in, &sl)) {
   return Status::InvalidArgument("DataStore:Bad value");
  }
  value_.clear();
  value_.append(sl.data(), sl.size());
  return Status::OK();
}

// create an iterator
std::unique_ptr<Iterator>
DataStoreImpl::CreateNewIterator() {
  IteratorImpl* it = new IteratorImpl(this);
  return std::unique_ptr<Iterator>(it);
}

IteratorImpl::IteratorImpl(DataStoreImpl *db) :
  db_(db), is_valid_(false), iter_version_(0),
  lastread_key_(""), lastread_value_("") {
}

Status
IteratorImpl::SeekInternal(bool first, const std::string& target)  {
  db_->mutex_.AssertHeld();
  if (first) {
    iter_ = db_->rawdata_.begin();      // first entry of database
  } else {
    iter_ = db_->rawdata_.lower_bound(target);
  }
  if (iter_ == db_->rawdata_.end()) {
    is_valid_ = false;
    return Status::NotFound("");
  }
  is_valid_ = true;
  iter_version_ = db_->rawdata_version_; // remember database version
  lastread_key_ = iter_->first;         // cache key/value
  lastread_value_ = iter_->second;
  return Status::OK();
}

Status
IteratorImpl::Seek()  {
  MutexLock lock(&db_->mutex_);
  return SeekInternal(true, "");
}

Status
IteratorImpl::Seek(const std::string& target)  {
  MutexLock lock(&db_->mutex_);
  return SeekInternal(false, target);
}

Status
IteratorImpl::Next() {
  MutexLock lock(&db_->mutex_);
  if (!is_valid_) {
    return Status::NotFound("");
  }
  assert(iter_version_ <= db_->rawdata_version_);

  // If the database version has changed, then reseek
  if (iter_version_ != db_->rawdata_version_) {
    return SeekInternal(false, lastread_key_);
  }

  // The database has not changed since we created the iterator.
  // Increment iterator.
  iter_++;

  // If we have reached the end, then return NotFound.
  if (iter_ == db_->rawdata_.end()) {
    is_valid_ = false;
    return Status::NotFound("");
  }
  // cache last read keys
  lastread_key_ = iter_->first;         // cache key/value
  lastread_value_ = iter_->second;
  return Status::OK();
}

const std::string&
IteratorImpl::key() const {
  MutexLock lock(&db_->mutex_);
  return lastread_key_;
}

const std::string&
IteratorImpl::value() const {
  MutexLock lock(&db_->mutex_);
  return lastread_value_;
}

// Static method to open a DataStore
Status
DataStore::Open(HostId machine,
  bool create_new,
  std::shared_ptr<Logger> info_log,
  std::unique_ptr<DataStore>* stream) {
  std::unique_ptr<ClientImpl> client;

  // create a configuration
  ClientOptions options;
  options.config = std::make_shared<FixedConfiguration>(machine, machine);

  // open the client
  Status status = ClientImpl::Create(
                     std::move(options), &client, true);
  if (!status.ok()) {
    return status;
  }

  // create our object
  stream->reset(new DataStoreImpl(std::move(client),
                                  create_new,
                                  info_log));
  return  Status::OK();
}

}  // namespace rocketspeed
