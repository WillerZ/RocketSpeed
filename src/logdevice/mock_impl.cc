// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma GCC diagnostic ignored "-Wshadow"

#include <algorithm>
#include <chrono>
#include <future>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include "include/Assert.h"
#include "include/Slice.h"
#include "include/Env.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/Record.h"

#include "src/messages/msg_loop.h"

namespace facebook { namespace logdevice {

__thread E err = E::OK;

// We don't use errorStrings, so don't care about the contents, but we
// need to define these for LogDevice.
ErrorCodeStringMap errorStrings;

template <>
void ErrorCodeStringMap::setValues() {
}

template <>
const ErrorCodeInfo& ErrorCodeStringMap::invalidValue() {
  static const ErrorCodeInfo invalidErrorCodeInfo {
    "UNKNOWN",
    "invalid error code"
  };
  return invalidErrorCodeInfo;
}

struct LogIDHash {
  size_t operator()(logid_t log_id) const {
    return std::hash<uint64_t>()(log_id.val());
  }
};

class DataStore {
 public:
  explicit DataStore()
  : env_(rocketspeed::Env::Default())
  , msg_loop_(env_,
              rocketspeed::EnvOptions(),
              -1,
              1,
              std::make_shared<rocketspeed::NullLogger>(),
              "ldserver") {
    // LogDevice "server" runs on its own set of threads.
    rocketspeed::Status st = msg_loop_.Initialize();
    RS_ASSERT(st.ok());
    msg_loop_thread_.reset(
      new rocketspeed::MsgLoopThread(env_, &msg_loop_, "ldserver"));
    st = msg_loop_.WaitUntilRunning();
    RS_ASSERT(st.ok());
    (void)st;
  }

  /** All these APIs are thread safe. */
  int append(logid_t logid,
             const Payload& payload,
             append_callback_t cb) noexcept;

  void startReading(uint64_t id,
                    std::function<bool(std::unique_ptr<DataRecord>&)> data_cb,
                    std::function<bool(const GapRecord&)> gap_cb,
                    logid_t log_id,
                    lsn_t from,
                    lsn_t until);

  void stopReading(uint64_t id, logid_t log_id, std::function<void()> cb);

  int trimSync(logid_t logid, lsn_t lsn) noexcept;

  int findTime(logid_t logid,
               std::chrono::milliseconds timestamp,
               find_time_callback_t cb) noexcept;

  struct Record {
    explicit Record(std::string _data,
                    std::chrono::milliseconds _timestamp)
    : data(std::move(_data))
    , timestamp(_timestamp) {}

    std::string data;
    std::chrono::milliseconds timestamp;
  };

  struct Reader {
    std::function<bool(std::unique_ptr<DataRecord>&)> data_cb;
    std::function<bool(const GapRecord&)> gap_cb;
    logid_t log_id;
    lsn_t from;
    lsn_t until;
  };

  struct Log {
    // Linear buffer of records.
    std::vector<Record> records;
    // Start records at 1000 to ensure we aren't assuming anything.
    lsn_t trim = 999;  // trimming doesn't actually erase from 'records'.
    lsn_t first_seqno = 1000;
    std::unordered_map<uint64_t, Reader> readers;
  };

  rocketspeed::Env* env_;
  rocketspeed::MsgLoop msg_loop_;
  std::unique_ptr<rocketspeed::MsgLoopThread> msg_loop_thread_;
  std::unordered_map<logid_t, Log, LogIDHash> logs_;
};

class ClientImpl : public Client {
 public:
  explicit ClientImpl(DataStore* data_store)
  : env_(rocketspeed::Env::Default())
  , msg_loop_(env_,
              rocketspeed::EnvOptions(),
              -1,
              1,
              std::make_shared<rocketspeed::NullLogger>(),
              "ldclient")
  , data_store_(data_store) {
    // Client has its own set of threads.
    rocketspeed::Status st = msg_loop_.Initialize();
    RS_ASSERT(st.ok());
    msg_loop_thread_.reset(
      new rocketspeed::MsgLoopThread(env_, &msg_loop_, "ldclient"));
    st = msg_loop_.WaitUntilRunning();
    RS_ASSERT(st.ok());
    (void)st;
  }

 private:
  friend class Client;
  friend class AsyncReader;
  friend class AsyncReaderImpl;

  rocketspeed::Env* env_;
  std::unique_ptr<ClientSettings> settings_;
  std::chrono::milliseconds timeout_;
  rocketspeed::MsgLoop msg_loop_;
  std::unique_ptr<rocketspeed::MsgLoopThread> msg_loop_thread_;
  DataStore* data_store_;
};

struct DataRecordOwned : public DataRecord {
  explicit DataRecordOwned(std::string data,
                           logid_t log_id,
                           lsn_t lsn,
                           std::chrono::milliseconds timestamp)
  : data_(std::move(data)) {
    payload.data = static_cast<const void*>(data_.data());
    payload.size = data_.size();
    logid = log_id;
    attrs.lsn = lsn;
    attrs.timestamp = timestamp;
    attrs.batch_offset = 0;
  }

 private:
  std::string data_;
};

int DataStore::append(logid_t logid,
                      const Payload& payload,
                      append_callback_t cb) noexcept {
  std::unique_ptr<rocketspeed::Command> cmd(rocketspeed::MakeExecuteCommand(
    [this, logid, payload, cb] () {
      auto now = std::chrono::system_clock::now().time_since_epoch();
      std::string data(static_cast<const char*>(payload.data), payload.size);
      auto& thelog = logs_[logid];
      lsn_t lsn(thelog.first_seqno + thelog.records.size());
      thelog.records.emplace_back(std::move(data),
        std::chrono::duration_cast<std::chrono::milliseconds>(now));
      DataRecord record(logid, payload, lsn);
      cb(E::OK, record);

      // Check for any tail readers.
      for (auto& item : logs_[logid].readers) {
        if (item.second.from == lsn &&
            item.second.from <= item.second.until) {
          auto& rec = thelog.records[lsn - thelog.first_seqno];
          std::unique_ptr<DataRecord> data_rec(
            new DataRecordOwned(rec.data, logid, lsn, rec.timestamp));
          item.second.data_cb(data_rec);
          ++item.second.from;
        }
      }
    }));
  return msg_loop_.SendCommand(std::move(cmd), 0).ok() ? 0 : 1;
}

void DataStore::startReading(
  uint64_t id,
  std::function<bool(std::unique_ptr<DataRecord>&)> data_cb,
  std::function<bool(const GapRecord&)> gap_cb,
  logid_t log_id,
  lsn_t from,
  lsn_t until) {
  std::unique_ptr<rocketspeed::Command> cmd(rocketspeed::MakeExecuteCommand(
    [this, id, data_cb, gap_cb, log_id, from, until] () mutable {
      auto& thelog = logs_[log_id];
      if (from <= thelog.trim) {
        GapRecord rec(log_id, GapType::TRIM, from, thelog.trim);
        if (!gap_cb(rec)) {
          // Try again later.
          // This enqueues the same command asynchronously. Ideally we'd
          // enqueue with a delay, but this is fine for mock impl.
          startReading(id, data_cb, gap_cb, log_id, from, until);
          return;
        }
        from = thelog.trim + 1;
      }
      // Deliver all available records immediately.
      while (from < thelog.first_seqno + thelog.records.size() &&
             from <= until) {
        auto& rec = thelog.records[from - thelog.first_seqno];
        std::unique_ptr<DataRecord> data_rec(
          new DataRecordOwned(rec.data, log_id, from, rec.timestamp));
        if (!data_cb(data_rec)) {
          // Try again later.
          // This enqueues the same command asynchronously. Ideally we'd
          // enqueue with a delay, but this is fine for mock impl.
          startReading(id, data_cb, gap_cb, log_id, from, until);
          return;
        }
        ++from;
      }
      Reader r;
      r.data_cb = data_cb;
      r.gap_cb = gap_cb;
      r.from = from;
      r.until = until;
      logs_[log_id].readers.emplace(id, std::move(r));
    }));
  msg_loop_.SendCommand(std::move(cmd), 0);
}

void DataStore::stopReading(uint64_t id,
                            logid_t log_id,
                            std::function<void()> cb) {
  std::unique_ptr<rocketspeed::Command> cmd(rocketspeed::MakeExecuteCommand(
    [this, id, log_id, cb] () {
      logs_[log_id].readers.erase(id);
      if (cb) {
        cb();
      }
    }));
  msg_loop_.SendCommand(std::move(cmd), 0);
}

int DataStore::trimSync(logid_t logid, lsn_t lsn) noexcept {
  std::unique_ptr<rocketspeed::Command> cmd(rocketspeed::MakeExecuteCommand(
    [this, logid, lsn] () {
      logs_[logid].trim = std::max(logs_[logid].trim, lsn);
    }));
  return msg_loop_.SendCommand(std::move(cmd), 0).ok() ? 0 : 1;
}

int DataStore::findTime(logid_t logid,
                        std::chrono::milliseconds timestamp,
                        find_time_callback_t cb) noexcept {
  std::unique_ptr<rocketspeed::Command> cmd(rocketspeed::MakeExecuteCommand(
    [this, logid, timestamp, cb] () {
      lsn_t lsn = logs_[logid].first_seqno;
      if (timestamp == std::chrono::milliseconds::max()) {
        // std::chrono::milliseconds::max is special case for next record.
        lsn += logs_[logid].records.size();
      } else {
        for (auto& rec : logs_[logid].records) {
          if (rec.timestamp >= timestamp) {
            break;
          }
          ++lsn;
        }
      }
      cb(E::OK, std::max(lsn, LSN_OLDEST));
    }));
  return msg_loop_.SendCommand(std::move(cmd), 0).ok() ? 0 : 1;
}

class AsyncReaderImpl : public AsyncReader {
 public:
  explicit AsyncReaderImpl(ClientImpl* client);
  ~AsyncReaderImpl();

 private:
  friend class AsyncReader;

  ClientImpl* client_;
  std::function<bool(std::unique_ptr<DataRecord>&)> data_cb_;
  std::function<bool(const GapRecord&)> gap_cb_;
  std::unordered_map<logid_t, uint64_t, LogIDHash> log_to_id_;
  DataStore* data_store_;
};

std::shared_ptr<Client> Client::create(
  std::string cluster_name,
  std::string config_url,
  std::string credentials,
  std::chrono::milliseconds timeout,
  std::unique_ptr<ClientSettings> &&settings) noexcept {
  // ignore cluster_name, config_url, and credentials.
  RS_ASSERT(false) << "Must create from DataStore";
  return {};
}

lsn_t Client::appendSync(logid_t logid, const Payload& payload) noexcept {
  lsn_t result;
  rocketspeed::port::Semaphore sem;
  int r = append(logid, payload,
    [&] (Status st, const DataRecord& rec) {
      result = rec.attrs.lsn;
      sem.Post();
    });
  if (r) {
    return LSN_INVALID;
  } else {
    sem.Wait();
    return result;
  }
}

int Client::append(logid_t logid,
                   const Payload& payload,
                   append_callback_t cb) noexcept {
  return impl()->data_store_->append(logid, payload, std::move(cb));
}

std::unique_ptr<Reader> Client::createReader(
  size_t max_logs,
  ssize_t buffer_size) noexcept {
  RS_ASSERT(false);  // not implemented
  return std::unique_ptr<Reader>(nullptr);
}

std::unique_ptr<AsyncReader> Client::createAsyncReader() noexcept {
  return std::unique_ptr<AsyncReader>(new AsyncReaderImpl(impl()));
}

void Client::setTimeout(std::chrono::milliseconds timeout) noexcept {
  impl()->timeout_ = timeout;
}

int Client::trimSync(logid_t logid, lsn_t lsn) noexcept {
  return impl()->data_store_->trimSync(logid, lsn);
}

lsn_t Client::findTimeSync(logid_t logid,
                           std::chrono::milliseconds timestamp,
                           Status *status_out,
                           FindTimeAccuracy) noexcept {
  lsn_t result;
  rocketspeed::port::Semaphore sem;
  int r = findTime(logid, timestamp,
    [&] (Status st, lsn_t lsn) {
      *status_out = st;
      result = lsn;
      sem.Post();
    });
  if (r) {
    return LSN_INVALID;
  } else {
    sem.Wait();
    return result;
  }
}

int Client::findTime(logid_t logid,
                     std::chrono::milliseconds timestamp,
                     find_time_callback_t cb,
                     FindTimeAccuracy) noexcept {
  return impl()->data_store_->findTime(logid, timestamp, std::move(cb));
}

std::pair<logid_t, logid_t> Client::getLogRangeByName(
  const std::string& name) noexcept {
  RS_ASSERT(false);  // not implemented
  return std::make_pair(LOGID_INVALID, LOGID_INVALID);
}

logid_t Client::getLogIdFromRange(const std::string& range_name,
                                  off_t offset) noexcept {
  RS_ASSERT(false);  // not implemented
  return LOGID_INVALID;
}

ClientSettings& Client::settings() {
  return *impl()->settings_.get();
}

ClientImpl *Client::impl() {
  return static_cast<ClientImpl*>(this);
}

AsyncReaderImpl::AsyncReaderImpl(ClientImpl* client)
: client_(client)
, data_store_(client->data_store_) {
  (void)client_;
}

AsyncReaderImpl::~AsyncReaderImpl() {
  auto open = log_to_id_;
  for (auto& entry : open) {
    stopReading(entry.first, nullptr);
  }
}

void AsyncReader::setRecordCallback(
    std::function<bool(std::unique_ptr<DataRecord>&)> cb) {
  impl()->data_cb_ = cb;
}

void AsyncReader::setGapCallback(std::function<bool(const GapRecord&)> cb) {
  impl()->gap_cb_ = cb;
}

void AsyncReader::setDoneCallback(std::function<void(logid_t)>) {
  RS_ASSERT(false);  // not implemented
}

int AsyncReader::startReading(logid_t log_id, lsn_t from, lsn_t until) {
  RS_ASSERT(impl()->data_cb_);  // must set CB before starting to read
  auto self = impl();
  auto it = self->log_to_id_.find(log_id);
  if (it != self->log_to_id_.end()) {
    stopReading(log_id, nullptr);
  }
  static std::atomic<uint64_t> id_gen{0};
  auto id = id_gen++;
  self->log_to_id_.emplace(log_id, id);
  impl()->data_store_->startReading(
    id, self->data_cb_, self->gap_cb_, log_id, from, until);
  return 0;
}

int AsyncReader::stopReading(logid_t log_id, std::function<void()> cb) {
  auto self = impl();
  auto it = self->log_to_id_.find(log_id);
  if (it != self->log_to_id_.end()) {
    auto id = it->second;
    self->log_to_id_.erase(it);
    impl()->data_store_->stopReading(id, log_id, std::move(cb));
    return 0;
  } else {
    return -1;
  }
}

void AsyncReader::withoutPayload() {
  RS_ASSERT(false);  // not implemented
}

void AsyncReader::forceNoSingleCopyDelivery() {
  RS_ASSERT(false);  // not implemented
}

int AsyncReader::isConnectionHealthy(logid_t) const {
  // Not relevant for the mock log. Always return healthy.
  return 1;
}

void AsyncReader::doNotDecodeBufferedWrites() {
  RS_ASSERT(false);  // not implemented
}

void AsyncReader::skipPartiallyTrimmedSections() {
  // Mock implementation doesn't need to do anything here.
  // There are no partially trimmed sections in our logs.
}

AsyncReaderImpl *AsyncReader::impl() {
  return static_cast<AsyncReaderImpl*>(this);
}

const AsyncReaderImpl *AsyncReader::impl() const {
  return static_cast<const AsyncReaderImpl*>(this);
}

class ClientSettingsImpl : public ClientSettings {
 public:
  ClientSettingsImpl() {}
};

ClientSettings* ClientSettings::create() {
  return new ClientSettingsImpl();
}

int ClientSettings::set(const char *name, const char *value) {
  return 0;
}

int ClientSettings::set(const char *name, int64_t value) {
  return 0;
}

ClientSettingsImpl* ClientSettings::impl() {
  return static_cast<ClientSettingsImpl*>(this);
}

namespace IntegrationTestUtils {
// Cluster typedefs DataStore -- implemented as a separate class so that
// Cluster can be forward declared.
class Cluster : public DataStore {
 public:
  using DataStore::DataStore;
};
}

std::shared_ptr<IntegrationTestUtils::Cluster>
CreateLogDeviceTestCluster(size_t /* num_logs */) {
  // DataStore supports writing to any log number, so number of logs is
  // not needed.
  return std::make_shared<IntegrationTestUtils::Cluster>();
}

std::shared_ptr<facebook::logdevice::Client>
CreateLogDeviceTestClient(
    std::shared_ptr<IntegrationTestUtils::Cluster> cluster) {
  return std::make_shared<ClientImpl>(cluster.get());
}

}  // namespace logdevice
}  // namespace facebook
