//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/controltower/tailer.h"
#include "src/logdevice/storage.h"
#include <vector>

namespace rocketspeed {

// Version of MessageData that holds a std::unique_ptr<LogRecord> to
// persist the LogDevice buffer.
struct LogRecordMessageData : public MessageData {
 public:
  explicit LogRecordMessageData(std::unique_ptr<LogRecord> record,
                                Status* status)
  : MessageData(MessageType::mDeliver)
  , record_(std::move(record)) {
    // Deserialize
    Slice payload = record_->payload;
    *status = DeSerializeStorage(&payload);
    SetSequenceNumber(record_->seqno);
  }

 private:
  std::unique_ptr<LogRecord> record_;
};

Tailer::Tailer(const std::vector<unique_ptr<ControlRoom>>& rooms,
               std::shared_ptr<LogStorage> storage,
               std::shared_ptr<Logger> info_log) :
  rooms_(rooms),
  storage_(storage),
  info_log_(info_log) {
}

Tailer::~Tailer() {
}

Status Tailer::Initialize() {
  if (reader_.size() != 0) {
    return Status::OK();  // already initialized, nothing more to do
  }
  // define a lambda for callback
  auto record_cb = [this] (std::unique_ptr<LogRecord> record) {
    LogID log_id = record->log_id;
    int room_number = log_id % rooms_.size();
    ControlRoom* room = rooms_[room_number].get();

    // Convert storage record into RocketSpeed message.
    Status st;
    MessageData* msg = new LogRecordMessageData(std::move(record), &st);
    if (!st.ok()) {
      LOG_WARN(info_log_,
        "Failed to deserialize message (%s).",
        st.ToString().c_str());
        info_log_->Flush();
    } else {
      LOG_INFO(info_log_,
        "Tailer received data (%.16s)@%lu for Topic(%s) in Log(%lu).",
        msg->GetPayload().ToString().c_str(),
        msg->GetSequenceNumber(),
        msg->GetTopicName().ToString().c_str(),
        log_id);
    }
    assert(st.ok());

    // forward to appropriate room
    std::unique_ptr<Message> m(msg);
    st = room->Forward(std::move(m), log_id, -1);
    assert(st.ok());
  };

  auto gap_cb = [this] (const GapRecord& record) {
    // Log the gap.
    switch (record.type) {
      case GapType::kDataLoss:
        LOG_WARN(info_log_,
            "Data Loss in Log(%lu) from %lu-%lu.",
            record.log_id, record.from, record.to);
        break;

      case GapType::kRetention:
        LOG_INFO(info_log_,
            "Retention gap in Log(%lu) from %lu-%lu.",
            record.log_id, record.from, record.to);
        break;

      case GapType::kBenign:
        LOG_INFO(info_log_,
            "Benign gap in Log(%lu) from %lu-%lu.",
            record.log_id, record.from, record.to);
        break;
    }

    // Create message to send to control room.
    // We don't know the tenant ID at this point, or the host ID. These will
    // have to be set in the control room.
    std::unique_ptr<Message> msg(new MessageGap(Tenant::InvalidTenant,
                                                ClientID(),
                                                record.type,
                                                record.from,
                                                record.to));

    int room_number = record.log_id % rooms_.size();
    ControlRoom* room = rooms_[room_number].get();
    room->Forward(std::move(msg), record.log_id, -1);
  };

  // create logdevice reader. There is one reader per ControlRoom.
  std::vector<AsyncLogReader*> handle;
  Status st = storage_->CreateAsyncReaders(rooms_.size(),
                                           record_cb,
                                           gap_cb,
                                           &handle);
  if (!st.ok()) {
    return st;
  }
  assert(handle.size() == rooms_.size());

  // store all the Readers
  for (unsigned int i = 0; i < handle.size(); i++) {
    reader_.emplace_back(handle[i]);
  }

  // initialize the number of open logs per reader to zero.
  num_open_logs_per_reader_.resize(rooms_.size(), 0);
  return Status::OK();
}

// Create a new instance of the LogStorage
Status
Tailer::CreateNewInstance(Env* env,
                          const std::vector<unique_ptr<ControlRoom>>& rooms,
                          std::shared_ptr<LogStorage> storage,
                          const URL& storage_url,
                          std::shared_ptr<Logger> info_log,
                          int num_workers,
                          Tailer** tailer) {
  if (storage == nullptr) {
    // create logdevice client
    std::unique_ptr<facebook::logdevice::ClientSettings> settings(
      facebook::logdevice::ClientSettings::create());
    settings->set("num-workers", num_workers);
    std::shared_ptr<facebook::logdevice::Client> client =
      facebook::logdevice::Client::create(
                           "rocketspeed.logdevice.primary",  // storage name
                           storage_url,
                           "",                               // credentials
                           std::chrono::milliseconds(1000),
                           std::move(settings));

    // created logdevice reader
    LogDeviceStorage* store;
    Status st = LogDeviceStorage::Create(client, env, &store);
    if (!st.ok()) {
      return st;
    }
    storage.reset(store);
  }
  *tailer = new Tailer(rooms, storage, info_log);
  return Status::OK();
}

Status Tailer::StartReading(LogID logid,
                            SequenceNumber start,
                            unsigned int room_id,
                            bool first_open) const {
  if (reader_.size() == 0) {
    return Status::NotInitialized();
  }
  AsyncLogReader* r = reader_[room_id].get();
  Status st = r->Open(logid, start);
  if (st.ok()) {
    LOG_INFO(info_log_,
             "AsyncReader %u start reading Log(%lu)@%lu.",
             room_id,
             logid,
             start);
    if (first_open) {
      num_open_logs_per_reader_[room_id]++;
    }
  } else {
    LOG_WARN(info_log_,
             "AsyncReader %u failed to start reading Log(%lu)@%lu (%s).",
             room_id,
             logid,
             start,
             st.ToString().c_str());
  }
  return st;
}

// Stop reading from this log
Status
Tailer::StopReading(LogID logid, unsigned int room_id) const {
  if (reader_.size() == 0) {
    return Status::NotInitialized();
  }
  AsyncLogReader* r = reader_[room_id].get();
  Status st = r->Close(logid);
  if (st.ok()) {
    LOG_INFO(info_log_,
        "AsyncReader %u stopped reading Log(%lu).",
        room_id, logid);
    num_open_logs_per_reader_[room_id]--;
  } else {
    LOG_WARN(info_log_,
        "AsyncReader %u failed to stop reading Log(%lu) (%s).",
        room_id, logid, st.ToString().c_str());
  }
  return st;
}

// find latest seqno then invoke callback
Status
Tailer::FindLatestSeqno(LogID logid,
                        std::function<void(Status, SequenceNumber)> callback)
    const {
  // LogDevice treats std::chrono::milliseconds::max() specially, avoiding
  // a binary search and just returning the next LSN.
  return storage_->FindTimeAsync(logid,
                                 std::chrono::milliseconds::max(),
                                 callback);
}

// This traverses the reader array without any locks but that is
// ok because it is used only by unit tests when there is no
// tailer activity.
int
Tailer::NumberOpenLogs() const {
  int count = 0;
  for (unsigned int i = 0; i < rooms_.size(); i++) {
    count += num_open_logs_per_reader_[i];
  }
  return count;
}

}  // namespace rocketspeed
