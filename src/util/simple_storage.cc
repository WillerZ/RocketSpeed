//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/simple_storage.h"
#include <assert.h>
#include <algorithm>
#include <thread>
#include "include/Env.h"

namespace rocketspeed {

Status SimpleLogStorage::Append(LogID id, const Slice& data) {
  Record rec;
  rec.logid = id;
  rec.timestamp = std::chrono::microseconds(Env::Default()->NowMicros());

  // Copy the data into the record's data.
  rec.data.resize(data.size());
  std::copy_n(data.data(), data.size(), rec.data.begin());

  {
    // Critical section to guard the sequence number increment and record append
    std::lock_guard<std::mutex> locker(mutex_);

    // Add the record to the back of the log at ID.
    rec.seqno = seqno_++;
    logs_[id].insert(logs_[id].end(), std::move(rec));
  }
  return Status::OK();
}

Status SimpleLogStorage::Trim(LogID id, std::chrono::microseconds age) {
  std::lock_guard<std::mutex> locker(mutex_);
  auto logIt = logs_.find(id);
  if (logIt == logs_.end()) {
    return Status::NotFound("Log ID doesn't exist");
  }
  // Find the cut-off point to trim at (time now, moved back by the trim age).
  auto threshold = std::chrono::microseconds(Env::Default()->NowMicros()) - age;
  auto& records = logIt->second;

  // Continuously remove from the front of the log set until the first record
  // timestamp is after the cut-off point.
  while (!records.empty() && records.begin()->timestamp < threshold) {
    records.erase(records.begin());
  }
  return Status::OK();
}

Status SimpleLogStorage::CreateReaders(unsigned int parallelism,
                                       std::vector<LogReader*>* readers) {
  if (!readers) {
    return Status::InvalidArgument("readers cannot be null");
  }
  // Construct a number of readers (number = parallelism).
  readers->clear();
  while (parallelism--) {
    readers->push_back(new SimpleLogReader(this));
  }
  return Status::OK();
}

SimpleLogReader::SimpleLogReader(SimpleLogStorage* storage)
: storage_(storage)
, selector_(nullptr) {
}

SimpleLogReader::~SimpleLogReader() {
  if (selector_) {
    assert(selector_->Deregister(this).ok());
  }
}

Status SimpleLogReader::Open(LogID id,
                             SequenceNumber startPoint,
                             SequenceNumber endPoint) {
  cursors_[id] = Cursor(startPoint, endPoint);
  return Status::OK();
}

Status SimpleLogReader::Close(LogID id) {
  auto it = cursors_.find(id);
  if (it == cursors_.end()) {
    return Status::InvalidArgument("log ID not opened on this reader");
  }
  // Ensure that any pending records for this log are removed.
  // This solves a problem with the "Select-Close-Read" scenario:
  //
  // 1. Call Select on a selector registered with a bunch of readers.
  //    This causes the readers to add pending records, which may be from any
  //    open log.
  // 2. Call Close(X) on one of those readers.
  // 3. Call Read on that reader. If that reader had a pending record from log
  //    X then it would be read, but we don't want to read it since we have
  //    closes log X.
  //
  // This code removes any pending logs from the log to be closed.
  auto pendingIt = pending_.begin();
  auto buffersIt = buffers_.begin();
  while (pendingIt != pending_.end()) {
    if (pendingIt->logID == id) {
      pendingIt = pending_.erase(pendingIt);
      buffersIt = buffers_.erase(buffersIt);
    } else {
      ++pendingIt;
      ++buffersIt;
    }
  }
  cursors_.erase(it);
  return Status::OK();
}

Status SimpleLogReader::Read(std::vector<LogRecord>* records,
                             size_t maxRecords) {
  if (!records) {
    return Status::InvalidArgument("records cannot be null");
  }

  // buffers_ may still contain some payloads from a previous Read call, so we
  // should clear those now. However, buffers_ may also contain some pending
  // payloads for this Read (from a call to Empty) so we need to keep those
  // intact. We do this by popping buffers_ until it is the same size as
  // pending_.
  size_t numBuffers = buffers_.size();
  assert(pending_.size() <= numBuffers);  // More records than payloads?
  while (numBuffers > pending_.size()) {
    buffers_.pop_front();
    --numBuffers;
  }

  // Now try to get some more records to satisfy the maxRecords request.
  if (maxRecords > pending_.size()) {
    GetRecords(maxRecords - pending_.size());
  }

  // And copy them out to *records, then remove from pending queue.
  size_t numRecords = std::min(maxRecords, pending_.size());
  records->clear();
  std::move(pending_.begin(),
            pending_.begin() + numRecords,
            std::back_inserter(*records));
  pending_.erase(pending_.begin(), pending_.begin() + numRecords);
  return Status::OK();
}

bool SimpleLogReader::HasData() {
  if (pending_.empty()) {
    GetRecords(1);  // check for more records
  }
  return !pending_.empty();
}

void SimpleLogReader::GetRecords(size_t maxRecords) {
  if (maxRecords == 0) {
    return;
  }
  std::lock_guard<std::mutex> locker(storage_->mutex_);

  // For each open log (with a cursor), we look for any records in that range.
  for (auto& logCursor : cursors_) {
    auto logID = logCursor.first;
    auto seqno = logCursor.second.current;
    auto seqnoEnd = logCursor.second.end;

    // Find the log in the log storage with logID.
    auto logIt = storage_->logs_.find(logID);
    if (logIt != storage_->logs_.end()) {
      auto const& log = logIt->second;

      // Binary search for first record at or after seqno
      SimpleLogStorage::Record searchRecord;
      searchRecord.seqno = seqno;
      auto recordIt = log.lower_bound(searchRecord);

      // While there are records in the log before the end sequence number,
      // keep adding them to the pending records buffer.
      while (recordIt != log.end() && recordIt->seqno < seqnoEnd) {
        buffers_.push_back(recordIt->data);  // copy data so we can slice it
        const auto& buf = buffers_.back();
        LogRecord newRecord;
        newRecord.logID = logID;
        newRecord.payload = Slice(buf.data(), buf.size());
        newRecord.sequenceNumber = recordIt->seqno;
        newRecord.timestamp = recordIt->timestamp;
        pending_.push_back(std::move(newRecord));  // add to pending records
        logCursor.second.current = recordIt->seqno + 1;
        maxRecords--;
        if (maxRecords == 0) {
          // Found enough records, so return
          return;
        }
        ++recordIt;
      }
    }
  }
}

void SimpleLogReader::SetSelector(SimpleLogSelector* selector) {
  selector_ = selector;
}

SimpleLogSelector::SimpleLogSelector() {
}

Status SimpleLogSelector::Create(SimpleLogSelector** selector) {
  if (!selector) {
    return Status::InvalidArgument("selector cannot be null");
  }
  *selector = new SimpleLogSelector();
  return Status::OK();
}

SimpleLogSelector::~SimpleLogSelector() {
  for (auto reader : readers_) {
    static_cast<SimpleLogReader*>(reader)->SetSelector(nullptr);
  }
}

Status SimpleLogSelector::Register(LogReader* reader) {
  if (!reader) {
    return Status::InvalidArgument("reader cannot be null");
  }
  if (std::find(readers_.begin(), readers_.end(), reader) != readers_.end()) {
    return Status::InvalidArgument("reader already registered");
  }
  readers_.push_back(reader);
  static_cast<SimpleLogReader*>(reader)->SetSelector(this);
  return Status::OK();
}

Status SimpleLogSelector::Deregister(LogReader* reader) {
  if (!reader) {
    return Status::InvalidArgument("reader cannot be null");
  }
  auto it = std::remove(readers_.begin(), readers_.end(), reader);
  if (it == readers_.end()) {
    return Status::InvalidArgument("reader not registered");
  }
  readers_.erase(it, readers_.end());
  static_cast<SimpleLogReader*>(reader)->SetSelector(nullptr);
  return Status::OK();
}

Status SimpleLogSelector::Select(std::vector<LogReader*>* selected,
                                 std::chrono::microseconds timeout) {
  if (!selected) {
    return Status::InvalidArgument("selected cannot be null");
  }
  selected->clear();
  auto env = Env::Default();
  auto threshold = std::chrono::microseconds(env->NowMicros()) + timeout;
  do {
    // Try to find a reader with data.
    std::copy_if(readers_.begin(),
                 readers_.end(),
                 std::back_inserter(*selected),
                 [](LogReader* reader) {
                   return static_cast<SimpleLogReader*>(reader)->HasData();
                 });
    if (!selected->empty()) {
      return Status::OK();
    }
    std::this_thread::yield();  // let any readers run
  } while (std::chrono::microseconds(env->NowMicros()) < threshold);
  return Status::NotFound("Timed out");
}

}  // namespace rocketspeed
