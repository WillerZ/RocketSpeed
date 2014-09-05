//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <list>
#include <map>
#include <mutex>
#include <set>
#include <vector>
#include "src/util/storage.h"

namespace rocketspeed {

class SimpleLogReader;
class SimpleLogSelector;

/**
 * Simple, local, in-memory, non-persistent implementation of the LogStorage
 * interface, useful for testing systems that depend on log storage.
 * SimpleLogStorage is not designed for efficiency, but should be suitably
 * efficient for testing purposes.
 *
 * All SimpleLogStorage operations are thread-safe.
 *
 * !!! NOT FOR USE IN PRODUCTION CODE !!!
 */
class SimpleLogStorage : public LogStorage {
 public:
  SimpleLogStorage() {}

  virtual ~SimpleLogStorage() {}

  virtual Status Append(LogID id,
                        const Slice& data);

  virtual Status Trim(LogID id,
                      std::chrono::microseconds age);

  virtual Status CreateReaders(unsigned int maxLogsPerReader,
                               unsigned int parallelism,
                               std::vector<LogReader*>* readers);
  Status CreateAsyncReaders(unsigned int maxLogsPerReader,
                            unsigned int parallelism,
                            std::function<void(const LogRecord&)> callback,
                            std::vector<AsyncLogReader*>* readers) {
    return Status::NotSupported("Not for production usage.");
  }
 private:
  friend class SimpleLogReader;

  // The SimpleLogStorage is responsible for storing all logs.
  // The implementation of a single Log is just an std::set of Records, with
  // the Records being ordered by sequence number. The reason for using a set
  // instead of a list or vector is so that we can binary search for starting
  // points, and also trim from the front in O(1).
  struct Record {
    LogID logid;
    std::vector<char> data;
    SequenceNumber seqno;
    std::chrono::microseconds timestamp;

    // Order by sequence number to allow binary searching.
    bool operator<(const Record& rhs) const {
      return seqno < rhs.seqno;
    }
  };

  // A Log is like an individual Log in LogDevice. It is implemented as a
  // set of Records, ordered by sequence number.
  typedef std::set<Record> Log;

  // This map of LogIDs to Logs allows us to lookup logs by their ID quickly.
  typedef std::map<LogID, Log> Logs;

  Logs logs_;                 // all the logs in the system.
  SequenceNumber seqno_ = 0;  // current sequence number, increments on Append.
  std::mutex mutex_;          // mutex for locking parallel appends/reads.
};

/**
 * Reads from one or more logs of a SimpleLogStorage.
 *
 * Each SimpleLogReader must be used from a single thread.
 */
class SimpleLogReader : public LogReader {
 public:
  SimpleLogReader(SimpleLogStorage* storage,
                  unsigned int maxLogs);

  virtual ~SimpleLogReader();

  virtual Status Open(LogID id,
                      SequenceNumber startPoint,
                      SequenceNumber endPoint);

  virtual Status Close(LogID id);

  virtual Status Read(std::vector<LogRecord>* records,
                      size_t maxRecords);

  bool HasData();

 private:
  friend class SimpleLogSelector;

  // Cursor is just a pair of SequenceNumbers defining the start and end point
  // of where to read in a specific log. As records are read, 'current' will
  // increase.
  struct Cursor {
    Cursor() {}

    Cursor(SequenceNumber _current, SequenceNumber _end)
    : current(_current)
    , end(_end) {
    }

    SequenceNumber current;
    SequenceNumber end;
  };

  // Called by SimpleLogSelector on Register/Deregister
  void SetSelector(SimpleLogSelector* selector);

  // Populates the pending_ records array, adding up to maxRecords.
  void GetRecords(size_t maxRecords);

  SimpleLogStorage* storage_;    // Storage to read from
  unsigned int maxLogs_;         // Maximum open logs
  SimpleLogSelector* selector_;  // Selector currently registered to

  // Sequence number ranges for each open log.
  std::map<LogID, Cursor> cursors_;

  // Copies of message payload buffers. The LogRecords returned by Read will
  // have payload slices that point to these buffers, so that the client doesn't
  // need to create their own copies.
  std::list<std::vector<char>> buffers_;

  // LogRecords received, but not currently returned in a Read call.
  std::vector<LogRecord> pending_;
};

/**
 * Provides a selector interface to multiple SimpleLogReaders.
 *
 * SimpleLogSelector must be used from a single thread.
 */
class SimpleLogSelector : public LogSelector {
 public:
  SimpleLogSelector();

  static Status Create(SimpleLogSelector** selector);

  virtual ~SimpleLogSelector();

  virtual Status Register(LogReader* reader);

  virtual Status Deregister(LogReader* reader);

  virtual Status Select(std::vector<LogReader*>* selected,
                        std::chrono::microseconds timeout);

 private:
  std::vector<LogReader*> readers_;  // The readers registered to this selector.
};

}  // namespace rocketspeed
