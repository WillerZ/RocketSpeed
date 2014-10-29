// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <chrono>
#include <memory>
#include <limits>
#include <vector>
#include "include/Slice.h"
#include "include/Status.h"
#include "include/Types.h"
#include "src/messages/messages.h"

namespace rocketspeed {

typedef uint64_t LogID;

enum SequencePoint : SequenceNumber {
  kBeginningOfTimeSeqno = 0,
  kEndOfTimeSeqno = std::numeric_limits<SequenceNumber>::max()
};

/**
 * Abstract raw log record entry.
 */
struct LogRecord {
 public:
  LogID log_id;                         // log that this record came from
  Slice payload;                        // raw record data
  SequenceNumber seqno = 0;             // sequence number of record
  std::chrono::microseconds timestamp;  // record timestamp

  virtual ~LogRecord() {}

 protected:
  LogRecord(LogID log_Id,
            Slice pay_load,
            SequenceNumber sequenceno,
            std::chrono::microseconds time) :
    log_id(log_Id),
    payload(pay_load),
    seqno(sequenceno),
    timestamp(time) {
  }

  LogRecord() {}
};

/**
 * Gap record. This indicates that a range of sequence numbers is missing
 * from the log for a variety of reasons.
 */
struct GapRecord {
  GapType type;
  LogID log_id;
  SequenceNumber from;
  SequenceNumber to;
};

/**
 * Callback for asynchronous append requests.
 */
typedef std::function<void(Status, SequenceNumber)> AppendCallback;

class AsyncLogReader;

/**
 * Abstract interface for the log storage.
 */
class LogStorage {
 public:
  /**
   * Sub-classes will provide their own construction methods, e.g.
   * LogDeviceStorage::Open, ZippyDBStorage::Open, with bespoke configuration
   * parameters.
   */
  LogStorage() {}

  /**
   * Closes the connection to the log storage.
   */
  virtual ~LogStorage() {}

  /**
   * Appends data to a log. This call will block until the append is processed.
   *
   * @param id ID number of the log to write to.
   * @param data the data to write.
   * @return on success returns OK(), otherwise errorcode.
   */
  virtual Status Append(LogID id,
                        const Slice& data) = 0;

  /**
   * Appends data to a log asynchronously. The call will return immediately,
   * with the return value indicating if the asynchronous request was made.
   * If AppendAsync returns success then at some point in the future the
   * callback will be called with a Status indicating if the append was
   * successfully written to the storage.
   *
   * Important: the data slice must remain valid and unmodified until the
   * callback is called.
   *
   * @param id ID number of the log to write to.
   * @param data the data to write.
   * @param callback Callback to process the append request.
   * @return on success returns OK(), otherwise errorcode.
   */
  virtual Status AppendAsync(LogID id,
                             const Slice& data,
                             AppendCallback callback) = 0;

  /**
   * Trims all messages from the log that are older than age.
   *
   * @param id ID number of the log to trim.
   * @param age the age of logs to trim.
   * @return on success returns OK(), otherwise errorcode.
   */
  virtual Status Trim(LogID id,
                      std::chrono::microseconds age) = 0;

  /**
   * Finds the sequence number for a point in time for a particular log
   * then invokes the callback with the sequence number.
   *
   * @param id ID number of the log to search.
   * @param timestamp Timestamp of the record to search for.
   *                  If std::chrono::milliseconds::max() is given, it will
   *                  return the next sequence number to be issued.
   * @param callback The callback to be called when the sequence number is
   *                 found. The callback will be called if and only if the
   *                 returned status is OK. If the callback status is not OK
   *                 then the sequence number argument is undefined.
   * @return on success returns OK(), otherwise errorcode.
   */
  virtual Status FindTimeAsync(LogID id,
                      std::chrono::milliseconds timestamp,
                      std::function<void(Status, SequenceNumber)> callback) = 0;

  /**
   * Creates a group of AsyncLogReaders that will execute in parallel.
   *
   * @param parallelism number of parallel readers to create.
   * @param record_cb a callback that will be called on an
   *        unspecified thread when a record is read.
   * @param gap_cb a callback that will be called on an
   *        unspecified thread when a gap occurs in the log.
   * @param readers output buffer for the AsyncLogReaders.
   * @return on success returns OK(), otherwise errorcode.
   */
  virtual Status CreateAsyncReaders(unsigned int parallelism,
                      std::function<void(std::unique_ptr<LogRecord>)> record_cb,
                      std::function<void(const GapRecord&)> gap_cb,
                      std::vector<AsyncLogReader*>* readers) = 0;
};

/**
 * Async Interface for reading from one or more logs.
 */
class AsyncLogReader {
 public:
  /**
   * Closes the log reader
   */
  virtual ~AsyncLogReader() {}
  /**
   * Opens a new log for reading.
   *
   * To read from the beginning of a log, supply kBeginningOfTimeSeqno as the
   * startPoint. To read to the end of a log, supply kEndOfTimeSeqno as the
   * endPoint.
   *
   * @param id ID number of the log to start reading.
   * @param startPoint sequence number of record to start reading from.
   * @param endPoint sequence number of record to stop reading at.
   * @return on success returns OK(), otherwise errorcode.
   */
  virtual Status Open(LogID id,
                      SequenceNumber startPoint = kBeginningOfTimeSeqno,
                      SequenceNumber endPoint = kEndOfTimeSeqno) = 0;

  /**
   * Stops reading from a log.
   *
   * @param id ID number of the log to stop reading from.
   * @return on success returns OK(), otherwise errorcode.
   */
  virtual Status Close(LogID id) = 0;
};

}  // namespace rocketspeed
