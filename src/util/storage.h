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

namespace rocketspeed {

typedef uint64_t LogID;

enum SequencePoint : SequenceNumber {
  kBeginningOfTimeSeqno = 0,
  kEndOfTimeSeqno = std::numeric_limits<SequenceNumber>::max()
};

/**
 * Raw log record entry.
 */
struct LogRecord {
  LogID logID;                          // log that this record came from
  Slice payload;                        // raw record data
  SequenceNumber sequenceNumber = 0;    // sequence number of record
  std::chrono::microseconds timestamp;  // record timestamp
  LogRecord(LogID l, Slice p, SequenceNumber s, std::chrono::microseconds t) :
    logID(l), payload(p), sequenceNumber(s), timestamp(t) {
  }
  LogRecord() {}
};

/**
 * Callback for asynchronous append requests.
 */
typedef std::function<void(Status)> AppendCallback;

class LogReader;
class AsyncLogReader;
class LogSelector;

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
   * Creates a group of LogReaders that will execute in parallel.
   *
   * @param parallelism number of parallel readers to create.
   * @param readers output buffer for the LogReaders.
   * @return on success returns OK(), otherwise errorcode.
   */
  virtual Status CreateReaders(unsigned int parallelism,
                               std::vector<LogReader*>* readers) = 0;

  /**
   * Creates a group of AsyncLogReaders that will execute in parallel.
   *
   * @param parallelism number of parallel readers to create.
   * @param callback a callback that will be called on an
   *        unspecified thread when a record is read.
   * @param readers output buffer for the AsyncLogReaders.
   * @return on success returns OK(), otherwise errorcode.
   */
  virtual Status CreateAsyncReaders(unsigned int parallelism,
                                std::function<void(const LogRecord&)> callback,
                                std::vector<AsyncLogReader*>* readers) = 0;
};

/**
 * Interface for reading from one or more logs.
 */
class LogReader {
 public:
  /**
   * Closes the log reader and deregisters itself from any selectors that it
   * is registered with.
   */
  virtual ~LogReader() {}

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
   * Stops reading from a log. Subsequent calls to Read will not return records
   * from this log ID.
   *
   * @param id ID number of the log to stop reading from.
   * @return on success returns OK(), otherwise errorcode.
   */
  virtual Status Close(LogID id) = 0;

  /**
   * Reads records from all logs opened by this reader.
   *
   * Will read records while they are available, up to maxRecords. Once no more
   * records are available in the log Read will return immediately instead of
   * waiting for more records.
   *
   * If there is a gap in the records before any records due to dataloss then
   * Read will return an errorcode, and will resume record reading on the next
   * call. Read will never silently lose data.
   *
   * NOTE: The slices written to the LogRecords are only guaranteed to be valid
   * until the next call to Read on this LogReader as buffers may be re-used for
   * efficiency.
   *
   * @param records output for records read.
   * @param maxRecords maximum number of records to read.
   * @return on success returns OK(), otherwise errorcode.
   */
  virtual Status Read(std::vector<LogRecord>* records,
                      size_t maxRecords) = 0;
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

/**
 * Provides an interface similar to the Posix select call. Multiple LogReaders
 * are registered to the selector, and Select will choose from these readers as
 * data becomes available. LogSelector essentially aggregates from multiple
 * LogReaders.
 */
class LogSelector {
 public:
  /**
   * Closes the selector and deregisters all registered LogReaders.
   */
  virtual ~LogSelector() {}

  /**
   * Registers a LogReader with this selector. Subsequent calls to Select may
   * select the newly registered LogReader.
   *
   * @param reader The LogReader to register.
   * @return on success returns OK(), otherwise errorcode.
   */
  virtual Status Register(LogReader* reader) = 0;

  /**
   * Deregisters a LogReader with this selector.
   *
   * @param reader The LogReader to deregister.
   * @return on success returns OK(), otherwise errorcode.
   */
  virtual Status Deregister(LogReader* reader) = 0;

  /**
   * Blocks until one or more of the registered LogReaders has data, then writes
   * those LogReaders to the selected output buffer. A LogReader will appear at
   * most once in the output.
   *
   * If no data is available after the timeout period, Select will return
   * NotFound().
   *
   * The returned LogReaders are guaranteed to have at least one record
   * available. However, if Close(logID) is called on one of the returned
   * readers before calling Read then this may cause records to not be available
   * on that reader.
   *
   * @param selected output parameter for the selected LogReader.
   * @param timeout maximum time to wait for data.
   * @return on success returns OK(), otherwise errorcode.
   */
  virtual Status Select(std::vector<LogReader*>* selected,
                        std::chrono::microseconds timeout) = 0;
};

}  // namespace rocketspeed
