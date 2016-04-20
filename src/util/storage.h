// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <chrono>
#include <memory>
#include <functional>
#include <limits>
#include <vector>
#include "include/Slice.h"
#include "include/Status.h"
#include "include/Types.h"

namespace rocketspeed {

typedef uint64_t LogID;
class Statistics;

enum SequencePoint : SequenceNumber {
  kBeginningOfTimeSeqno = 0,
  kEndOfTimeSeqno = std::numeric_limits<SequenceNumber>::max()
};

struct TopicUUID;

/**
 * Abstract raw log record entry.
 */
struct LogRecord {
 public:
  LogRecord(LogID log_Id,
            Slice pay_load,
            SequenceNumber sequenceno,
            std::chrono::microseconds time,
            std::unique_ptr<void, void(*)(void*)> _context) :
    log_id(log_Id),
    payload(pay_load),
    seqno(sequenceno),
    timestamp(time),
    context(std::move(_context)) {
  }

  LogRecord() :
    log_id(0),
    seqno(0),
    timestamp(0),
    context(nullptr, &DefaultDeleter) {}

  LogID log_id;                         // log that this record came from
  Slice payload;                        // raw record data
  SequenceNumber seqno = 0;             // sequence number of record
  std::chrono::microseconds timestamp;  // record timestamp

  // Implementation can provide a pointer to some context, usually to track
  // ownership of the payload slice.
  std::unique_ptr<void, void(*)(void*)> context;

 private:
  // Do nothing deleter for default empty context.
  static void DefaultDeleter(void*) {}
};

/**
 * Type of gaps that may appear in the logs.
 */
enum GapType : uint8_t {
  kBenign = 0x00,     // Gap due to operational issue, no data lost.
  kDataLoss = 0x01,   // Catastrophic failure, acknowledged data was lost.
  kRetention = 0x02,  // Gap due to data falling out of retention period.
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
   * Exports statistics from the storage.
   */
  virtual Statistics GetStatistics();

  /**
   * Appends data to a topic asynchronously. The call will return immediately,
   * with the return value indicating if the asynchronous request was made.
   * If AppendAsync returns success then at some point in the future the
   * callback will be called with a Status indicating if the append was
   * successfully written to the storage.
   *
   * Important: the data slice must remain valid and unmodified until the
   * callback is called.
   *
   * @param topic_name the name of the topic to append to.
   * @param data the data to write.
   * @param callback Callback to process the append request.
   * @return on success returns OK(), otherwise errorcode.
   */
  virtual Status AppendAsync(LogID id,
                             const Slice& data,
                             AppendCallback callback) = 0;

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
   * Trim log up to a sequence number.
   */
  virtual Status Trim(LogID id, SequenceNumber seqno) {
    return Status::NotSupported("Trimming not supported");
  }

  /**
   * Creates a group of AsyncLogReaders that will execute in parallel.
   *
   * @param parallelism number of parallel readers to create.
   * @param record_cb a callback that will be called on an
   *        unspecified thread when a record is read. Should return true if
   *        processed successfully, or false if the reader should retry the
   *        same record later. If the callback returned false and the record
   *        was moved, the callback will be invoked again with empty record.
   * @param gap_cb a callback that will be called on an
   *        unspecified thread when a gap occurs in the log.
   * @param readers output buffer for the AsyncLogReaders.
   * @return on success returns OK(), otherwise errorcode.
   */
  virtual Status CreateAsyncReaders(
    unsigned int parallelism,
    std::function<bool(LogRecord&)> record_cb,
    std::function<bool(const GapRecord&)> gap_cb,
    std::vector<AsyncLogReader*>* readers) = 0;

  /**
   * Returns true if subscribers can subscribe past the end of a log.
   */
  virtual bool CanSubscribePastEnd() const = 0;
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
 * Abstract Log Router.
 */
class LogRouter {
 public:
  /**
   * Routes a topic to a log ID. Default implementation uses RouteToLog.
   *
   * @param namespace_id the namespace of the topic to route.
   * @param topic_name the topic to route.
   * @param out output for logid.
   * @return on success returns OK(), otherwise errorcode.
   */
  virtual Status GetLogID(Slice namespace_id,
                          Slice topic_name,
                          LogID* out) const;

  /**
   * Routes a topic to a log ID. Default implementation uses RouteToLog.
   *
   * @param topic the UUID of the topic to route.
   * @param out output for logid.
   * @return on success returns OK(), otherwise errorcode.
   */
  virtual Status GetLogID(const TopicUUID& topic,
                          LogID* out) const;

  virtual ~LogRouter() {}

 private:
  // Internal: Maps a routing hash to a log.
  virtual Status RouteToLog(size_t routing_hash, LogID* out) const = 0;
};

}  // namespace rocketspeed
