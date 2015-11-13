/* -*- Mode: C++; tab-width: 2; c-basic-offset: 2; indent-tabs-mode: nil -*- */
#pragma once

#include <chrono>
#include <cstdint>
#include "types.h"

namespace facebook { namespace logdevice {


/**
 * Types of gaps in the numbering sequence of a log. See GapRecord below.
 */
enum class GapType {
  UNKNOWN = 0,  // default gap type; used by storage nodes when they don't have
                // enough information to determine gap type
  BRIDGE = 1,   // a "bridge" that completes an epoch
  HOLE = 2,     // a hole in the numbering sequence that appeared due
                // to a sequencer crash. No acknowledged records were lost.
  DATALOSS = 3, // all records in the gap were permanently lost
  TRIM = 4,     // a gap caused by trimming the log
  ACCESS = 5,   // a get sent when the client does not have the required
                // permissions

  MAX
};


/**
 * This const struct represents an opaque payload that a client may wish to
 * store in a log record on a LogDevice.
 */
struct Payload {
  Payload() : data(nullptr), size(0) {}
  Payload(const void *data, size_t size) : data(data), size(size) {}

  const void * data;
  size_t size;

  // returns maximum payload size supported by this implementation of
  // LogDevice client.
  // DEPRECATED! use Client::getMaxPayloadSize() instead. Appends that exceed
  // Client::getMaxPayloadSize() will fail.
  static constexpr size_t maxSize() { return 32*1024*1024; }
};


/**
 * All that is known about a LogRecord is which log it belongs to.
 */
struct LogRecord {
  LogRecord() { }
  explicit LogRecord(logid_t logid) : logid(logid) {}

  logid_t logid;
};


/**
 * This struct contains the basic attributes of data records.
 *
 * We can't add attributes without breaking binary compatibility. That's ok
 * since we do not expect data records to acquire new basic attributes.
 *
 * Should any non-essential attributes come up in the future, we can add
 * support for them with a DataRecord::getExtendedAttributes() or similar.
 */
struct DataRecordAttributes {
  DataRecordAttributes() { }
  DataRecordAttributes(lsn_t lsn,
                       std::chrono::milliseconds timestamp,
                       int batch_offset = 0) :
    lsn(lsn),
    timestamp(timestamp),
    batch_offset(batch_offset) {}

  lsn_t   lsn;   // log sequence number (LSN) of this record

  // timestamp in milliseconds since epoch assigned to this record by a
  // LogDevice server
  std::chrono::milliseconds timestamp;

  // If the record is part of a batch written through BufferedWriter, this
  // contains the record's 0-based index within the batch.  (All records that
  // get batched into a single write by BufferedWriter have the same LSN.)
  int batch_offset;
};


/**
 * DataRecords are log records that contain data payload written to the log
 * by LogDevice clients. In addition to payload every data record has a fixed
 * set of basic attributes.
 */
struct DataRecord : public LogRecord {
  DataRecord() { }
  DataRecord(logid_t logid, const Payload& payload,
             lsn_t lsn=LSN_INVALID,
             std::chrono::milliseconds timestamp=std::chrono::milliseconds{0},
             int batch_offset = 0) :
    LogRecord(logid),
    payload(payload),
    attrs(lsn, timestamp, batch_offset) {}

  Payload payload;             // payload of this record
  DataRecordAttributes attrs;  // attributes of this record. Not const,
                               // can be set after the record instance
                               // is constructed. Log::append() will use this.

  // LogDevice library internals may at runtime slip DataRecord subclasses
  // that free the payload on destruction, so allow a virtual destructor
  virtual ~DataRecord() { }

  // Not movable or copyable because of memory management - a subclass may be
  // managing the payload's lifetime
  DataRecord(const DataRecord &other) = delete;
  DataRecord(DataRecord &&other) = delete;
  DataRecord& operator=(const DataRecord &other) = delete;
  DataRecord& operator=(DataRecord &&other) = delete;
};


/**
 * GapRecords represent gaps in the numbering sequence of a given log.
 */
struct GapRecord : public LogRecord {
  GapRecord() { }
  GapRecord(logid_t logid, GapType type, lsn_t lo, lsn_t hi) :
    LogRecord(logid),
    type(type), lo(lo), hi(hi) {}

  GapType type;  // see definition above
  lsn_t lo;      // lowest LSN in this gap (inclusive)
  lsn_t hi;      // highest LSN in this gap (inclusive)
};


}} // namespace
