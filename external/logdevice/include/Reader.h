/* -*- Mode: C++; tab-width: 2; c-basic-offset: 2; indent-tabs-mode: nil -*- */
#pragma once

#include <chrono>
#include <memory>
#include <vector>

#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file Reader objects are the main interface for reading from logs, in the
 * style of read(2).  Non-blocking, blocking and reads with a timeout are
 * supported.  Multiple logs can be read, resulting in a interleaved stream of
 * records from different logs (but maintaining ordering within any one log).
 *
 * This class is *not* thread-safe - calls should be made from one thread at a
 * time.
 */

class ReaderImpl; // private implementation

class Reader {
 public:
  /**
   * Start reading a log.  Similar to AsyncReader::startReading().
   *
   * Any one log can only be read once by a single Reader.  If this method is
   * called for the same log multiple times, it restarts reading, optionally
   * at a different point.
   *
   * @return On success, returns 0.  On failure, -1 is returned and
   *         logdevice::err is set to:
   *             NOBUFS        if request could not be enqueued because a buffer
   *                           space limit was reached
   *             INVALID_PARAM if from > until or the record callback was not
   *                           specified.
   *             SHUTDOWN      the logdevice::Client instance was destroyedA.
   *             INTERNAL      An internal error has been detected, check logs.
   *             TOOMANY       exceeded limit on number of logs that had been
   *                           specified the Reader was created.
   */
  int startReading(logid_t log_id, lsn_t from, lsn_t until=LSN_MAX);

  /**
   * Stop reading a log.
   *
   * @return On success, returns 0.  On failure, -1 is returned and
   *         logdevice::err is set to:
   *            [any of the error codes from AsyncReader::stopReading()]
   *            NOTFOUND  log is not being read
   */
  int stopReading(logid_t log_id);

  /**
   * Sets the limit on how long read() calls may wait for records to become
   * available.  A timeout of -1 means no limit (infinite timeout).  A timeout
   * of 0 means no waiting (nonblocking reads).
   *
   * Default is no limit.
   *
   * The maximum timeout is 2^31-1 milliseconds (about 24 days).  If a timeout
   * larger than that is passed in, it will be capped.
   *
   * @return 0 on success, -1 if the parameter was invalid
   */
  int setTimeout(std::chrono::milliseconds timeout);

  const std::chrono::milliseconds MAX_TIMEOUT{(1LL<<31) - 1};

  /**
   * Attempts to read a batch of records.
   *
   * The call either delivers 0 or more (up to `nrecords`) data records, or
   * one gap record.
   *
   * The call returns when any of this is true:
   * - `nrecords` records have been delivered
   * - there are no more records to deliver at the moment and the timeout
   *   specified by setTimeout() has been reached
   * - there are no more records to deliver at the moment and
   *   waitOnlyWhenNoData() was called
   * - a gap in sequence numbers is encountered
   * - `until` LSN for some log was reached
   * - not reading any logs, possibly because the ends of all logs have been
   *   reached (returns 0 quickly)
   *
   * Note that even in the case of an infinite timeout, the call may deliver
   * less than `nrecords` data records when a gap is encountered.  The next
   * call to read() will deliver the gap.
   *
   * Waiting will not be interrupted if a signal is delivered to the thread.
   *
   * Example usage:
   *   std::vector<std::unique_ptr<DataRecord> > records;
   *   GapRecord gap;
   *   ssize_t nread = reader->read(100, &records, &gap);
   *   if (nread >= 0) {
   *     for (int i=0; i<nread; ++i) {
   *       // process *records[i]
   *     }
   *   } else {
   *     assert(err == E::GAP);
   *     // process gap
   *   }
   *
   * @param nrecords  limit on number of records to return
   *        data_out  pointer to vector to append data records to
   *        gap_out   pointer to a single GapRecord instance, populated when
   *                  there is a gap in sequence numbers
   *
   * @return Returns the number of records delivered (between 0 and
   *         `nrecords`), or -1 if there was a gap.  If >= 0, that many data
   *         records were appended to the vector.  If -1 then logdevice::err
   *         is set to E::GAP, and *gap_out is filled with information about
   *         the gap.
   */
  ssize_t read(size_t nrecords,
               std::vector<std::unique_ptr<DataRecord> > *data_out,
               GapRecord *gap_out);

  /**
   * If called, whenever read() can return some records but not the number
   * requested by the caller, it will return the records instead of waiting
   * for more.
   *
   * Example:
   * - Caller calls read(100, ...) asking for 100 data records.
   * - Only 20 records are immediately available.
   * - By default, read() would wait until 80 more records arrive or the
   *   timeout expires.  This makes sense for callers that can benefit from
   *   reading and processing batches of data records.
   * - If this method was called before read(), it would return the 20 records
   *   without waiting for more.  This may make sense for cases where latency
   *   is more important.
   */
  void waitOnlyWhenNoData();

  /**
   * If called, data records read by this Reader will not include payloads.
   *
   * This makes reading more efficient when payloads are not needed (they won't
   * be transmitted over the network).
   *
   * Only affects subsequent startReading() calls.
   */
  void withoutPayload();

  /**
   * Checks if the connection to the LogDevice cluster for a log appears
   * healthy.  When a read() call times out, this can be used to make an
   * informed guess whether this is because there is no data or because there
   * a service interruption.
   *
   * NOTE: this is not 100% accurate but will expose common issues like losing
   * network connectivity.
   *
   * @return On success, returns 1 if the connection appears healthy or 0 if
   * there are issues talking to the cluster.  On error returns -1 and sets
   * err to NOTFOUND (not reading given log).
   */
  int isConnectionHealthy(logid_t) const;

  /**
   * Note that, unless all reading is stopped prior to destruction, the
   * destructor may block for some time while all reading gets stopped.  This
   * will typically finish quickly in applications but it does involve
   * interthread communication for each log.
   */
  virtual ~Reader() { }

 private:
  ReaderImpl *impl(); // downcast(this)
  const ReaderImpl *impl() const;
};

}}
