/* -*- Mode: C++; tab-width: 2; c-basic-offset: 2; indent-tabs-mode: nil -*- */
#pragma once

#include <functional>
#include <memory>

#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file AsyncReader objects offer an alternative interface (to the
 * synchronous Reader) for reading logs.  Records are delivered via callbacks.
 *
 * Callbacks are invoked on internal LogDevice threads that belong to a
 * Client.  Callbacks should not do too much work or they might block other
 * communication on the Client.  Callbacks for one log will be called on the
 * same thread, however callbacks for different logs typically use multiple
 * threads.  The thread for one log may change if reading is stopped and
 * restarted for the log.
 *
 * This class is *not* thread-safe - calls should be made from one thread at a
 * time.
 */

class AsyncReaderImpl; // private implementation

class AsyncReader {
 public:
  /**
   * Sets a callback that the LogDevice client library will call when a record
   * is read.
   *
   * The callback should return true if the record was successfully consumed.
   * If the callback returns false, delivery of the same record will be
   * retried after some time. Redelivery can also be requested with a
   * resumeReading() call.
   *
   * NOTE: The callback must not drain the input unique_ptr& if it return false
   * (this is asserted in debug builds).
   *
   * Only affects subsequent startReading() calls; calling startReading()
   * first and setRecordCallback() after has no effect.
   */
  void setRecordCallback(std::function<bool(std::unique_ptr<DataRecord>&)>);


  /**
   * Sets a callback that the LogDevice client library will call when a gap
   * record is delivered for this log. A gap record informs the reader about
   * gaps in the sequence of record numbers. In most cases such gaps are
   * benign and not an indication of data loss. See class GapRecord in
   * Record.h for details.
   *
   * The callback should return true if the gap was successfully consumed.
   * If the callback returns false, delivery of the same gap will be
   * retried after some time. Redelivery can also be requested with a
   * resumeReading() call.
   */
  void setGapCallback(std::function<bool(const GapRecord&)>);

  /**
   * Sets a callback that the LogDevice client library will call when it has
   * finished reading the requested range of LSNs.
   */
  void setDoneCallback(std::function<void(logid_t)>);

  /**
   * Start reading records from a log in a specified range of LSNs.  The
   * function will return as soon as the request is put on a local queue.
   * This method is used to both start reading a newly opened log, and to
   * continue reading from a different LSN. Upon successful return, the next
   * record to be delivered to a callback will be as described in @param from
   * below.
   *
   * @param log_id log ID to start reading
   *
   * @param from  log sequence number (LSN) to move the read pointer to. If this
   *              LSN identifies a data record currently in the log, that record
   *              will be the next one delivered to a data callback installed
   *              for the log, or to a Reader object for the log.
   *
   *              If the lowest (oldest) LSN in the log is greater than
   *              this value, the read pointer will move to the oldest record
   *              in the log and that record will be the next one delivered.
   *              See LSN_OLDEST in types.h.
   *
   *              If _from_ falls into a gap in the numbering sequence, the
   *              next record delivered to this reader will be the gap record.
   *
   * @param until  the highest LSN the LogDevice cluster will deliver to this
   *               AsyncReader object.  Once this LSN is reached, the LogDevice
   *               client library will call the done callback. The client
   *               must call startReading() again in order to continue
   *               delivery. If the read pointer comes across a sequence gap
   *               that includes this LSN, the delivery stops after the gap
   *               record is delivered. By default (see LSN_MAX in types.h)
   *               records continue to be delivered in sequence until delivery
   *               is explicitly cancelled by a call to stopReading() below,
   *               or altered by another call to startReading().
   *
   * @return  0 is returned if the request was successfully enqueued for
   *          delivery. On failure -1 is returned and logdevice::err is set to
   *             NOBUFS        if request could not be enqueued because a buffer
   *                           space limit was reached
   *             INVALID_PARAM if from > until or the record callback was not
   *                           specified.
   *             SHUTDOWN      the logdevice::Client instance was destroyed.
   *             INTERNAL      An internal error has been detected, check logs.
   *
   */
  int startReading(logid_t log_id, lsn_t from, lsn_t until=LSN_MAX);


  /**
   * Ask LogDevice cluster to stop delivery of this log's records.  The
   * callbacks registered for the log or the Reader object reading records
   * from this log will stop receiving this log's records until one of
   * startReading() methods is called again.
   *
   * The function returns as soon as the request is put on a local queue.
   * However, record/gap callbacks may continue to be called for the log until
   * a Client thread is able to process the stop request.  It is not safe to
   * destroy the AsyncReader during that time.  After the optional callback is
   * called (on the Client thread), it is guaranted that no further records or
   * gaps will be delivered.
   *
   * @param log_id log ID to stop reading
   * @param callback optional callback to invoke when the request has taken
   *                 effect and no more records will be delivered
   *
   * @return  0 is returned if a stop request was successfully enqueued for
   *          delivery. On failure -1 is returned and logdevice::err is set to
   *             NOBUFS  if request could not be enqueued because a buffer
   *                     space limit was reached
   *             NOTFOUND if reading was not started for specified log
   */
  int stopReading(logid_t log_id, std::function<void()> callback);

  /**
   * Requests delivery for a log to resume after a previous delivery was
   * declined (callback returned false). This can be used to avoid waiting on
   * the redelivery timer when the callback becomes ready to accept new
   * records.
   *
   * NOTE: involves interthread communication which can fail if the queues
   * fill up.  However, no failure handling is generally needed because
   * delivery is retried on a timer.
   *
   * @param log_id log ID to stop reading
   *
   * @return  0 is returned if resume request was successfully enqueued for
   *          delivery. On failure -1 is returned and logdevice::err is set to
   *              NOBUFS   if request could not be enqueued because a buffer
   *                       space limit was reached
   *              NOTFOUND if reading was not started for specified log
   */
  int resumeReading(logid_t log_id);

  /**
   * If called, data records read by this AsyncReader will not include payloads.
   *
   * This makes reading more efficient when payloads are not needed (they won't
   * be transmitted over the network).
   *
   * Only affects subsequent startReading() calls.
   */
  void withoutPayload();

  /**
   * If called, disable the single copy delivery optimization even if the log is
   * configured to support it. Each data record will be sent by all storage
   * nodes that have a copy instead of exactly one.
   * This greatly increases read availability at the cost of higher network
   * bandwith and cpu usage.
   *
   * Only affects subsequent startReading() calls.
   */
  void forceNoSingleCopyDelivery();

  /**
   * If called, when reading a section of the log that has been partially
   * trimmed, the reader will prefer to deliver a large trim gap for the
   * entire section.
   *
   * The default behaviour is to deliver whatever records are still
   * available, which (because of LogDevice's distributed and nondeterministic
   * nature) results in an interleaved stream of records and TRIM gaps, which
   * is undesirable in some cases.
   *
   * See doc/partially-trimmed.md for a detailed explanation.
   */
  void skipPartiallyTrimmedSections();

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
   * Instructs the Reader instance to pass through blobs created by
   * BufferedWriter.
   *
   * By default (if this method is not called), AsyncReader automatically
   * decodes blobs written by BufferedWriter and yields original records as
   * passed to BufferedWriter::append(). If this method is called,
   * BufferedWriteDecoder can be used to decode the blobs.
   */
  void doNotDecodeBufferedWrites();

  virtual ~AsyncReader() { }

private:
  friend class AsyncReaderImpl;
  AsyncReaderImpl *impl(); // downcast(this)
  const AsyncReaderImpl *impl() const;
};

}}
