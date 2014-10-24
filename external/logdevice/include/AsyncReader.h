/* -*- Mode: C++; tab-width: 2; c-basic-offset: 2; indent-tabs-mode: nil -*- */
#pragma once

#include <functional>
#include <memory>

#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file AsyncReader objects offer an alternative interface (to the
 * synchronous Reader) for reading logs.  Records are delivered via callbacks
 * called on an unspecified thread.
 *
 * This interface is currently of limited utility.  Because "unspecified
 * thread" above is really a LogDevice internal thread that belongs to a
 * Client, callbacks cannot do too much work or they might block other
 * communication on the Client.  At the same time, callbacks cannot safely
 * transfer a lot of data to application threads because there is no mechanism
 * for pushback when the application threads cannot keep up (#4141220).  For
 * now, this class is appropriate for quick inspection of records which can
 * afford to run on a LogDevice internal thread, or for reading a bounded
 * number of records where there is no risk of a queue growing uncontrollably.
 *
 * This class is *not* thread-safe - calls should be made from one thread at a
 * time.
 */

class AsyncReaderImpl; // private implementation

class AsyncReader {
 public:
  /**
   * Sets a callback that the LogDevice client library will call on an
   * unspecified thread when a record is read. The callback for a log will
   * always be called on the same thread.
   *
   * Only affects subsequent startReading() calls; calling startReading()
   * first and setRecordCallback() after has no effect.
   */
  void setRecordCallback(std::function<void(std::unique_ptr<DataRecord>)>);


  /**
   * Sets a callback that the LogDevice client library will call on an
   * unspecified thread when a gap record is delivered for this log. A gap
   * record informs the reader about gaps in the sequence of record
   * numbers. In most cases such gaps are benign and not an indication of data
   * loss. See class GapRecord in Record.h for details.
   */
  void setGapCallback(std::function<void(const GapRecord&)>);

  /**
   * Start reading records from a log in a specified range of LSNs. This call
   * involves synchronous interthread communication and may block until a
   * Client thread is able to process the request.  The function will return
   * as soon as the request is put on a local queue.  This method is used to
   * both start reading a newly opened log, and to continue reading from a
   * different LSN. Upon successful return, the next record to be delivered to
   * a callback will be as described in @param from below.
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
   *               AsyncReader object.  Once this LSN is reached, the client
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
   * This call involves synchronous interthread communication and may block
   * until a Client thread is able to process the request.  It is guaranteed
   * that callbacks will no longer be called for the log after this call
   * returns.
   *
   * @param log_id log ID to stop reading
   *
   * @return  0 is returned if a stop request was successfully enqueued for
   *          delivery. On failure -1 is returned and logdevice::err is set to
   *             NOBUFS  if request could not be enqueued because a buffer
   *                     space limit was reached
   */
  int stopReading(logid_t log_id);

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

  virtual ~AsyncReader() { }

private:
  friend class AsyncReaderImpl;
  AsyncReaderImpl *impl(); // downcast(this)
  const AsyncReaderImpl *impl() const;
};

}}
