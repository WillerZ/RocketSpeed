/* -*- Mode: C++; tab-width: 2; c-basic-offset: 2; indent-tabs-mode: nil -*- */
#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <utility>

#include "logdevice/include/AsyncReader.h"
#include "logdevice/include/ClientSettings.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/LogGroupAttributes.h"
#include "logdevice/include/Reader.h"
#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"

/**
 * @file LogDevice client class. "Client" is a generic name, we expect
 *       application code to use namespaces, possibly aliasing
 *       facebook::logdevice to something shorter, like ld.
 *       See also README.
 */

namespace facebook { namespace logdevice {

class ClientImpl; // private implementation


/**
 * Type of callback that is called when a non-blocking append completes.
 *
 * @param st   E::OK on success. On failure this will be one of the error
 *             codes defined for Client::appendSync().
 *
 * @param r    contains the log id and payload passed to the async append
 *             call. If the operation succeeded (st==E::OK), it will also
 *             contain the LSN and timestamp assigned to the new record.
 *             If the operation failed, the LSN will be set to LSN_INVALID,
 *             timestamp to the time the record was accepted for delivery.
 */
typedef std::function<void(Status st, const DataRecord& r)> append_callback_t;


/**
 * Type of callback that is called when a non-blocking findTime() request
 * completes.
 *
 * See findTime() and findTimeSync() for docs.
 */
typedef std::function<void(Status, lsn_t result)> find_time_callback_t;


class Client {
public:
  /**
   * This is the only way to create new Client instances.
   *
   * @param cluster_name   name of the LogDevice cluster to connect to
   * @param config_url     a URL that identifies at a LogDevice configuration
   *                       resource (such as a file) describing the LogDevice
   *                       cluster this client will talk to. The only supported
   *                       formats are currently
   *                       file:<path-to-configuration-file> and
   *                       configerator:<configerator-path>. Examples:
   *                         "file:logdevice.test.conf"
   *                         "configerator:logdevice/logdevice.test.conf"
   * @param credentials    credentials specification. This may include
   *                       credentials to present to the LogDevice cluster
   *                       along with authentication and encryption specifiers.
   *                       Format TBD. Currently ignored.
   * @param timeout        construction timeout. This value also serves as the
   *                       default timeout for methods on the created object
   * @param settings       client settings instance to take ownership of,
   *                       or nullptr for default settings
   *
   * @return on success, a fully constructed LogDevice client object for the
   *         specified LogDevice cluster. On failure nullptr is returned
   *         and logdevice::err is set to
   *           INVALID_PARAM    invalid config URL or cluster name
   *           TIMEDOUT         timed out while trying to get config
   *           FILE_OPEN        config file could not be opened
   *           FILE_READ        error reading config file
   *           INVALID_CONFIG   various errors in parsing the config
   *           SYSLIMIT         monitoring thread for the config could
   *                            not be started
   */
  static std::shared_ptr<Client>
  create(std::string cluster_name,
         std::string config_url,
         std::string credentials,
         std::chrono::milliseconds timeout,
         std::unique_ptr<ClientSettings> &&settings) noexcept;

  /**
   * create() actually returns pointers to objects of class ClientImpl
   * that inherits from Client. The destructor must be virtual in
   * order to work correctly.
   */
  virtual ~Client() {};


  /**
   * Appends a new record to the log. Blocks until operation completes.
   * The delivery of a signal does not interrupt the wait.
   *
   * @param logid     unique id of the log to which to append a new record
   *
   * @param payload   record payload
   *
   * @return on success the sequence number (LSN) of new record is returned.
   *         On failure LSN_INVALID is returned and logdevice::err is set to
   *         one of:
   *    TIMEDOUT       timeout expired before operation status was known. The
   *                   record may or may not be appended. The timeout
   *                   used is from this Client object.
   *    NOSEQUENCER    The client has been unable to locate a sequencer for
   *                   this log. For example, the server that was previously
   *                   sequencing this log has crashed or is shutting down,
   *                   and a replacement has not yet been brought up, or its
   *                   identity has not yet been communicated to this client.
   *    CONNFAILED     Failed to connect to sequencer. Request was not sent.
   *                   Possible reasons:
   *                    - invalid address in cluster config
   *                    - logdeviced running the sequencer is down or
   *                      unreachable
   *    PEER_CLOSED    Sequencer closed connection after we sent the append
   *                   request but before we got a reply. Record may or
   *                   may not be appended.
   *    TOOBIG         Payload is too big (see Client::getMaxPayloadSize())
   *    NOBUFS         request could not be enqueued because a buffer
   *                   space limit was reached in this Client object. Request
   *                   was not sent
   *    SYSLIMIT       client process has reached a system limit on resources,
   *                   such as file descriptors, ephemeral ports, or memory.
   *                   Request was not sent.
   *    SEQNOBUFS      sequencer is out of buffer space for this log. Record
   *                   was not appended.
   *    SEQSYSLIMIT    sequencer has reached a file descriptor limit,
   *                   the maximum number of ephemeral ports, or some other
   *                   system limit. Record may or may not be appended.
   *    NOSPC          too many nodes on the storage cluster have run out of
   *                   free disk space. Record was not appended.
   *    OVERLOADED     too many nodes on the storage cluster are
   *                   overloaded. Record was not appended.
   *    ACCESS         the service denied access to this client based on
   *                   credentials presented
   *    SHUTDOWN       the logdevice::Client instance was destroyed. Request
   *                   was not sent.
   *    INTERNAL       an internal error has been detected, check logs
   *    INVALID_PARAM  logid is invalid
   */
  lsn_t appendSync(logid_t logid, std::string payload) noexcept;

  /**
   * Appends a new record to the log. Blocks until operation completes.
   * The delivery of a signal does not interrupt the wait.
   *
   * @param payload   record payload, see Record.h. The function does not
   *                  make an internal copy of payload. Other threads of the
   *                  caller must not modify payload data until the call
   *                  returns.
   *
   * See appendSync(logid_t, const Payload&) for a description of return
   * values.
   */
  lsn_t appendSync(logid_t logid, const Payload& payload) noexcept;

  /**
   * Appends a new record to the log without blocking. The function returns
   * control to caller as soon as the append request is put on a delivery
   * queue in this process' address space. The LogDevice client library will
   * call a callback on an unspecified thread when the operation completes.
   *
   * NOTE: records appended to the same log by calling append() method of the
   *       same Client object on the same thread are guaranteed to receive
   *       sequence numbers in the order the append() calls were made. That is,
   *       if both appends succeed, the sequence number assigned to the record
   *       sent earler will be smaller than the sequence number assigned to
   *       the later record.
   *
   *       This is not always true for a pair of append() calls on the same
   *       log made by _different_ threads or through _different_ Client
   *       objects. In those cases internal buffering in various LogDevice
   *       client and server components may result in the record in an earlier
   *       append() call to receive a higher sequence number than the one
   *       submitted by a later append() call made by a different thread or
   *       process, or made through a different logdevice::Client object.
   *
   * @param logid     unique id of the log to which to append a new record
   *
   * @param payload   record payload.
   *
   * @param cb        the callback to call
   *
   * @return  0 is returned if the request was successfully enqueued for
   *          delivery. On failure -1 is returned and logdevice::err is set to
   *             TOOBIG      if payload is too big (see
   *                         Client::getMaxPayloadSize())
   *             NOBUFS      if request could not be enqueued because a buffer
   *                         space limit was reached
   *      INVALID_PARAM      logid is invalid
   */
  int append(logid_t logid,
             std::string payload,
             append_callback_t cb) noexcept;


  /**
   * Appends a new record to the log without blocking. This version doesn't
   * transfer the ownership of the payload to LogDevice and assumes that the
   * caller will be responsible for destroying it.
   *
   *  IMPORTANT: for performance reasons this function does not make
   *  an internal copy of payload.  It just passes payload.data
   *  pointer and payload.size value to the LogDevice client thread
   *  pool. The caller MUST make sure that the payload is not free'd
   *  or modified, or its memory is otherwise reused until the
   *  callback cb() is called with the same payload as its argument. A
   *  common pattern for sending a payload that's on the stack is to
   *  memcpy() it into a malloc'ed buffer, then call free() on
   *  payload.data pointer passed to cb().
   */
  int append(logid_t logid,
             const Payload& payload,
             append_callback_t cb) noexcept;

  /**
   * Creates a Reader object that can be used to read from one or more logs.
   *
   * Approximate memory usage when reading is:
   *   max_logs * client_read_buffer_size * (24*F + C + avg_record_size) bytes
   *
   * The constant F is between 1 and 2 depending on the
   * client_read_flow_control_threshold setting.  The constant C is
   * ClientReadStream overhead, probably a few pointers.
   *
   * When reading many logs, or when memory is important, the client read
   * buffer size can be reduced (before creating the Reader) from the default
   * 4096:
   *
   *   int rv = client->settings().set("client-read-buffer-size", 128);
   *   assert(rv == 0);
   *
   * The client can also set its individual buffer size via the optional
   * buffer_size parameter
   *
   * @param max_logs maximum number of logs that can be read from this Reader
   *                 at the same time
   * @param buffer_size specify the read buffer size for this client, fallback
   *                 to the value in settings if it is -1 or omitted
   */
  std::unique_ptr<Reader> createReader(size_t max_logs,
                                       ssize_t buffer_size = -1) noexcept;


  /**
   * Creates an AsyncReader object that can be used to read from one or more
   * logs via callbacks.
   */
  std::unique_ptr<AsyncReader> createAsyncReader() noexcept;


  /**
   * Overrides the timeout value passed to Client::create() everywhere
   * that timeout is used.
   */
  void setTimeout(std::chrono::milliseconds timeout) noexcept;


  /**
   * Ask LogDevice cluster to trim the log up to and including the specified
   * LSN. After the operation successfully completes records with LSNs up to
   * 'lsn' are no longer accessible to LogDevice clients.
   *
   * This method is synchronous -- it blocks until all storage nodes
   * acknowledge the trim command, or the timeout occurs.
   *
   * @param logid ID of log to trim
   * @param lsn   Trim the log up to this LSN (inclusive), should not be larger
   *              than the LSN of the most recent record available to readers
   * @return      Returns 0 if the request was successfully acknowledged
   *              by all nodes. Otherwise, returns -1 with logdevice::err set to
   *
   *    E::INVALID_PARAM      logid or lsn is invalid
   *    E::FAILED             FAILED to trim on all storage nodes
   *    E::PARTIAL            if some, but not all, nodes successfully
   *                          trimmed the log. In this case, some storage
   *                          nodes might not have trimmed their part of the
   *                          log, so records with LSNs less than or equal to
   *                          'lsn' might still be delivered).
   */
  int trim(logid_t logid, lsn_t lsn) noexcept;


  /**
   * Looks for the sequence number that the log was at at the given time.  The
   * most common use case is to read all records since that time, by
   * subsequently calling startReading(result_lsn).
   *
   * More precisely, this attempts to find the first LSN at or after the given
   * time.  However, if we cannot get a conclusive answer (system issues
   * prevent us from getting answers from part of the cluster), this may
   * return a slightly earlier LSN (with an appropriate status as documented
   * below).  Note that even in that case startReading(result_lsn) will read
   * all records at the given timestamp or later, but it may also read some
   * earlier records.
   *
   * If the given timestamp is earlier than all records in the log, this returns
   * the LSN after the point to which the log was trimmed.
   *
   * If the given timestamp is later than all records in the log, this returns
   * the next sequence number to be issued.  Calling startReading(result_lsn)
   * will read newly written records.
   *
   * If the log is empty, this returns LSN_OLDEST.
   *
   * All of the above assumes that records in the log have increasing
   * timestamps.  If timestamps are not monotonic, the accuracy of this API
   * may be affected.  This may be the case if the sequencer's system clock is
   * changed, or if the sequencer moves and the clocks are not in sync.
   *
   * The delivery of a signal does not interrupt the wait.
   *
   * @param logid       ID of log to query
   * @param timestamp   select the oldest record in this log whose
   *                    timestamp is greater or equal to _timestamp_.
   * @param status_out  if this argument is nullptr, it is ignored. Otherwise,
   *                    *status_out will hold the outcome of the request as
   *                    described below.
   *
   * @return
   * Returns LSN_INVALID on complete failure or an LSN as described above.  If
   * status_out is not null, *status_out can be inspected to determine the
   * accuracy of the result:
   * - E::INVALID_PARAM: logid was invalid
   * - E::OK: Enough of the cluster responded to produce a conclusive answer.
   *   Assuming monotonic timestamps, the returned LSN is exactly the first
   *   record at or after the given time.
   * - E::PARTIAL: Only part of the cluster responded and we only got an
   *   approximate answer.  Assuming monotonic timestamps, the returned LSN is
   *   no later than any record at or after the given time.
   * - E::FAILED: No storage nodes responded, or another critical failure.
   * - E::SHUTDOWN: Client was destroyed while the request was processing.
   */
  lsn_t findTimeSync(logid_t logid,
                     std::chrono::milliseconds timestamp,
                     Status *status_out = nullptr) noexcept;


  /**
   * A non-blocking version of findTimeSync().
   *
   * @return If the request was successfully submitted for processing, returns
   * 0.  In that case, the supplied callback is guaranteed to be called at a
   * later time with the outcome of the request.  See findTimeSync() for
   * documentation for the result.  Otherwise, returns -1.
   */
  int findTime(logid_t logid,
               std::chrono::milliseconds timestamp,
               find_time_callback_t cb) noexcept;


  /**
   * Looks up the boundaries of a log range by its name as specified
   * in this Client's configuration.
   *
   * If configuration has a JSON object in the "logs" section with "name"
   * attribute @param name and without "layout" attribute, returns the lowest
   * and highest log ids in the range. If a JSON object in the "logs"
   * section has "layout" attribute with value "AxB@C", it produces A ranges,
   * each of length C, named "<name>", "<name>#1", "<name>#2", ...,
   * "<name>#<A-1>", where <name> is the value of "name" attribute.
   *
   * @return  If there's a range with name @param name, returns a pair
   *          containing the lowest and  highest log ids in the range
   *          (this may be the same id for log ranges of size 1).
   *          Otherwise returns a pair where both ids are set to LOGID_INVALID.
   */
  std::pair<logid_t, logid_t> getLogRangeByName(const std::string& name)
                                                                     noexcept;

  /**
   * Looks up metadata of a log group by its name as specified in this Client's
   * configuration.
   *
   * @return  If configuration has a JSON object in the "logs" section with
   *          "name" attribute @param name, returns the LogGroupAttributes
   *          object that contains the attributes for that entry.
   */
  std::unique_ptr<LogGroupAttributes>
    getLogGroupAttributes(const std::string& name) noexcept;

  /**
   * @return  on success returns the log id at offset @param offset in log
   *          range identified in the cluster config by @param range_name.
   *          See getLogRangeByName() for description of "layout" attribute.
   *          On failure returns LOGID_INVALID and sets logdevice::err to:
   *
   *            NOTFOUND if no range with @param name is present in the config
   *            INVALID_PARAM  if offset is negative or >= the range size
   */
  logid_t getLogIdFromRange(const std::string& range_name,
                            off_t offset) noexcept;

  /**
   * @return  returns the maximum permitted payload size for this client. The
   *          default is 1MB, but this can be increased via changing the
   *          max-payload-size setting.
   */
  size_t getMaxPayloadSize() noexcept;

  /**
   * Exposes a ClientSettings instance that can be used to change settings
   * for the Client.
   */
  ClientSettings& settings();


private:
  Client() {}             // can be constructed by the factory only
  Client(const Client&) = delete;              // non-copyable
  Client& operator= (const Client&) = delete;  // non-assignable

  friend class ClientImpl;
  ClientImpl *impl(); // downcasts (this)
};

}} // namespace
