/* -*- Mode: C++; tab-width: 2; c-basic-offset: 2; indent-tabs-mode: nil -*- */
/* can be included multiple times */

#ifndef ERROR_CODE
#error ERROR_CODE() macro not defined
#define ERROR_CODE(...)
#endif


ERROR_CODE(OK,       = 0,"success")
ERROR_CODE(NOTFOUND,    ,"requested identifier does not exist")
ERROR_CODE(TIMEDOUT,    ,"request or connection timed out")
ERROR_CODE(NOSEQUENCER, ,"no sequencer was found for log")
ERROR_CODE(CONNFAILED,  ,"connection failed")
ERROR_CODE(NOTCONN,     ,"not connected to destination")
ERROR_CODE(TOOBIG,      ,"argument or return value too big")
ERROR_CODE(TOOMANY,     ,"operation exceeded limit")
ERROR_CODE(PREEMPTED,   ,"log writer was preempted by another one")
ERROR_CODE(NOBUFS,      ,"buffer space limit was reached")
ERROR_CODE(NOMEM,       ,"failure of a global memory allocator") //e.g. malloc()
ERROR_CODE(INTERNAL,    ,"internal error. Likely a bug in LogDevice code.")
ERROR_CODE(SYSLIMIT,    ,"an operation hit a limit set by the OS or runtime")
                         // such as the maximum number of threads. Changing the
                         // configuration of your execution environment may
                         // fix this.
ERROR_CODE(TEMPLIMIT,   ,"an operation hit a limit but may succeed if retried")
ERROR_CODE(PERMLIMIT,   ,"an operation hit a limit, cannot succeed if retried")
ERROR_CODE(ACCESS,      ,"permission denied") // from errno(3)
ERROR_CODE(ALREADY,     ,"connection is already in progress") // from errno(3)
ERROR_CODE(ISCONN,      ,"socket already connected") // from errno(3)
ERROR_CODE(UNREACHABLE, ,"outgoing connections to peer of this socket are "
                         "not supported")
ERROR_CODE(UNROUTABLE,  ,"attempt to connect to an address to which there is "
                         "no route")  // this is most likely a config error
ERROR_CODE(BADMSG,      ,"invalid message format") // from errno(3)
ERROR_CODE(DISABLED,    ,"server is marked down") // connection failed because
                        // the server is currently marked down (disabled) after
                        // a series of unsuccessful connection attempts
ERROR_CODE(EXISTS,      ,"object already exists")
ERROR_CODE(SHUTDOWN,    ,"shutting down")
ERROR_CODE(NOTINCONFIG, ,"server is not in cluster configuration")
ERROR_CODE(PROTONOSUPPORT, ,"unsupport LogDevice protocol version") // errno(3)
ERROR_CODE(PROTO,       ,"protocol error") // errno(3)
ERROR_CODE(PEER_CLOSED, ,"connection closed by peer")
ERROR_CODE(FAILED,      ,"service failed to receive or execute request")
ERROR_CODE(WOULDBLOCK,  ,"operation would block")
ERROR_CODE(ABORTED,     ,"operation aborted")
ERROR_CODE(INPROGRESS,  ,"operation is already in progress") // errno(3)
ERROR_CODE(CANCELLED,   ,"message was cancelled") // errno(3)
ERROR_CODE(NOTSTORAGE,  ,"not a storage node") // request that can only be
 // completed by a storage node was sent to a node not configured to run a
 // local log store, such as a pure sequencer node.
ERROR_CODE(AGAIN,       ,"temporary error, try again later")
ERROR_CODE(PARTIAL,     ,"operation partially succeeded")
ERROR_CODE(GAP,         ,"gap in sequence numbers detected")
ERROR_CODE(TRUNCATED,   ,"result truncated")
ERROR_CODE(STALE,       ,"object or identifier is stale")

ERROR_CODE(INVALID_PARAM, =100,"invalid parameter")
ERROR_CODE(INVALID_CLUSTER,   ,"invalid LogDevice cluster name")
ERROR_CODE(INVALID_CONFIG,    ,"various errors in config contents")
ERROR_CODE(INVALID_THREAD,    ,"function cannot execute on this thread") // for
                               // example, it's not running an event loop)
ERROR_CODE(INVALID_IP,        ,"invalid server IP address in cluster config")
ERROR_CODE(INVALID_OPERATION, ,"invalid operation for mode")
ERROR_CODE(UNKNOWN_SETTING,   ,"unrecognized setting name")
ERROR_CODE(INVALID_SETTING_VALUE, ,"invalid value of a setting")
ERROR_CODE(UPTODATE,          ,"value was already up to date")

// Configuration errors

ERROR_CODE(FILE_OPEN, =200,"error opening config file")
ERROR_CODE(FILE_READ,     ,"error reading config file contents")

// Local log store errors

ERROR_CODE(LOCAL_LOG_STORE_WRITE, =300,"error writing to local log store")
ERROR_CODE(CAUGHT_UP,                 ,"no records with supplied LSN or greater")
ERROR_CODE(UNTIL_LSN_REACHED,         ,"client-provided final LSN reached")
ERROR_CODE(WINDOW_END_REACHED,        ,"hit client flow control window end")
ERROR_CODE(BYTE_LIMIT_REACHED,        ,"hit max_record_bytes_queued")
ERROR_CODE(MALFORMED_RECORD,          ,"malformed record in local log store")
ERROR_CODE(LOCAL_LOG_STORE_READ,      ,"error reading from local log store")

ERROR_CODE(PENDING,                   ,"have pending Appenders")
#undef ERROR_CODE
