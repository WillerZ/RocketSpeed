/* -*- Mode: C++; tab-width: 2; c-basic-offset: 2; indent-tabs-mode: nil -*- */
#pragma once


namespace facebook { namespace logdevice {

namespace dbg {


enum class Level : unsigned {
  NONE,
  CRITICAL,
  ERROR,
  WARNING,
  NOTIFY,
  INFO,
  DEBUG,
  SPEW
};


/**
 * log errors and debug messages at this level or worse. Default is
 * INFO. NONE is not a valid debug level and should not be used. The
 * same value is used for all logdevice::Client objects. Applications
 * can change this variable at any time.
 */
extern Level currentLevel;


/**
 * This function directs all error and debug output to the specified
 * fd. -1 or any other negative value turns off all logging. Until
 * this function is called, all LogDevice error and debug output will
 * go to stderr (fd 2).
 *
 * @return the previous value of debug log fd
 */
int useFD(int fd);

} //namespace dbg

}} //outer namespaces
