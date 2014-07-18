/* -*- Mode: C++; tab-width: 2; c-basic-offset: 2; indent-tabs-mode: nil -*- */
#pragma once

#include <cstdint>
#include "logdevice/include/EnumMap.h"

namespace facebook { namespace logdevice {

/**
 * Errors reported by various methods and functions in the public LogDevice
 * API as well as in the implementation. I chose a short name to improve
 * the readability of error handling code.
 */
enum class E : std::uint16_t {
#define ERROR_CODE(id, val, str) id val,
#include "errors.inc"
  UNKNOWN = 1024, // a special value that variables of type E (Status) may
                  // assume before any status is known. Never reported.
  MAX
};

typedef E Status; // a longer, more descriptive alias to use in function
                  // declarations.


// a (name, description) record for an error code
struct ErrorCodeInfo {
  const char *name;
  const char *description;

  bool valid() { return name != nullptr; }

  static const ErrorCodeInfo invalid;
};


/**
 * All LogDevice functions report failures through this thread-local. This
 * is a LogDevice-level errno.
 */
extern __thread E err;


/**
 * errorStrings is a sole instance of an EnumMap specialization that
 * maps logdevice::E error codes into short names and longer full
 * descriptions (that also include name)
 */
extern EnumMap<E, ErrorCodeInfo> errorStrings;

}} // namespace
