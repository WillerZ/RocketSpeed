/* -*- Mode: C++; tab-width: 2; c-basic-offset: 2; indent-tabs-mode: nil -*- */
#pragma once

#include <cstdint>
#include <climits>

#include "logdevice/include/strong_typedef.h"

namespace facebook { namespace logdevice {

// Logs are uniquely identified by 64-bit ints
LOGDEVICE_STRONG_TYPEDEF(int64_t, logid_t);

constexpr logid_t LOGID_INVALID(0);
constexpr logid_t LOGID_MAX((1ull<<48)-1); //48 bits

// Log sequence numbers are 64-bit unsigned ints.
typedef uint64_t lsn_t;

// 0 is not a valid LSN.
const lsn_t LSN_INVALID = 0;

// Guaranteed to be less than or equal to the smallest valid LSN possible.
// Use this to seek to the oldest record in a log.
const lsn_t LSN_OLDEST = 1;

// Greatest valid LSN possible plus one.
const lsn_t LSN_MAX = ULLONG_MAX;

}} // namespace
