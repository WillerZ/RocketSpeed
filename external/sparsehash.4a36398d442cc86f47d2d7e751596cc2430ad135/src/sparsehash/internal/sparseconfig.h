/*
 * NOTE: This file is for internal use only.
 *       Do not use these #defines in your own program!
 */

#pragma once

#include "smhash_definitions.h"

/* Namespace for Google classes */
#define ROCKETSPEED_NAMESPACE ::rocketspeed

/* the location of the header defining hash functions */
#define ROCKETSPEED_HASH_FUN_H <functional>

/* the namespace of the hash<> function */
#define ROCKETSPEED_HASH_NAMESPACE rocketspeed 

/* Define to 1 if you have the <inttypes.h> header file. */
#define ROCKETSPEED_HAVE_INTTYPES_H 1

/* Define to 1 if the system has the type `long long'. */
#define ROCKETSPEED_HAVE_LONG_LONG 1

/* Define to 1 if you have the `memcpy' function. */
#define ROCKETSPEED_HAVE_MEMCPY 1

/* Define to 1 if you have the <stdint.h> header file. */
#define ROCKETSPEED_HAVE_STDINT_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#define ROCKETSPEED_HAVE_SYS_TYPES_H 1

/* Define to 1 if the system has the type `uint16_t'. */
#define ROCKETSPEED_HAVE_UINT16_T 1

/* Define to 1 if the system has the type `u_int16_t'. */
#define ROCKETSPEED_HAVE_U_INT16_T 1

/* Define to 1 if the system has the type `__uint16'. */
/* #undef HAVE___UINT16 */

/* The system-provided hash function including the namespace. */
#define ROCKETSPEED_SPARSEHASH_HASH ROCKETSPEED_HASH_NAMESPACE::SMHash

/* Stops putting the code inside the Google namespace */
#define _END_ROCKETSPEED_NAMESPACE_ }

/* Puts following code inside the Google namespace */
#define _START_ROCKETSPEED_NAMESPACE_ namespace rocketspeed {
