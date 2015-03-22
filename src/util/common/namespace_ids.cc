// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "include/Types.h"

namespace rocketspeed {

bool IsReserved(const NamespaceID& ns) {
  return !ns.empty() && ns[0] == '_';
}

const NamespaceID InvalidNamespace("");

const NamespaceID GuestNamespace("guest");

const NamespaceID SytemNamespacePermanent("_sys/perm");

}  // namespace rocketspeed
