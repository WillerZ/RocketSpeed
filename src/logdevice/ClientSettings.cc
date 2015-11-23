// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include <stdint.h>
#include "logdevice/include/ClientSettings.h"

namespace facebook { namespace logdevice {

class ClientSettingsImpl : public ClientSettings {
 public:
  ClientSettingsImpl() {}
};

ClientSettings* ClientSettings::create() {
  return new ClientSettingsImpl();
}

int ClientSettings::set(const char *name, const char *value) {
  return 0;
}

int ClientSettings::set(const char *name, int64_t value) {
  return 0;
}

ClientSettingsImpl* ClientSettings::impl() {
  return static_cast<ClientSettingsImpl*>(this);
}

}  // namespace logdevice
}  // namespace facebook
