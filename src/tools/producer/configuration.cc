// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "include/RocketSpeed.h"
#include "include/Slice.h"
#include "include/Status.h"
#include "include/Types.h"

namespace rocketspeed {

/**
 * Simple implementation of Configuration where the user manually specifies
 * the pilot hostname and port to connect to. In production, these will be
 * retrieved from something like Zookeeper.
 */
class ManualConfiguration : public Configuration {
 public:
  ManualConfiguration(const std::vector<HostId>& pilots,
                      TenantID tenant_id,
                      int local_port)
  : pilots_(pilots)
  , tenant_id_(tenant_id)
  , local_port_(local_port) {
  }

  virtual ~ManualConfiguration() {
  }

  virtual const std::vector<HostId>& GetPilotHostIds() const {
    return pilots_;
  }

  virtual TenantID GetTenantID() const {
    return tenant_id_;
  }

  virtual int GetLocalPort() const {
    return local_port_;
  }

 private:
  std::vector<HostId> pilots_;
  TenantID tenant_id_;
  int local_port_;
};

Configuration* Configuration::Create(const std::vector<HostId>& pilots,
                                     TenantID tenant_id,
                                     int local_port) {
  return new ManualConfiguration(pilots,
                                 tenant_id,
                                 local_port);
}

}  // namespace rocketspeed
