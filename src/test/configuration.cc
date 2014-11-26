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
                      const std::vector<HostId>& copilots,
                      TenantID tenant_id,
                      int num_workers)
  : pilots_(pilots)
  , copilots_(copilots)
  , tenant_id_(tenant_id)
  , num_workers_(num_workers) {
  }

  virtual ~ManualConfiguration() {
  }

  virtual const std::vector<HostId>& GetPilotHostIds() const {
    return pilots_;
  }

  virtual const std::vector<HostId>& GetCopilotHostIds() const {
    return copilots_;
  }

  virtual TenantID GetTenantID() const {
    return tenant_id_;
  }

  virtual int GetNumWorkers() const {
    return num_workers_;
  }

 private:
  std::vector<HostId> pilots_;
  std::vector<HostId> copilots_;
  TenantID tenant_id_;
  int num_workers_;
};

Configuration* Configuration::Create(const std::vector<HostId>& pilots,
                                     const std::vector<HostId>& copilots,
                                     TenantID tenant_id,
                                     int num_workers) {
  return new ManualConfiguration(pilots,
                                 copilots,
                                 tenant_id,
                                 num_workers);
}

}  // namespace rocketspeed
