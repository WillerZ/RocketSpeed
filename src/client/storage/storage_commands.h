// Copyright (c) 2014, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <memory>

#include "src/client/storage/snapshot_state.h"
#include "src/messages/commands.h"

namespace rocketspeed {

/**
 * Triggers update of subscription state.
 */
class StorageUpdateCommand final : public Command {
 public:
  SubscriptionRequest request;

  explicit StorageUpdateCommand(SubscriptionRequest _request)
      : request(_request) {}

  CommandType GetCommandType() const { return kStorageUpdateCommand; }
};

/**
 * Triggers loading of subscription state, empty query vector means that all
 * available state should be loaded.
 */
class StorageLoadCommand final : public Command {
 public:
  std::vector<SubscriptionRequest> query;

  explicit StorageLoadCommand() {}

  explicit StorageLoadCommand(std::vector<SubscriptionRequest> _query)
      : query(_query) {}

  CommandType GetCommandType() const { return kStorageLoadCommand; }
};

/**
 * Triggers dumping of subscription state to the underlying file.
 */
class StorageSnapshotCommand final : public Command {
 public:
   std::shared_ptr<SnapshotState> snapshot_state;

  explicit StorageSnapshotCommand(
    std::shared_ptr<SnapshotState> _snapshot_state)
      : snapshot_state(std::move(_snapshot_state)) {}

  CommandType GetCommandType() const { return kStorageSnapshotCommand; }
};

}  // namespace rocketspeed
