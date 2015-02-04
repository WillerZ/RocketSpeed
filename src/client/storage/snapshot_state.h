// Copyright (c) 2014, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <errno.h>
#include <cstring>
#include <atomic>
#include <memory>

#include "include/Logger.h"
#include "include/Status.h"
#include "include/SubscriptionStorage.h"
#include "src/messages/descriptor_event.h"

namespace rocketspeed {

/**
 * Describes the state of a in-progress snapshot.
 */
class SnapshotState final {
 public:
  SnapshotState(SnapshotCallback callback,
                std::shared_ptr<Logger> info_log,
                const int num_workers,
                std::string write_path,
                std::string read_path,
                int fd)
      : callback_(std::move(callback))
      , info_log_(std::move(info_log))
      , num_workers_(num_workers)
      , write_path_(std::move(write_path))
      , read_path_(std::move(read_path))
      , descriptor_(new DescriptorEvent(info_log, fd))
      , status_(Status::OK())
      , chunks_(new std::string[num_workers]) {
    first_failed_.clear();
  }

  ~SnapshotState() {
    // This will be called once all threads participating in a snapshot finish
    // their job or fail. In the latter case we abort writing out the snapshot.
    // Important: at this point no other thread has a way to modify this
    // structure.

    // Important: we do acquire here in order for all chunks and status of a
    // failed thread to become visible to this thread.
    order_enforcer_.load(std::memory_order_acquire);

    if (!status_.ok()) {
      // We had an error in some thread.
      callback_(std::move(status_));
      return;
    }

    // Attempt to write buffers one by one.
    for (int i = 0; i < num_workers_; ++i) {
      status_ = descriptor_->Write(std::move(chunks_[i]));
      if (!status_.ok()) {
        // Abort snapshot.
        callback_(std::move(status_));
        return;
      }
    }
    // Close write file, so that we can swap it with main DB.
    descriptor_.reset();

    // Commit snapshot.
    if (std::rename(write_path_.c_str(), read_path_.c_str()) != 0) {
      // Abort the snapshot
      callback_(Status::IOError("Snapshot failed when committing state: ",
                                std::string(strerror(errno))));
      return;
    }

    // We've successfully performed snapshot.
    callback_(Status::OK());
  }

  void SnapshotFailed(Status status) {
    assert(!status.ok());
    // Acquire the right to set status, only the first failure gets propagated
    // to the client. Consecutive failures get logged and swallowed.
    if (first_failed_.test_and_set()) {
      LOG_WARN(info_log_,
               "Failed preparing a snapshot %s."
               "This error is overridden by other error in snapshot procedure.",
               status.ToString().c_str());
    } else {
      status_ = std::move(status);
    }
    // Release the status that we've written.
    order_enforcer_.store(false, std::memory_order_release);
  }

  void ChunkSucceeded(int worker_id, std::string buffer) {
    assert(worker_id >= 0 && worker_id < num_workers_);
    chunks_[worker_id] = std::move(buffer);
    // Release the chunk that we've written.
    order_enforcer_.store(false, std::memory_order_release);
  }

 private:
  const SnapshotCallback callback_;
  const std::shared_ptr<Logger> info_log_;
  const int num_workers_;

  // Path to the write file.
  const std::string write_path_;
  // Path to the read file.
  const std::string read_path_;

  // An atomic to perform stores and loads in order to order memory between
  // workers and a thread that writes snapshot to a file.
  std::atomic<bool> order_enforcer_;

  // Destination for snapshot data.
  std::unique_ptr<DescriptorEvent> descriptor_;
  // Leader election for status_.
  std::atomic_flag first_failed_;
  // First failed status of any worker thread.
  Status status_;
  // Snapshot data sharded across workers.
  std::unique_ptr<std::string[]> chunks_;
};

}  // namespace rocketspeed
