//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <atomic>
#include <functional>
#include <vector>

#include "include/SubscriptionStorage.h"
#include "src/messages/descriptor_event.h"
#include "src/util/common/thread_check.h"

namespace rocketspeed {

class BaseEnv;
class Logger;
class Status;
class SubscriptionParameters;

/** A storage strategy which persists subscriptions in a binary file. */
class FileStorage : public SubscriptionStorage {
 public:
  class Snapshot : public SubscriptionStorage::Snapshot {
   public:
    Snapshot(std::string final_path,
             std::string temp_path,
             DescriptorEvent descriptor,
             size_t num_threads);

    Status Append(size_t thread_id,
                  TenantID tenant_id,
                  const NamespaceID& namespace_id,
                  const Topic& topic_name,
                  SequenceNumber start_seqno) override;

    Status Commit() override;

   private:
#ifndef NDEBUG
    std::vector<ThreadCheck> thread_checks_;
#endif  // NDEBUG

    // A path to the file which should be overwritten with snapshot.
    const std::string final_path_;
    // A path to the temporary file, which we directly write.
    const std::string temp_path_;
    // A file descriptor for temporary file.
    DescriptorEvent descriptor_;

    std::vector<std::string> chunks_;
  };

  /**
   * Creates a new file-based storage.
   *
   * @param env An environment used by the client.
   * @param info_log A client's logger.
   * @param file_path Path of a file, where subscriptions will be written.
   */
  FileStorage(BaseEnv* env,
              std::shared_ptr<Logger> info_log,
              std::string file_path);

  Status RestoreSubscriptions(
      std::vector<SubscriptionParameters>* subscriptions) override;

  Status CreateSnapshot(
      size_t num_threads,
      std::shared_ptr<SubscriptionStorage::Snapshot>* snapshot) override;

 private:
  BaseEnv* env_;
  const std::shared_ptr<Logger> info_log_;
  const std::string file_path_;
};

}  // namespace rocketspeed
