//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "file_storage.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <vector>

#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "include/Logger.h"
#include "src/messages/descriptor_event.h"
#include "src/client/topic_id.h"
#include "src/util/common/base_env.h"
#include "src/util/common/coding.h"

namespace rocketspeed {

////////////////////////////////////////////////////////////////////////////////
Status SubscriptionStorage::File(BaseEnv* env,
                                 std::shared_ptr<Logger> info_log,
                                 std::string file_path,
                                 std::unique_ptr<SubscriptionStorage>* out) {
  out->reset(new FileStorage(env, info_log, std::move(file_path)));
  return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////
FileStorage::Snapshot::Snapshot(std::string final_path,
                                std::string temp_path,
                                DescriptorEvent descriptor,
                                size_t num_threads)
    : final_path_(std::move(final_path))
    , temp_path_(std::move(temp_path))
    , descriptor_(std::move(descriptor)) {
#ifndef NDEBUG
  thread_checks_.resize(num_threads);
#endif  // NDEBUG
  chunks_.resize(num_threads);
}

Status FileStorage::Snapshot::Append(size_t thread_id,
                                     TenantID tenant_id,
                                     const NamespaceID& namespace_id,
                                     const Topic& topic_name,
                                     SequenceNumber start_seqno) {
#ifndef NDEBUG
  assert(thread_id < thread_checks_.size());
  thread_checks_[thread_id].Check();
#endif  // NDEBUG

  auto buffer = &chunks_[thread_id];

  PutFixed16(buffer, tenant_id);
  PutFixed64(buffer, start_seqno);
  std::string topic_id;
  PutTopicID(&topic_id, namespace_id, topic_name);
  PutFixed32(buffer, static_cast<uint32_t>(topic_id.size()));
  buffer->append(topic_id);

  return Status::OK();
}

Status FileStorage::Snapshot::Commit() {
  // Attempt to write buffers one by one.
  for (auto& chunk : chunks_) {
    Status st = descriptor_.Write(chunk);
    if (!st.ok()) {
      return st;
    }
  }
  // Close temp file, so that we can swap it with the destination file.
  descriptor_ = DescriptorEvent();

  // Commit snapshot.
  if (std::rename(temp_path_.c_str(), final_path_.c_str()) != 0) {
    return Status::IOError("Snapshot failed when committing state: ",
                           std::string(strerror(errno)));
  }
  return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////
FileStorage::FileStorage(BaseEnv* env,
                         std::shared_ptr<Logger> info_log,
                         std::string file_path)
    : env_(env)
    , info_log_(std::move(info_log))
    , file_path_(std::move(file_path)) {
}

Status FileStorage::RestoreSubscriptions(
    std::vector<SubscriptionParameters>* subscriptions) {
  // Open the file.
  std::unique_ptr<SequentialFile> file;
  EnvOptions env_options;
  Status st = env_->NewSequentialFile(file_path_, &file, env_options);
  if (!st.ok()) {
    return st;
  }

  std::vector<SubscriptionParameters> result;

  const size_t kHeaderSize =
      sizeof(uint16_t) + sizeof(uint64_t) + sizeof(uint32_t);
  std::vector<char> buffer(kHeaderSize);
  Slice chunk;

  do {
    st = file->Read(kHeaderSize, &chunk, &buffer[0]);
    if (!st.ok()) {
      return st;
    }
    if (chunk.empty()) {
      // End of data.
      break;
    }

    // TenantID.
    TenantID tenant_id;
    if (!GetFixed16(&chunk, &tenant_id)) {
      return Status::IOError("Bad tenant ID");
    }

    // SequenceNumber.
    SequenceNumber seqno;
    if (!GetFixed64(&chunk, &seqno)) {
      return Status::IOError("Bad sequence number");
    }

    // TopicID.
    uint32_t name_size;
    if (!GetFixed32(&chunk, &name_size)) {
      return Status::IOError("Bad topic name length");
    }
    if (buffer.size() < name_size) {
      buffer.resize(name_size);
    }
    st = file->Read(name_size, &chunk, &buffer[0]);
    if (!st.ok()) {
      return st;
    }
    if (name_size != chunk.size()) {
      return Status::IOError("Bad topic ID");
    }

    // NamespaceID and Topic.
    Slice buffer_in(buffer.data(), buffer.size());
    NamespaceID namespace_id;
    Topic topic_name;
    if (!GetTopicID(&buffer_in, &namespace_id, &topic_name)) {
      return Status::IOError("Bad topic ID");
    }
    TopicID topic_id(namespace_id, topic_name);

    result.emplace_back(
        tenant_id, std::move(namespace_id), std::move(topic_name), seqno);
  } while (true);

  // Successfully read all subscriptions.
  *subscriptions = std::move(result);

  return Status::OK();
}

namespace {

/**
 * A helper function which creates and opens a new file whose name starts with
 * given prefix. The file is guaranteed to be opened exclusively for the caller.
 * Returns file descriptor of the file and updates path with actual file path
 * when completed successfully or returns -1 and does not modify path on error.
 */
int OpenNewTempFile(std::string prefix, std::string* path) {
  prefix += "XXXXXX";
  std::unique_ptr<char[]> path_c(new char[prefix.size() + 1]);
  std::copy(prefix.begin(), prefix.end(), path_c.get());
  path_c[prefix.size()] = '\0';
  int fd = mkstemp(path_c.get());
  if (fd < 0) {
    return -1;
  }
  // Note that O_NOBLOCK has absolutely no effect on reguar file descriptors.
  // Regular files are always readable and writable, therefore we're fine with
  // default flags here.
  path->assign(path_c.get());
  return fd;
}

}  // namespace

Status FileStorage::CreateSnapshot(
    size_t num_threads,
    std::shared_ptr<SubscriptionStorage::Snapshot>* snapshot) {
  // Create a new temporary file for writing.
  std::string temp_path;
  int fd = OpenNewTempFile(file_path_, &temp_path);
  if (fd < 0) {
    return Status::IOError("Cannot open: " + temp_path, strerror(errno));
  }
  DescriptorEvent descriptor(fd);
  fd = -1;

  snapshot->reset(new Snapshot(
      file_path_, std::move(temp_path), std::move(descriptor), num_threads));
  return Status::OK();
}

}  // namespace rocketspeed
