// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
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
#include "src/messages/msg_loop.h"
#include "src/messages/event_loop.h"
#include "storage_commands.h"
#include "src/util/common/coding.h"
#include "src/util/common/hash.h"

namespace rocketspeed {

Status SubscriptionStorage::File(BaseEnv* env,
                                 const std::string& file_path,
                                 std::shared_ptr<Logger> info_log,
                                 std::unique_ptr<SubscriptionStorage>* out) {
  out->reset(new FileStorage(env, file_path, info_log));
  return Status::OK();
}

FileStorage::FileStorage(BaseEnv* env,
                         std::string read_path,
                         std::shared_ptr<Logger> info_log)
    : env_(env)
    , info_log_(std::move(info_log))
    , read_path_(std::move(read_path))
    , msg_loop_(nullptr) {
}

FileStorage::~FileStorage(){};

void FileStorage::Initialize(LoadCallback load_callback,
                             MsgLoopBase* msg_loop) {
  using std::placeholders::_1;
  assert(!msg_loop_);
  msg_loop_ = msg_loop;
  assert(msg_loop_);

  // Message loop must not be running.
  assert(!msg_loop_->IsRunning());

  load_callback_ = std::move(load_callback);

  // Empty subscription state, so that c'tor leaves the object in valid state.
  const int num_workers = msg_loop_->GetNumWorkers();
  worker_data_.reset(new WorkerData[num_workers]);

  // Setup handlers for our custom commands.
  auto update_handler = [this](std::unique_ptr<Command> command) {
    HandleUpdateCommand(std::move(command));
  };
  msg_loop_->RegisterCommandCallback(CommandType::kStorageUpdateCommand,
                                     update_handler);

  auto load_handler = [this](std::unique_ptr<Command> command) {
    HandleLoadCommand(std::move(command));
  };
  msg_loop_->RegisterCommandCallback(CommandType::kStorageLoadCommand,
                                     load_handler);

  msg_loop_->RegisterCommandCallback(
      CommandType::kStorageSnapshotCommand,
      std::bind(&FileStorage::HandleSnapshotCommand, this, _1));
}

Status FileStorage::Update(SubscriptionRequest request) {
  const int worker_id = GetWorkerForTopic(request.topic_name);
  std::unique_ptr<Command> command(
      new StorageUpdateCommand(env_->NowMicros(), std::move(request)));
  return msg_loop_->SendCommand(std::move(command), worker_id);
}

Status FileStorage::Load(std::vector<SubscriptionRequest> requests) {
  assert(!requests.empty());

  const int num_workers = msg_loop_->GetNumWorkers();
  std::vector<std::vector<SubscriptionRequest>> sharded(num_workers);
  for (auto& request : requests) {
    const int worker_id = GetWorkerForTopic(request.topic_name);
    assert(0 <= worker_id && worker_id < num_workers);
    sharded[worker_id].push_back(std::move(request));
  }

  Status status;
  for (int worker_id = 0; worker_id < num_workers && status.ok(); ++worker_id) {
    if (sharded[worker_id].empty()) {
      continue;
    }
    std::unique_ptr<Command> command(new StorageLoadCommand(
        env_->NowMicros(), std::move(sharded[worker_id])));
    status = msg_loop_->SendCommand(std::move(command), worker_id);
  }
  // TODO(t6161065) If status != OK then abort command on all threads.
  return status;
}

Status FileStorage::LoadAll() {
  const int num_workers = msg_loop_->GetNumWorkers();
  Status status;
  for (int worker_id = 0; worker_id < num_workers && status.ok(); ++worker_id) {
    std::unique_ptr<Command> command(new StorageLoadCommand(env_->NowMicros()));
    status = msg_loop_->SendCommand(std::move(command), worker_id);
  }
  // TODO(t6161065) If status != OK then abort command on all threads.
  return status;
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
  path->assign(path_c.get());
  return fd;
}

}  // namespace

void FileStorage::WriteSnapshot(SnapshotCallback callback) {
  // Create a new temporary file for writing.
  // Note that O_NOBLOCK has absolutely no effect on reguar file descriptors.
  // Regular files are always readable and writable, therefor we're fine with
  // default flags here.
  std::string write_path;
  int fd = OpenNewTempFile(read_path_, &write_path);
  if (fd < 0) {
    // Abort the snapshot.
    callback(Status::IOError("Cannot open: " + write_path, strerror(errno)));
    return;
  }
  // Assert: nothrow guarantee until fd is owned by snapshot_state.
  auto snapshot_state =
      std::make_shared<SnapshotState>(std::move(callback),
                                      info_log_,
                                      msg_loop_->GetNumWorkers(),
                                      std::move(write_path),
                                      read_path_,
                                      fd);
  // Fan-out to every worker.
  const int num_workers = msg_loop_->GetNumWorkers();
  Status status;
  for (int worker_id = 0; worker_id < num_workers && status.ok(); ++worker_id) {
    std::unique_ptr<Command> command(
        new StorageSnapshotCommand(env_->NowMicros(), snapshot_state));
    status = msg_loop_->SendCommand(std::move(command), worker_id);
  }
  if (!status.ok()) {
    // Fail the snapshot, call back will be invoked by SnapshotState.
    snapshot_state->SnapshotFailed(status);
  }
}

Status FileStorage::ReadSnapshot() {
  // Message loop must not be running.
  assert(!msg_loop_->IsRunning());

  Status status;
  // Open the file.
  std::unique_ptr<SequentialFile> file;
  status = env_->NewSequentialFile(read_path_, &file, env_options_);
  if (!status.ok()) {
    return status;
  }

  // We have to store subscriptions in a temporary structure, so that we do not
  // restore data from a corrupted file.
  const int num_workers = msg_loop_->GetNumWorkers();
  std::unique_ptr<WorkerData[]> new_worker_data(new WorkerData[num_workers]);

  // Read.
  const size_t kHeaderSize =
      sizeof(uint64_t) + sizeof(uint16_t) + sizeof(uint32_t);
  std::vector<char> buffer(kHeaderSize);
  Slice chunk;
  do {
    status = file->Read(kHeaderSize, &chunk, &buffer[0]);
    if (!status.ok()) {
      return status;
    }
    if (chunk.empty()) {
      // End of data.
      break;
    }

    // Sequence number.
    SequenceNumber seqno;
    if (!GetFixed64(&chunk, &seqno)) {
      return Status::InternalError("Bad sequence number");
    }

    // Namespace id.
    NamespaceID namespace_id;
    if (!GetFixed16(&chunk, &namespace_id)) {
      return Status::InternalError("Bad namespace id");
    }

    // Topic name.
    uint32_t name_size;
    if (!GetFixed32(&chunk, &name_size)) {
      return Status::InternalError("Bad topic name length");
    }
    if (buffer.size() < name_size) {
      buffer.resize(name_size);
    }
    status = file->Read(name_size, &chunk, &buffer[0]);
    if (name_size != chunk.size()) {
      return Status::InternalError("Bad topic");
    }
    TopicID topic_id(namespace_id, std::string(&buffer[0], buffer.size()));

    // Assign entry to the right worker.
    const int worker_id = GetWorkerForTopic(topic_id.topic_name);
    new_worker_data[worker_id].topics_subscribed.emplace(
        std::move(topic_id), SubscriptionState(seqno));
  } while (true);

  // Commit. No synchronization is needed, as worker threads are not running.
  worker_data_ = std::move(new_worker_data);

  return Status::OK();
}

void FileStorage::HandleUpdateCommand(std::unique_ptr<Command> command) {
  StorageUpdateCommand* update =
      static_cast<StorageUpdateCommand*>(command.get());
  auto& worker_data = GetCurrentWorkerData();

  // Find an entry for the topic if it exists.
  auto iter = worker_data.Find(&update->request);

  const SubscriptionRequest& request = update->request;
  if (request.subscribe) {
    if (request.start) {
      SequenceNumber new_seqno = request.start.get();
      // Add or update subscription.
      if (iter == worker_data.topics_subscribed.end()) {
        // We're missing entry for this topic.
        SubscriptionState state(new_seqno);
        TopicID topic_id(request.namespace_id, request.topic_name);
        worker_data.topics_subscribed.emplace(std::move(topic_id), state);
      } else {
        // We alreaady have an entry for this topic, so updating seq no only.
        // We do not need a lock for this, since current thread is the only
        // modifier and performs update atomically.
        iter->second.IncreaseSequenceNumber(new_seqno);
      }
    } else {
      // Otherwise we just ignore. Without knowing the sequence number, we
      // cannot restore the subscription.
    }
  } else {
    // Remove subscription.
    if (iter != worker_data.topics_subscribed.end()) {
      // We have to remove an entry.
      worker_data.topics_subscribed.erase(iter);
    } else {
      LOG_WARN(info_log_,
               "Attempt to unsubscribe from already unsubscribed Topic(%d, %s)",
               request.namespace_id,
               request.topic_name.c_str());
      info_log_->Flush();
    }
  }
}

void FileStorage::HandleLoadCommand(std::unique_ptr<Command> command) {
  StorageLoadCommand* load = static_cast<StorageLoadCommand*>(command.get());
  auto& worker_data = GetCurrentWorkerData();

  if (load->query.empty()) {
    // There is no list of subscriptions to query, return all state.
    for (const auto& pair : worker_data.topics_subscribed) {
      load->query.emplace_back(pair.first.namespace_id,
                               pair.first.topic_name,
                               true,
                               pair.second.GetSequenceNumber());
    }
  } else {
    for (auto& request : load->query) {
      // Find an entry for the topic if it exists.
      auto iter = worker_data.Find(&request);
      if (iter != worker_data.topics_subscribed.end()) {
        request.start = iter->second.GetSequenceNumber();
      } else {
        // No state found for this topic.
        request.start = SubscriptionStart();
      }
    }
  }

  // Invoke callback with both successful and failed queries.
  load_callback_(load->query);
}

void FileStorage::HandleSnapshotCommand(std::unique_ptr<Command> command) {
  StorageSnapshotCommand* snapshot =
      static_cast<StorageSnapshotCommand*>(command.get());
  const int worker_id = msg_loop_->GetThreadWorkerIndex();
  auto& worker_data = worker_data_[worker_id];
  std::string buffer;
  // Write topics in this part.
  for (auto& it : worker_data.topics_subscribed) {
    PutFixed64(&buffer, it.second.GetSequenceNumber());
    PutFixed16(&buffer, it.first.namespace_id);
    PutFixed32(&buffer, static_cast<uint32_t>(it.first.topic_name.size()));
    buffer.append(it.first.topic_name);
  }
  // Append to snapshot state.
  snapshot->snapshot_state->ChunkSucceeded(worker_id, std::move(buffer));
}

int FileStorage::GetWorkerForTopic(const Topic& topic_name) const {
  const int num_workers = msg_loop_->GetNumWorkers();
  return MurmurHash2<std::string>()(topic_name) % num_workers;
}

FileStorage::WorkerData& FileStorage::GetCurrentWorkerData() {
  return worker_data_[msg_loop_->GetThreadWorkerIndex()];
}

}  // namespace rocketspeed
