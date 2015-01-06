// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "file_storage.h"

#include <cstdio>
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

Status SubscriptionStorage::File(const std::string& file_path,
                                 std::shared_ptr<Logger> info_log,
                                 std::unique_ptr<SubscriptionStorage>* out) {
  out->reset(new FileStorage(file_path,
                             info_log));
  return Status::OK();
}

FileStorage::FileStorage(std::string read_path,
                         std::shared_ptr<Logger> info_log)
    : env_(Env::Default()),
      info_log_(std::move(info_log)),
      write_path_(read_path + ".tmp"),
      // Field write_path_ will be initialized first, so we do not need
      // read_path anymore.
      read_path_(std::move(read_path)),
      msg_loop_(nullptr),
      running_(false) {};

FileStorage::~FileStorage() {};

void FileStorage::Initialize(LoadCallback load_callback,
                             UpdateCallback update_callback,
                             SnapshotCallback write_snapshot_callback,
                             MsgLoopBase* msg_loop) {
  assert(!msg_loop_);
  msg_loop_ = msg_loop;
  assert(msg_loop_);

  // Message loop must not be running.
  assert(!msg_loop_->IsRunning());

  load_callback_ = std::move(load_callback);
  update_callback_ = std::move(update_callback);
  write_snapshot_callback_ = std::move(write_snapshot_callback);

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

  auto snapshot_handler = [this](std::unique_ptr<Command> command) {
    InitiateSnapshot();
  };
  msg_loop_->RegisterCommandCallback(CommandType::kStorageSnapshotCommand,
                                     snapshot_handler);
}

void FileStorage::Update(SubscriptionRequest request) {
  const int worker_id = GetWorkerForTopic(request.topic_name);
  std::unique_ptr<Command> command(
      new StorageUpdateCommand(env_->NowMicros(),
                               std::move(request)));
  msg_loop_->SendCommand(std::move(command), worker_id);
}

void FileStorage::Load(std::vector<SubscriptionRequest> requests) {
  assert(!requests.empty());

  const int num_workers = msg_loop_->GetNumWorkers();
  std::vector<std::vector<SubscriptionRequest>> sharded(num_workers);
  for (auto& request : requests) {
    const int worker_id = GetWorkerForTopic(request.topic_name);
    assert(0 <= worker_id && worker_id < num_workers);
    sharded[worker_id].push_back(std::move(request));
  }

  for (int worker_id = 0; worker_id < num_workers; ++worker_id) {
    if (!sharded[worker_id].empty()) {
      std::unique_ptr<Command> command(
          new StorageLoadCommand(env_->NowMicros(),
                                 std::move(sharded[worker_id])));
      msg_loop_->SendCommand(std::move(command), worker_id);
    }
  }
}

void FileStorage::LoadAll() {
  const int num_workers = msg_loop_->GetNumWorkers();
  for (int worker_id = 0; worker_id < num_workers; ++worker_id) {
    // Default c'tor is guaranteed to do no allocations.
    std::vector<SubscriptionRequest> empty;
    std::unique_ptr<Command> command(
        new StorageLoadCommand(env_->NowMicros(),
                               std::move(empty)));
    msg_loop_->SendCommand(std::move(command), worker_id);
  }
}

void FileStorage::WriteSnapshot() {
  if (!running_.exchange(true)) {
    // Snapshot thread inherits a critical section.
    std::unique_ptr<Command> command(
        new StorageSnapshotCommand(env_->NowMicros()));
    msg_loop_->SendCommand(std::move(command),
                           msg_loop_->LoadBalancedWorkerId());
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
        // We have to acquire lock, since we will be modifying the map, possibly
        // reallocating memory.
        std::lock_guard<std::mutex> lock(worker_data.topics_subscribed_mutex);
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
      // We have to acquire lock, since we will be modifying the map.
      std::lock_guard<std::mutex> lock(worker_data.topics_subscribed_mutex);
      worker_data.topics_subscribed.erase(iter);
    } else {
      LOG_WARN(info_log_,
               "Attempt to unsubscribe from already unsubscribed Topic(%d, %s)",
               request.namespace_id,
               request.topic_name.c_str());
      info_log_->Flush();
      assert(0);
    }
  }

  // Fire the callback
  update_callback_(request);
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
                               pair.second.GetSequenceNumberRelaxed());
    }
  } else {
    for (auto& request : load->query) {
      // Find an entry for the topic if it exists.
      auto iter = worker_data.Find(&request);
      if (iter != worker_data.topics_subscribed.end()) {
        request.start = iter->second.GetSequenceNumberRelaxed();
      } else {
        // No state found for this topic.
        request.start = SubscriptionStart();
      }
    }
  }

  // Invoke callback with both successful and failed queries.
  load_callback_(load->query);
}

void FileStorage::InitiateSnapshot() {
  // A single snapshot should be handled by a single thread, but different
  // snapshots might be performed by different threads.
  thread_check_.Reset();

  status_ = Status::OK();
  // Create a new file for writing.
  // Note that O_NOBLOCK has absolutely no effect on reguar file descriptors,
  // as stated in POSIX documentation. Regular files are always readable and
  // writable. The writes might be arbitrarily slow though.
  int fd = open(write_path_.c_str(),
                O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NONBLOCK,
                S_IRUSR | S_IWUSR);
  if (fd < 0) {
    status_ = Status::IOError("Cannot open: " + write_path_, strerror(errno));
    // Abort the snapshot.
    FinalizeSnapshot();
    return;
  }

  // Setup descriptor event.
  descriptor_.reset(new DescriptorEvent(info_log_, fd));
  const int num_workers = msg_loop_->GetNumWorkers();
  remaining_writes_ = num_workers;
  auto callback = [this](Status status) {
    thread_check_.Check();
    if (!status_.ok()) {
      status_ = status;
    }
    if (--remaining_writes_ == 0) {
      FinalizeSnapshot();
    }
  };

  // Serialize and write parts.
  for (int i = 0; i < num_workers; ++i) {
    auto& data = worker_data_[i];
    // TODO(stupaq #5930245)
    // Get rid of all allocations under the lock by estimating buffer size after
    // each insert/remove and storing it in an atomic.
    // If we misallocate outside of the lock, we can do single reallocation
    // inside, but that will not be a common case (hopefully).
    std::string buffer;
    {
      // We have to acquire lock, there might be concurrent insertion/removal.
      std::lock_guard<std::mutex> lock(data.topics_subscribed_mutex);
      // Write topics in this part.
      for (auto& it : data.topics_subscribed) {
        PutFixed64(&buffer, it.second.GetSequenceNumber());
        PutFixed16(&buffer, it.first.namespace_id);
        PutFixed32(&buffer, it.first.topic_name.size());
        buffer.append(it.first.topic_name);
      }
    }
    // Enqueue write outside of the lock.
    descriptor_->Enqueue(std::move(buffer), callback);
  }
}

void FileStorage::FinalizeSnapshot() {
  thread_check_.Check();

  // Close file descriptor.
  descriptor_.reset();
  // Commit snapshot if was successful.
  if (!status_.ok()) {
    LOG_WARN(info_log_,
             "Snapshot failed (%s)",
             status_.ToString().c_str());
    info_log_->Flush();
  } else if (std::rename(write_path_.c_str(), read_path_.c_str()) != 0) {
    LOG_WARN(info_log_,
             "Snapshot failed when substituting files (%s)",
             strerror(errno));
    info_log_->Flush();
  }

  // Snapshot is done now.
  running_ = false;
  write_snapshot_callback_(status_);
}

int FileStorage::GetWorkerForTopic(const Topic& topic_name) const {
  const int num_workers = msg_loop_->GetNumWorkers();
  return MurmurHash2<std::string>()(topic_name) % num_workers;
}

FileStorage::WorkerData& FileStorage::GetCurrentWorkerData() {
  return worker_data_[msg_loop_->GetThreadWorkerIndex()];
}

}  // namespace rocketspeed
