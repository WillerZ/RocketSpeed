// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/buffered_storage.h"
#include "src/util/timeout_list.h"

#include "external/folly/move_wrapper.h"
#include "src/messages/msg_loop.h"
#include "src/util/common/coding.h"

namespace rocketspeed {

class BufferedAsyncLogReader : public AsyncLogReader {
 public:
  BufferedAsyncLogReader(
    BufferedLogStorage* storage,
    std::function<bool(LogRecord&)> record_cb,
    std::function<bool(const GapRecord&)> gap_cb,
    std::unique_ptr<AsyncLogReader> reader);

  ~BufferedAsyncLogReader() final;

  Status Open(LogID id,
              SequenceNumber startPoint,
              SequenceNumber endPoint) final;

  Status Close(LogID id) final;

 private:
  std::unique_ptr<AsyncLogReader> reader_;
  BufferedLogStorage* storage_;
};

class BufferedLogStorageWorker {
 public:
  using clock = std::chrono::steady_clock;

  BufferedLogStorageWorker(std::shared_ptr<LogStorage> storage,
                           size_t max_batch_entries,
                           size_t max_batch_bytes,
                           std::chrono::microseconds max_batch_latency,
                           size_t batch_bits)
  : storage_(std::move(storage))
  , max_batch_entries_(max_batch_entries)
  , max_batch_bytes_(max_batch_bytes)
  , max_batch_latency_(max_batch_latency)
  , batch_bits_(batch_bits) {
  }

  void Enqueue(LogID log_id, Slice data, AppendCallback callback) {
    thread_check_.Check();
    auto it = queues_.find(log_id);
    if (it == queues_.end()) {
      // No queue for this log yet, so create one and start timeout.
      auto result = queues_.emplace(log_id, RequestQueue(clock::now()));
      assert(result.second);
      it = result.first;
      timeouts_.Add(log_id);
    }
    // Enqueue request.
    it->second.requests.emplace_back(data, std::move(callback));
    it->second.bytes += data.size();

    // Check if we are at max requests or bytes.
    if (it->second.requests.size() >= max_batch_entries_ ||
        it->second.bytes >= max_batch_bytes_) {
      Flush(log_id);
    }
  }

  void FlushCheck() {
    thread_check_.Check();
    timeouts_.ProcessExpired(
      max_batch_latency_,
      [this] (LogID log_id) {
        // Log requests have been in queue for too long, flush.
        Flush(log_id);
      },
      -1);
  }

  void Flush(LogID log_id) {
    thread_check_.Check();
    auto it = queues_.find(log_id);
    if (it != queues_.end()) {
      // Create encoded string.
      // Consists of 1-byte batch size followed by a serious of
      // length-prefixed slices for each entry.
      std::unique_ptr<std::string> encoded(new std::string());
      encoded->reserve(1 + it->second.bytes);
      uint8_t batch_size = static_cast<uint8_t>(it->second.requests.size());
      PutFixed8(encoded.get(), batch_size);
      for (Request& req : it->second.requests) {
        PutLengthPrefixedSlice(encoded.get(), req.data);
      }

      // Capture the context so that the slices are still valid.
      auto context = folly::makeMoveWrapper(std::move(it->second.requests));

      // Forward encoded string to underlying storage.
      Slice encoded_slice(*encoded);
      storage_->AppendAsync(
        log_id,
        encoded_slice,
        [this, context] (Status st, SequenceNumber seqno) {
          for (size_t i = 0; i < context->size(); ++i) {
            // Invoke callback on original requests with modified seqno.
            (*context)[i].callback(st, seqno << batch_bits_ | i);
          }
        });
      queues_.erase(it);
    }
  }

 private:
  // Single request.
  // Assumes that the context for the data is captured in the callback,
  // or otherwise available through some higher level organization.
  struct Request {
    explicit Request(Slice _data, AppendCallback _callback)
    : data(_data)
    , callback(_callback) {}

    Slice data;
    AppendCallback callback;
  };

  // Queue of request that have not yet been sent to underlying storage.
  struct RequestQueue {
    explicit RequestQueue(clock::time_point _timestamp)
    : timestamp(_timestamp)
    , bytes(0) {}

    std::vector<Request> requests;
    clock::time_point timestamp;
    size_t bytes;
  };

  ThreadCheck thread_check_;
  std::unordered_map<LogID, RequestQueue> queues_;
  TimeoutList<LogID> timeouts_;
  std::shared_ptr<LogStorage> storage_;
  size_t max_batch_entries_;
  size_t max_batch_bytes_;
  std::chrono::microseconds max_batch_latency_;
  size_t batch_bits_;
};

Status BufferedLogStorage::Create(Env* env,
                                  std::shared_ptr<Logger> info_log,
                                  std::shared_ptr<LogStorage> wrapped_storage,
                                  MsgLoop* msg_loop,
                                  size_t max_batch_entries,
                                  size_t max_batch_bytes,
                                  std::chrono::microseconds max_batch_latency,
                                  LogStorage** storage) {
  *storage = new BufferedLogStorage(env,
                                    std::move(info_log),
                                    std::move(wrapped_storage),
                                    msg_loop,
                                    max_batch_entries,
                                    max_batch_bytes,
                                    max_batch_latency);
  return Status::OK();
}

BufferedLogStorage::~BufferedLogStorage() {

}

Status BufferedLogStorage::AppendAsync(LogID id,
                                       const Slice& data,
                                       AppendCallback callback) {
  // Send to the worker thread for batching, sharded by log so that all
  // requests for a log go to the same thread (better batching).
  int worker_id = static_cast<int>(id % msg_loop_->GetNumWorkers());
  Slice data_copy(data);
  auto moved_callback = folly::makeMoveWrapper(std::move(callback));
  std::unique_ptr<Command> cmd(MakeExecuteCommand(
    [this, id, data_copy, moved_callback, worker_id] () mutable {
      workers_[worker_id]->Enqueue(id, data_copy, moved_callback.move());
    }));
  return msg_loop_->SendCommand(std::move(cmd), worker_id);
}

Status BufferedLogStorage::FindTimeAsync(
  LogID id,
  std::chrono::milliseconds timestamp,
  std::function<void(Status, SequenceNumber)> callback) {
  // Forward to underlying storage.
  auto moved_callback = folly::makeMoveWrapper(std::move(callback));
  return storage_->FindTimeAsync(
    id,
    timestamp,
    [this, moved_callback] (Status st, SequenceNumber seqno) {
      // Adjust seqno and invoke callback.
      (*moved_callback)(st, seqno << batch_bits_);
    });
}

Status BufferedLogStorage::CreateAsyncReaders(
  unsigned int parallelism,
  std::function<bool(LogRecord&)> record_cb,
  std::function<bool(const GapRecord&)> gap_cb,
  std::vector<AsyncLogReader*>* readers) {
  return Status::OK();
}

BufferedLogStorage::BufferedLogStorage(
  Env* env,
  std::shared_ptr<Logger> info_log,
  std::shared_ptr<LogStorage> wrapped_storage,
  MsgLoop* msg_loop,
  size_t max_batch_entries,
  size_t max_batch_bytes,
  std::chrono::microseconds max_batch_latency)
: env_(env)
, info_log_(std::move(info_log))
, storage_(std::move(wrapped_storage))
, msg_loop_(msg_loop)
, max_batch_entries_(max_batch_entries)
, max_batch_bytes_(max_batch_bytes)
, max_batch_latency_(max_batch_latency) {
  // Calculate bits for batch.
  batch_bits_ = 0;
  while ((1u << batch_bits_) < max_batch_entries_) {
    ++batch_bits_;
  }
  assert(batch_bits_ <= 8);  // up to 8 bits supported

  int num_workers = msg_loop_->GetNumWorkers();
  for (int i = 0; i < num_workers; ++i) {
    workers_.emplace_back(
      new BufferedLogStorageWorker(storage_,
                                   max_batch_entries_,
                                   max_batch_bytes_,
                                   max_batch_latency_,
                                   batch_bits_));
  }

  // Register timer to check latency induced flushes.
  msg_loop_->RegisterTimerCallback(
    [this] () {
      int worker_id = msg_loop_->GetThreadWorkerIndex();
      workers_[worker_id]->FlushCheck();
    },
    max_batch_latency_ / 10);
}

BufferedAsyncLogReader::BufferedAsyncLogReader(
  BufferedLogStorage* storage,
  std::function<bool(LogRecord&)> record_cb,
  std::function<bool(const GapRecord&)> gap_cb,
  std::unique_ptr<AsyncLogReader> reader)
: reader_(std::move(reader))
, storage_(storage) {

}

BufferedAsyncLogReader::~BufferedAsyncLogReader() {

}

Status BufferedAsyncLogReader::Open(LogID id,
            SequenceNumber startPoint,
            SequenceNumber endPoint) {
  return Status::OK();
}

Status BufferedAsyncLogReader::Close(LogID id) {
  return Status::OK();
}

}  // namespace rocketspeed
