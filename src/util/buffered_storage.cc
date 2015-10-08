// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "buffered_storage.h"

#include <chrono>
#include <deque>
#include <memory>
#include <unordered_map>

#include "external/folly/move_wrapper.h"
#include "src/messages/msg_loop.h"
#include "src/util/common/coding.h"
#include "src/util/common/thread_check.h"
#include "src/util/timeout_list.h"
#include "src/util/unsafe_shared_ptr.h"

namespace rocketspeed {

class BufferedAsyncLogReader : public AsyncLogReader {
 public:
  BufferedAsyncLogReader(BufferedLogStorage* storage,
                         std::function<bool(LogRecord&)> record_cb,
                         std::function<bool(const GapRecord&)> gap_cb);

  ~BufferedAsyncLogReader() final;

  Status Open(LogID id,
              SequenceNumber startPoint,
              SequenceNumber endPoint) final;

  Status Close(LogID id) final;

 private:
  friend class ReaderCallbacks;

  BufferedLogStorage* const buffered_storage_;
  const std::function<bool(LogRecord&)> record_cb_;
  const std::function<bool(const GapRecord&)> gap_cb_;

  ThreadCheck thread_check_;
  std::unordered_map<LogID, std::unique_ptr<AsyncLogReader>>
      underlying_readers_;
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
      // Capture the serialised payload as well.
      auto moved_encoded = folly::makeMoveWrapper(std::move(encoded));
      storage_->AppendAsync(
        log_id,
        encoded_slice,
        [this, context, moved_encoded] (Status st, SequenceNumber seqno) {
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
    const std::function<bool(LogRecord&)> record_cb,
    const std::function<bool(const GapRecord&)> gap_cb,
    std::vector<AsyncLogReader*>* out) {
  if (!out) {
    return Status::InvalidArgument("out parameter must not be null.");
  }

  out->reserve(parallelism);
  while (parallelism-- > 0) {
    out->push_back(new BufferedAsyncLogReader(this, record_cb, gap_cb));
  }

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
  ((void)env_);
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
    BufferedLogStorage* buffered_storage,
    std::function<bool(LogRecord&)> record_cb,
    std::function<bool(const GapRecord&)> gap_cb)
: buffered_storage_(buffered_storage)
, record_cb_(std::move(record_cb))
, gap_cb_(std::move(gap_cb)) {}

BufferedAsyncLogReader::~BufferedAsyncLogReader() {}

class ReaderCallbacks {
 public:
  ReaderCallbacks(BufferedAsyncLogReader* buffered_reader, size_t batch_bits)
  : buffered_reader_(buffered_reader)
  , batch_bits_(batch_bits)
  , overflow_gap_present_(false) {}

  bool DeliverRecord(LogRecord& batch_record) {
    thread_check_.Check();

    // We never start processing a new record until we're done flushing the
    // overflow queue and gap.
    if (!FlushOverflow()) {
      return false;
    }
    assert(overflow_records_.empty());

    // If we've returned false to apply backpressure in the middle of processing
    // a payload, then the LogRecord will be empty. In this case we skip it
    // after flusing the overflow queue.
    // BufferedStorage's write path will never append an empty record.
    if (batch_record.payload.empty()) {
      return true;
    }
    // Capture the context in a shared pointer. All records in a batch share
    // the ownership of the batch record.
    std::shared_ptr<void> shared_context(batch_record.context.release(),
                                         batch_record.context.get_deleter());
    // Even though we've moved away the LogRecord's context, we can still return
    // false to apply backpressure, see documentation of CreateAsyncReaders.
    // In this case, we will be delivered an empty record with the same sequence
    // number on the next attempt, which is handled above.

    SequenceNumber current_seqno = batch_record.seqno << batch_bits_,
                   last_seqno = current_seqno + (1 << batch_bits_) - 1;
    Slice remaining = batch_record.payload;
    // Deserialize the number of records.
    uint8_t batch_size;
    if (!GetFixed8(&remaining, &batch_size)) {
      // There are not pending records, so this gap will be delivered first.
      return EnqueueGap({
          GapType::kDataLoss,
          batch_record.log_id,
          current_seqno,
          last_seqno,
      });
    }

    // Deserialize individual records from the batch and add to the queue.
    while (batch_size-- > 0) {
      Slice payload;
      if (!GetLengthPrefixedSlice(&remaining, &payload)) {
        return EnqueueGap({
            GapType::kDataLoss,
            batch_record.log_id,
            current_seqno,
            last_seqno,
        });
      }

      // Prepare a handle to the context.
      std::unique_ptr<std::shared_ptr<void>> context(new std::shared_ptr<void>);
      *context = shared_context;

      // Add the record.
      EnqueueRecord(LogRecord(batch_record.log_id,
                              payload,
                              current_seqno,
                              batch_record.timestamp,
                              EraseType(std::move(context))));
      ++current_seqno;
    }
    assert(remaining.empty());

    // Add a gap to cover missing sequence numbers.
    if (current_seqno <= last_seqno) {
      EnqueueGap({
          GapType::kBenign,
          batch_record.log_id,
          current_seqno,
          last_seqno,
      });
    }

    return IsOverflowEmpty();
  }

  bool DeliverGap(const GapRecord& batch_gap) {
    thread_check_.Check();

    // We never start processing a new record until we're done flushing the
    // overflow queue and gap.
    if (!FlushOverflow()) {
      return false;
    }
    assert(overflow_records_.empty());

    // Since a batched gap always maps to a single call of the callback, we can
    // use backoff straight away.
    size_t length = batch_gap.to - batch_gap.from + 1;
    SequenceNumber current_seqno = batch_gap.from << batch_bits_,
                   last_seqno = current_seqno + (length << batch_bits_) - 1;
    return buffered_reader_->gap_cb_(
        {batch_gap.type, batch_gap.log_id, current_seqno, last_seqno});
  }

 private:
  BufferedAsyncLogReader* const buffered_reader_;
  const size_t batch_bits_;

  ThreadCheck thread_check_;
  std::deque<LogRecord> overflow_records_;
  bool overflow_gap_present_;
  GapRecord overflow_gap_;

  bool EnqueueGap(GapRecord gap) {
    assert(!overflow_gap_present_);
    if (!IsOverflowEmpty() || !buffered_reader_->gap_cb_(gap)) {
      overflow_gap_ = std::move(gap);
      overflow_gap_present_ = true;
      return false;
    }
    return true;
  }

  bool EnqueueRecord(LogRecord record) {
    assert(!overflow_gap_present_);
    if (!IsOverflowEmpty() || !buffered_reader_->record_cb_(record)) {
      overflow_records_.emplace_back(std::move(record));
      return false;
    }
    return true;
  }

  bool IsOverflowEmpty() {
    return overflow_records_.empty() && !overflow_gap_present_;
  }

  bool FlushOverflow() {
    // Flush records first.
    while (!overflow_records_.empty()) {
      if (!buffered_reader_->record_cb_(overflow_records_.front())) {
        return false;
      }
      assert(!overflow_records_.front().context.get() ||
             overflow_records_.front().context.get_deleter());
      overflow_records_.pop_front();
    }
    // And then any gap.
    if (overflow_gap_present_) {
      if (!buffered_reader_->gap_cb_(overflow_gap_)) {
        return false;
      }
      overflow_gap_present_ = false;
    }
    return true;
  }
};

Status BufferedAsyncLogReader::Open(LogID log_id,
                                    SequenceNumber start_point,
                                    SequenceNumber end_point) {
  thread_check_.Check();

  // Close the log before (re)starting reading it.
  auto st = Close(log_id);
  if (!st.ok()) {
    return st;
  }

  // Each log needs independent flow control state, which has to be shared by
  // record and gap callback.
  auto callback =
      std::make_shared<ReaderCallbacks>(this, buffered_storage_->batch_bits_);
  auto record_cb1 = [callback](LogRecord& record) -> bool {
    return callback->DeliverRecord(record);
  };
  auto gap_cb1 = [callback](const GapRecord& batch_gap) -> bool {
    return callback->DeliverGap(batch_gap);
  };

  std::unique_ptr<AsyncLogReader> underlying_reader;
  {  // Create a new reader.
    std::vector<AsyncLogReader*> raw_reader;
    st = buffered_storage_->storage_->CreateAsyncReaders(
        1, std::move(record_cb1), std::move(gap_cb1), &raw_reader);
    if (!st.ok()) {
      return st;
    }
    assert(raw_reader.size() == 1);
    underlying_reader.reset(raw_reader.front());
  }

  // Compute sequence numbers which the underlying reader operates on.
  start_point >>= buffered_storage_->batch_bits_;
  if (end_point != SequencePoint::kEndOfTimeSeqno) {
    end_point = (end_point >> buffered_storage_->batch_bits_) + 1;
  }

  // Start reading.
  st = underlying_reader->Open(log_id, start_point, end_point);
  if (!st.ok()) {
    return st;
  }

  // Keep a reader so that it can be closed.
  underlying_readers_.emplace(log_id, std::move(underlying_reader));
  return Status::OK();
}

Status BufferedAsyncLogReader::Close(LogID log_id) {
  thread_check_.Check();

  // Find the reader, do nothing if it doesn't exist.
  auto it = underlying_readers_.find(log_id);
  if (it == underlying_readers_.end()) {
    return Status::OK();
  }

  auto st = it->second->Close(log_id);
  if (!st.ok()) {
    return st;
  }

  underlying_readers_.erase(it);
  return Status::OK();
}

}  // namespace rocketspeed
