// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/util/buffered_storage.h"

#include "external/folly/move_wrapper.h"
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
  // Encode into batch of 1.
  // Using unique_ptr here because string slice needs to be valid even
  // after move into append callback capture.
  std::unique_ptr<std::string> encoded(new std::string());

  // Format is 1 byte batch size followed by length-prefixed slice for each
  // entry in the batch.
  uint8_t batch_size = 1;
  PutFixed8(encoded.get(), batch_size);
  PutLengthPrefixedSlice(encoded.get(), data);

  // Capture string to ensure slice is valid throughput asynchronous append.
  Slice append_data(*encoded);
  auto context = folly::makeMoveWrapper(std::move(encoded));
  auto moved_callback = folly::makeMoveWrapper(std::move(callback));

  // Forward encoded data to underlying storage.
  return storage_->AppendAsync(
    id,
    append_data,
    [this, moved_callback, context, batch_size]
    (Status st, SequenceNumber seqno) {
      // Can acknowledge all batch entries now.
      for (uint8_t batch_index = 0; batch_index < batch_size; ++batch_index) {
        // Adjust seqno and invoke callback.
        (*moved_callback)(st, seqno << batch_bits_ | batch_index);
      }
    });
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
