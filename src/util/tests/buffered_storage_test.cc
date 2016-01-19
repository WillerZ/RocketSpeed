// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#include <chrono>

#include "include/Status.h"
#include "include/Types.h"
#include "include/Env.h"
#include "src/util/common/coding.h"
#include "src/util/buffered_storage.h"
#include "src/util/memory.h"
#include "src/util/storage.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"
#include "src/test/test_cluster.h"

namespace rocketspeed {

class BufferedLogStorageTest {
 public:
  BufferedLogStorageTest()
  : env(Env::Default())
  , log_range(1, 1000) {
    ASSERT_OK(test::CreateLogger(env, "BufferedLogStorageTest", &info_log));
  }

 protected:
  Env* const env;
  std::shared_ptr<rocketspeed::Logger> info_log;
  std::pair<LogID, LogID> log_range;
};

TEST(BufferedLogStorageTest, Creation) {
  MsgLoop loop(env, EnvOptions(), -1, 4, info_log, "loop");
  ASSERT_OK(loop.Initialize());

  std::unique_ptr<TestStorage> underlying_storage =
    LocalTestCluster::CreateStorage(env, info_log, log_range);
  ASSERT_TRUE(underlying_storage);

  LogStorage* storage;
  Status st =
    BufferedLogStorage::Create(env,
                               info_log,
                               underlying_storage->GetLogStorage(),
                               &loop,
                               128,
                               4096,
                               std::chrono::microseconds(10000),
                               &storage);
  std::unique_ptr<LogStorage> owned_storage(storage);
  ASSERT_OK(st);
  ASSERT_TRUE(owned_storage);

  // Must live shorter than storage.
  MsgLoopThread t1(env, &loop, "loop");
  ASSERT_OK(loop.WaitUntilRunning());
}

class VerifyingReader : public AsyncLogReader {
 public:
  VerifyingReader(std::function<bool(LogRecord&)> record_cb,
                  std::function<bool(const GapRecord&)> gap_cb)
  : record_cb_(std::move(record_cb))
  , gap_cb_(std::move(gap_cb))
  , open_log_(0) {}

  Status Open(LogID log_id,
              SequenceNumber start_point,
              SequenceNumber end_point) override {
    ASSERT_TRUE(open_log_ == 0);
    open_log_ = log_id;
    next_seqno_ = start_point;
    return Status::OK();
  }

  Status Close(LogID log_id) override {
    ASSERT_TRUE(open_log_ == log_id);
    open_log_ = 0;
    return Status::OK();
  }

  bool DeliverRecord(LogID log_id, std::string data) {
    ASSERT_TRUE(open_log_ == log_id);
    std::unique_ptr<std::string> context(new std::string(std::move(data)));
    Slice payload(*context);
    LogRecord record(log_id,
                     payload,
                     next_seqno_,
                     std::chrono::microseconds(next_seqno_),
                     EraseType(std::move(context)));
    bool result = record_cb_(record);
    if (result) {
      ++next_seqno_;
    }
    return result;
  }

  bool DeliverGap(LogID log_id, GapType type, size_t num_seqnos) {
    ASSERT_TRUE(open_log_ == log_id);
    ASSERT_GE(num_seqnos, 1);
    bool result =
        gap_cb_({type, log_id, next_seqno_, next_seqno_ + num_seqnos - 1});
    if (result) {
      next_seqno_ += num_seqnos;
    }
    return result;
  }

 private:
  const std::function<bool(LogRecord&)> record_cb_;
  const std::function<bool(const GapRecord&)> gap_cb_;

  LogID open_log_;
  SequenceNumber next_seqno_;
};

class VerifyingStorage : public LogStorage {
 public:
  explicit VerifyingStorage(std::function<void(LogID, Slice)> on_append)
  : on_append_(std::move(on_append)), verify_reader_(nullptr) {
  }

  virtual ~VerifyingStorage() {}

  virtual Status AppendAsync(LogID id,
                             const Slice& data,
                             AppendCallback callback) {
    on_append_(id, data);
    std::lock_guard<std::mutex> lock(mutex_);
    callback(Status::OK(), seqnos_[id]++);
    return Status::OK();
  }

  virtual Status FindTimeAsync(
    LogID id,
    std::chrono::milliseconds timestamp,
    std::function<void(Status, SequenceNumber)> callback) {
    return Status::NotSupported("");
  }

  virtual Status CreateAsyncReaders(
      unsigned int parallelism,
      std::function<bool(LogRecord&)> record_cb,
      std::function<bool(const GapRecord&)> gap_cb,
      std::vector<AsyncLogReader*>* readers) {
    ASSERT_TRUE(!verify_reader_);
    ASSERT_EQ(1, parallelism);
    verify_reader_ = new VerifyingReader(record_cb, gap_cb);
    readers->push_back(verify_reader_);
    return Status::OK();
  }

  virtual bool CanSubscribePastEnd() const { return true; }

  VerifyingReader* GetReader() { return verify_reader_; }

 private:
  std::function<void(LogID, Slice)> on_append_;
  VerifyingReader* verify_reader_;
  std::mutex mutex_;
  std::unordered_map<LogID, SequenceNumber> seqnos_;
};

TEST(BufferedLogStorageTest, SingleBatching) {
  MsgLoop loop(env, EnvOptions(), -1, 4, info_log, "loop");
  ASSERT_OK(loop.Initialize());

  std::vector<LogID> logs { 123, 123, 456 };
  std::vector<std::string> msgs = { "foo", "bar", "baz" };
  size_t received = 0;

  port::Semaphore sem;
  auto verify_storage = std::make_shared<VerifyingStorage>(
    [&] (LogID log_id, Slice slice) {
      ASSERT_EQ(log_id, logs[received]);
      uint8_t batch_size;
      ASSERT_TRUE(GetFixed8(&slice, &batch_size));
      ASSERT_EQ(batch_size, 1);
      Slice data;
      ASSERT_TRUE(GetLengthPrefixedSlice(&slice, &data));
      ASSERT_TRUE(data.ToString() == msgs[received]);
      ++received;
      sem.Post();
    });

  // Buffered storage that writes 1 per batch.
  LogStorage* storage;
  Status st =
    BufferedLogStorage::Create(env,
                               info_log,
                               verify_storage,
                               &loop,
                               8,
                               1,
                               std::chrono::microseconds(1),
                               &storage);
  std::unique_ptr<LogStorage> owned_storage(storage);
  ASSERT_OK(st);
  ASSERT_TRUE(owned_storage);

  // Must live shorter than storage.
  MsgLoopThread t1(env, &loop, "loop");
  ASSERT_OK(loop.WaitUntilRunning());

  ASSERT_OK(storage->AppendAsync(123, "foo",
    [] (Status status, SequenceNumber seqno) {
      ASSERT_OK(status);
      ASSERT_EQ(seqno, 0);
    }));
  sem.TimedWait(std::chrono::seconds(1));

  ASSERT_OK(storage->AppendAsync(123, "bar",
    [] (Status status, SequenceNumber seqno) {
      ASSERT_OK(status);
      ASSERT_EQ(seqno, 8);
    }));
  sem.TimedWait(std::chrono::seconds(1));

  ASSERT_OK(storage->AppendAsync(456, "baz",
    [] (Status status, SequenceNumber seqno) {
      ASSERT_OK(status);
      ASSERT_EQ(seqno, 0);
    }));
  sem.TimedWait(std::chrono::seconds(1));
}

TEST(BufferedLogStorageTest, CountLimitedBatch) {
  MsgLoop loop(env, EnvOptions(), -1, 4, info_log, "loop");
  ASSERT_OK(loop.Initialize());

  const uint8_t kBatchSize = 16;
  const size_t kNumMessages = 1600;
  const LogID kLogID = 123;
  size_t received = 0;
  port::Semaphore sem;
  auto verify_storage = std::make_shared<VerifyingStorage>(
    [&] (LogID log_id, Slice slice) {
      ASSERT_EQ(log_id, kLogID);
      uint8_t batch_size;
      ASSERT_TRUE(GetFixed8(&slice, &batch_size));
      ASSERT_EQ(batch_size, kBatchSize);
      for (int i = 0; i < batch_size; ++i) {
        Slice data;
        ASSERT_TRUE(GetLengthPrefixedSlice(&slice, &data));
        ASSERT_EQ(data.ToString(), std::to_string(received));
        ++received;
        sem.Post();
      }
    });

  // Buffered storage that writes 10 elements per batch.
  LogStorage* storage;
  Status st =
    BufferedLogStorage::Create(env,
                               info_log,
                               verify_storage,
                               &loop,
                               kBatchSize,
                               std::numeric_limits<size_t>::max(),
                               std::chrono::microseconds(1000000),
                               &storage);
  std::unique_ptr<LogStorage> owned_storage(storage);
  ASSERT_OK(st);
  ASSERT_TRUE(owned_storage);

  // Must live shorter than storage.
  MsgLoopThread t1(env, &loop, "loop");
  ASSERT_OK(loop.WaitUntilRunning());

  std::string msgs[kNumMessages];  // to keep slice memory around
  for (size_t i = 0; i < kNumMessages; ++i) {
    msgs[i] = std::to_string(i);
    ASSERT_OK(storage->AppendAsync(kLogID, msgs[i],
      [i] (Status status, SequenceNumber seqno) {
        ASSERT_OK(status);
        ASSERT_EQ(seqno, i);  // we fill entire batches, so seqno == #sent
      }));
  }

  // Wait for all to be received.
  for (size_t i = 0; i < kNumMessages; ++i) {
    ASSERT_TRUE(sem.TimedWait(std::chrono::seconds(1)));
  }
}

TEST(BufferedLogStorageTest, ByteLimitedBatches) {
  MsgLoop loop(env, EnvOptions(), -1, 4, info_log, "loop");
  ASSERT_OK(loop.Initialize());

  const size_t kByteLimit = 100;
  const size_t kNumMessages = 1000;
  const LogID kLogID = 456;
  size_t received = 0;
  port::Semaphore sem;
  auto verify_storage = std::make_shared<VerifyingStorage>(
    [&] (LogID log_id, Slice slice) {
      ASSERT_EQ(log_id, kLogID);
      uint8_t batch_size;
      ASSERT_TRUE(GetFixed8(&slice, &batch_size));
      size_t bytes = 0;
      for (int i = 0; i < batch_size; ++i) {
        ASSERT_LE(bytes, kByteLimit);
        Slice data;
        ASSERT_TRUE(GetLengthPrefixedSlice(&slice, &data));
        ASSERT_EQ(data.ToString(), std::to_string(received));
        ++received;
        sem.Post();
        bytes += data.size();
      }
      if (received < kNumMessages) {
        // Last one might not hit limit.
        ASSERT_GE(bytes, kByteLimit);
      }
    });

  // Buffered storage that writes 10 elements per batch.
  LogStorage* storage;
  Status st =
    BufferedLogStorage::Create(env,
                               info_log,
                               verify_storage,
                               &loop,
                               255,
                               kByteLimit,
                               std::chrono::microseconds(100000),
                               &storage);
  std::unique_ptr<LogStorage> owned_storage(storage);
  ASSERT_OK(st);
  ASSERT_TRUE(owned_storage);

  // Must live shorter than storage.
  MsgLoopThread t1(env, &loop, "loop");
  ASSERT_OK(loop.WaitUntilRunning());

  std::string msgs[kNumMessages];  // to keep slice memory around
  for (size_t i = 0; i < kNumMessages; ++i) {
    msgs[i] = std::to_string(i);
    ASSERT_OK(storage->AppendAsync(kLogID, msgs[i],
      [i] (Status status, SequenceNumber seqno) {
        ASSERT_OK(status);
      }));
  }

  // Wait for all to be received.
  for (size_t i = 0; i < kNumMessages; ++i) {
    ASSERT_TRUE(sem.TimedWait(std::chrono::seconds(1)));
  }
}

TEST(BufferedLogStorageTest, LatencyLimitedBatches) {
  MsgLoop loop(env, EnvOptions(), -1, 4, info_log, "loop");
  ASSERT_OK(loop.Initialize());

  const std::chrono::microseconds time_limit(10000);
  const std::chrono::microseconds between_messages(3000);
  const uint8_t min_batch = 3;
  const uint8_t max_batch = 4;
  const size_t kNumMessages = 100;
  const LogID kLogID = 789;
  size_t received = 0;
  port::Semaphore sem;
  auto verify_storage = std::make_shared<VerifyingStorage>(
    [&] (LogID log_id, Slice slice) {
      ASSERT_EQ(log_id, kLogID);
      uint8_t batch_size;
      ASSERT_TRUE(GetFixed8(&slice, &batch_size));
      if (received + batch_size != kNumMessages) {
        // Due to latency limit, batch size should be constant.
        // (except last batch).
        ASSERT_GE(batch_size, min_batch);
        ASSERT_LE(batch_size, max_batch);
      }
      for (int i = 0; i < batch_size; ++i) {
        Slice data;
        ASSERT_TRUE(GetLengthPrefixedSlice(&slice, &data));
        ASSERT_EQ(data.ToString(), std::to_string(received));
        ++received;
        sem.Post();
      }
    });

  // Buffered storage that writes 10 elements per batch.
  LogStorage* storage;
  Status st =
    BufferedLogStorage::Create(env,
                               info_log,
                               verify_storage,
                               &loop,
                               255,
                               std::numeric_limits<size_t>::max(),
                               time_limit,
                               &storage);
  std::unique_ptr<LogStorage> owned_storage(storage);
  ASSERT_OK(st);
  ASSERT_TRUE(owned_storage);

  // Must live shorter than storage.
  MsgLoopThread t1(env, &loop, "loop");
  ASSERT_OK(loop.WaitUntilRunning());

  std::string msgs[kNumMessages];  // to keep slice memory around
  for (size_t i = 0; i < kNumMessages; ++i) {
    msgs[i] = std::to_string(i);
    ASSERT_OK(storage->AppendAsync(kLogID, msgs[i],
      [i] (Status status, SequenceNumber seqno) {
        ASSERT_OK(status);
      }));
    /* sleep override */
    std::this_thread::sleep_for(between_messages);
  }

  // Wait for all to be received.
  for (size_t i = 0; i < kNumMessages; ++i) {
    ASSERT_TRUE(sem.TimedWait(std::chrono::seconds(1)));
  }
}

TEST(BufferedLogStorageTest, AsyncReader) {
  MsgLoop loop(env, EnvOptions(), -1, 4, info_log, "loop");
  ASSERT_OK(loop.Initialize());

  auto verify_storage =
      std::make_shared<VerifyingStorage>([&](LogID log_id, Slice slice) {});

  const LogID log_id = 123;
  const size_t max_batch_entries = 4;

  LogStorage* storage;
  Status st = BufferedLogStorage::Create(env,
                                         info_log,
                                         verify_storage,
                                         &loop,
                                         max_batch_entries,
                                         std::numeric_limits<size_t>::max(),
                                         std::chrono::seconds(1),
                                         &storage);
  std::unique_ptr<LogStorage> owned_storage(storage);
  ASSERT_OK(st);
  ASSERT_TRUE(owned_storage);

  // Verification of the data seen after decoding the batch.
  SequenceNumber next_seqno = max_batch_entries;
  int countdown_to_fail = -1;
  auto record_cb = [&](LogRecord& record) -> bool {
    if (countdown_to_fail-- == 0) {
      LOG_INFO(info_log,
               "Requesting backoff on record (%" PRIu64 ")",
               record.seqno);
      return false;
    }
    ASSERT_EQ(log_id, record.log_id);
    ASSERT_EQ(next_seqno, record.seqno);
    ++next_seqno;
    LOG_INFO(info_log, "Received record (%" PRIu64 ")", record.seqno);
    return true;
  };
  auto gap_cb = [&](const GapRecord& gap) -> bool {
    if (countdown_to_fail-- == 0) {
      LOG_INFO(info_log,
               "Requesting backoff on gap (%" PRIu64 ", %" PRIu64 ")",
               gap.from,
               gap.to);
      return false;
    }
    ASSERT_EQ(log_id, gap.log_id);
    ASSERT_TRUE(GapType::kBenign == gap.type);
    ASSERT_EQ(next_seqno, gap.from);
    next_seqno = gap.to + 1;
    LOG_INFO(
        info_log, "Received gap (%" PRIu64 ", %" PRIu64 ")", gap.from, gap.to);
    return true;
  };

  std::unique_ptr<AsyncLogReader> buffered_reader;
  {
    std::vector<AsyncLogReader*> readers;
    ASSERT_OK(storage->CreateAsyncReaders(1, record_cb, gap_cb, &readers));
    buffered_reader.reset(readers.front());
  }

  // Start from not aligned point, will be rewound back to aligned position.
  buffered_reader->Open(log_id,
                        next_seqno + max_batch_entries / 2,
                        SequencePoint::kEndOfTimeSeqno);
  // We can only capture pointer to the underlying reader after opening the
  // batched reader for some log.
  auto verify_reader = verify_storage->GetReader();

  // Helper to create batches filled to arbitrary level.
  auto create_batch = [&](uint8_t batch_enrties) -> std::string {
    std::string batch;
    PutFixed8(&batch, batch_enrties);
    for (uint8_t i = 0; i < batch_enrties; ++i) {
      PutLengthPrefixedSlice(&batch, "data");
    }
    return batch;
  };
  // Helper to create an empty LogRecord in a readable way.
  auto create_empty = []() -> std::string { return ""; };

  // Deliver a record with only one element,
  ASSERT_TRUE(verify_reader->DeliverRecord(log_id, create_batch(1)));
  // and a full one,
  ASSERT_TRUE(
      verify_reader->DeliverRecord(log_id, create_batch(max_batch_entries)));
  // and a wide gap,
  ASSERT_TRUE(verify_reader->DeliverGap(log_id, GapType::kBenign, 3));
  // and a nearly full record.
  ASSERT_TRUE(verify_reader->DeliverRecord(
      log_id, create_batch(max_batch_entries - 1)));

  // Fail on the next message,
  countdown_to_fail = 0;
  ASSERT_TRUE(!verify_reader->DeliverGap(log_id, GapType::kBenign, 1));
  // retry,
  ASSERT_TRUE(verify_reader->DeliverGap(log_id, GapType::kBenign, 1));
  // fail while processing the batch,
  countdown_to_fail = 2;
  ASSERT_TRUE(!verify_reader->DeliverRecord(log_id, create_batch(3)));
  // keep failing for a while,
  countdown_to_fail = 0;
  ASSERT_TRUE(!verify_reader->DeliverRecord(log_id, create_empty()));
  countdown_to_fail = 0;
  ASSERT_TRUE(!verify_reader->DeliverRecord(log_id, create_empty()));
  // eventually succeed pushing everything through.
  ASSERT_TRUE(verify_reader->DeliverRecord(log_id, create_empty()));
  // Push an additional gap, that'll fail,
  countdown_to_fail = 0;
  ASSERT_TRUE(!verify_reader->DeliverGap(log_id, GapType::kBenign, 3));
  // and succeed right after.
  ASSERT_TRUE(verify_reader->DeliverGap(log_id, GapType::kBenign, 3));
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
