// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#include "include/Status.h"
#include "include/Types.h"
#include "src/port/Env.h"
#include "src/util/common/coding.h"
#include "src/util/buffered_storage.h"
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
  MsgLoopThread t1(env, &loop, "loop");

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
}

class VerifyingStorage : public LogStorage {
 public:
  explicit VerifyingStorage(std::function<void(LogID, Slice)> on_append)
  : on_append_(std::move(on_append)) {
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
    return Status::NotSupported("");
  }

  virtual bool CanSubscribePastEnd() const { return true; }

 private:
  std::function<void(LogID, Slice)> on_append_;
  std::mutex mutex_;
  std::unordered_map<LogID, SequenceNumber> seqnos_;
};

TEST(BufferedLogStorageTest, SingleBatching) {
  MsgLoop loop(env, EnvOptions(), -1, 4, info_log, "loop");
  ASSERT_OK(loop.Initialize());
  MsgLoopThread t1(env, &loop, "loop");

  std::vector<LogID> logs { 123, 123, 456 };
  std::vector<std::string> msgs = { "foo", "bar", "baz" };
  size_t received = 0;

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

  ASSERT_OK(storage->AppendAsync(123, "foo",
    [] (Status status, SequenceNumber seqno) {
      ASSERT_OK(status);
      ASSERT_EQ(seqno, 0);
    }));
  ASSERT_OK(storage->AppendAsync(123, "bar",
    [] (Status status, SequenceNumber seqno) {
      ASSERT_OK(status);
      ASSERT_EQ(seqno, 8);
    }));
  ASSERT_OK(storage->AppendAsync(456, "baz",
    [] (Status status, SequenceNumber seqno) {
      ASSERT_OK(status);
      ASSERT_EQ(seqno, 0);
    }));
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
