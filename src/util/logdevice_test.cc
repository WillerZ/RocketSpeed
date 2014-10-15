// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <algorithm>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include "src/util/logdevice.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

namespace rocketspeed {

class LogDeviceStorageTest { };

std::shared_ptr<facebook::logdevice::Client> MakeTestClient() {
  std::unique_ptr<facebook::logdevice::ClientSettings> settings(
    facebook::logdevice::ClientSettings::create());
  return facebook::logdevice::Client::create(
    "",
    "",
    "",
    std::chrono::milliseconds(1000),
    std::move(settings));
}

/*TEST(LogDeviceStorageTest, AppendingAndReading) {
  LogDeviceStorage* storage;
  LogDeviceStorage::Create(MakeTestClient(), Env::Default(), &storage);

  ASSERT_TRUE(storage->Append(1001, "Rocket").ok());
  ASSERT_TRUE(storage->Append(1002, "Test 1").ok());
  ASSERT_TRUE(storage->Append(1001, "Speed").ok());

  std::vector<AsyncLogReader*> readers;
  ASSERT_TRUE(storage->CreateAsyncReaders(2, &readers).ok());
  ASSERT_EQ(readers.size(), 2);
  ASSERT_NE(readers[0], static_cast<LogReader*>(nullptr));
  ASSERT_NE(readers[1], static_cast<LogReader*>(nullptr));

  ASSERT_TRUE(readers[0]->Open(1001).ok());
  ASSERT_TRUE(readers[1]->Open(1002).ok());
  std::this_thread::sleep_for(std::chrono::seconds(1));

  std::vector<LogRecord> records;
  ASSERT_TRUE(readers[0]->Read(&records, 10).ok());
  ASSERT_EQ(records.size(), 2);
  ASSERT_EQ(records[0].log_id, 1001);
  ASSERT_EQ(records[0].payload.compare("Rocket"), 0);
  ASSERT_EQ(records[1].log_id, 1001);
  ASSERT_EQ(records[1].payload.compare("Speed"), 0);
  ASSERT_GT(records[1].seqno, records[0].seqno);
  ASSERT_TRUE(records[1].timestamp >= records[0].timestamp);
  auto speedSeqno = records[1].seqno;

  ASSERT_TRUE(readers[1]->Read(&records, 10).ok());
  ASSERT_EQ(records.size(), 1);
  ASSERT_EQ(records[0].log_id, 1002);
  ASSERT_EQ(records[0].payload.compare("Test 1"), 0);
  auto test1Seqno = records[0].seqno;

  // Should be no records left now
  ASSERT_TRUE(readers[0]->Read(&records, 10).ok());
  ASSERT_EQ(records.size(), 0);
  ASSERT_TRUE(readers[1]->Read(&records, 10).ok());
  ASSERT_EQ(records.size(), 0);

  // Write some more
  ASSERT_TRUE(storage->Append(1002, "Test 2").ok());
  ASSERT_TRUE(storage->Append(1002, "Test 3").ok());

  // Now also subscribe readers[1] to log 42, starting at "Speed"
  ASSERT_TRUE(readers[1]->Open(1001, speedSeqno).ok());
  std::this_thread::sleep_for(std::chrono::seconds(1));

  ASSERT_TRUE(readers[1]->Read(&records, 10).ok());
  ASSERT_EQ(records.size(), 3);

  // readers[1] should now read "Speed", "Test 2", and "Test 3", but undefined
  // order, so we'll sort by payload so they appear in that order.
  std::sort(records.begin(), records.end(),
    [](const LogRecord& lhs, const LogRecord& rhs) {
      return lhs.payload.compare(rhs.payload) < 0;
    });
  ASSERT_EQ(records[0].log_id, 1001);
  ASSERT_EQ(records[0].payload.compare("Speed"), 0);
  ASSERT_EQ(records[1].log_id, 1002);
  ASSERT_EQ(records[1].payload.compare("Test 2"), 0);
  ASSERT_EQ(records[2].log_id, 1002);
  ASSERT_EQ(records[2].payload.compare("Test 3"), 0);
  auto test2Seqno = records[1].seqno;

  // Now subscribe readers[0] to log 100, from Test 1 to Test 2.
  ASSERT_TRUE(readers[0]->Open(1002, test1Seqno, test2Seqno).ok());
  std::this_thread::sleep_for(std::chrono::seconds(1));

  ASSERT_TRUE(readers[0]->Read(&records, 1).ok());
  ASSERT_EQ(records.size(), 1);
  ASSERT_EQ(records[0].log_id, 1002);
  ASSERT_EQ(records[0].payload.compare("Test 1"), 0);
  ASSERT_TRUE(readers[0]->Read(&records, 10).ok());
  ASSERT_EQ(records.size(), 1);
  ASSERT_EQ(records[0].log_id, 1002);
  ASSERT_EQ(records[0].payload.compare("Test 2"), 0);
}

TEST(LogDeviceStorageTest, SelectorBasic) {
  LogDeviceStorage* storage;
  LogDeviceStorage::Create(MakeTestClient(), Env::Default(), &storage);

  std::vector<LogReader*> readers;
  const int numLogs = 10;
  storage->CreateReaders(numLogs, &readers);

  LogDeviceSelector selector;
  for (int i = 0; i < numLogs; ++i) {
    ASSERT_TRUE(selector.Register(readers[i]).ok());
    ASSERT_TRUE(readers[i]->Open(2000 + i).ok());
  }

  // Write the ints 0 to 1000 inclusive to random logs 0-10
  const int numValues = 1000;
  std::uniform_int_distribution<> rng(0, numLogs - 1);
  std::default_random_engine rd;
  for (int i = 0; i <= numValues; ++i) {
    Slice data = Slice(reinterpret_cast<const char*>(&i), sizeof(i));
    ASSERT_TRUE(storage->Append(2000 + rng(rd), data).ok());
  }

  std::vector<LogReader*> selected;
  std::vector<LogRecord> records;
  int sum = 0;
  while (selector.Select(&selected, std::chrono::seconds(1)).ok()) {
    for (auto reader : selected) {
      ASSERT_TRUE(reader->Read(&records, 1).ok());
      ASSERT_EQ(records.size(), 1);  // should always read exactly 1
      int value = *reinterpret_cast<const int*>(records[0].payload.data());
      sum += value;
    }
  }
  ASSERT_EQ(sum, numValues * (numValues + 1) / 2);
}

TEST(LogDeviceStorageTest, SelectorParallel) {
  LogDeviceStorage* storage;
  LogDeviceStorage::Create(MakeTestClient(), Env::Default(), &storage);

  std::vector<LogReader*> readers;
  const int numLogs = 10;
  storage->CreateReaders(numLogs, &readers);

  // Create selector, register all readers, and open the readers on one log each
  LogDeviceSelector selector;
  for (int i = 0; i < numLogs; ++i) {
    ASSERT_TRUE(selector.Register(readers[i]).ok());
    ASSERT_TRUE(readers[i]->Open(3000 + i).ok());
  }

  // Start the sum selector thread
  // This thread will read integers using the selector, and sum them up
  // Will finish summing after timing out for 1 second
  std::future<uint64_t> sum = std::async(std::launch::async, [&selector]() {
    const auto timeout = std::chrono::seconds(1);
    std::vector<LogReader*> selected;
    std::vector<LogRecord> records;
    uint64_t sum = 0;
    while (selector.Select(&selected, timeout).ok()) {
      for (auto reader : selected) {
        reader->Read(&records, 1);
        int value = *reinterpret_cast<const int*>(records[0].payload.data());
        sum += value;
      }
    }
    return sum;
  });

  // Writer thread function. Writes consecutive integers to a random log.
  auto writer = [&storage](int from, int to) {
    std::uniform_int_distribution<> rng(0, numLogs - 1);
    std::default_random_engine rd;
    for (int i = from; i <= to; ++i) {
      Slice data = Slice(reinterpret_cast<const char*>(&i), sizeof(i));
      ASSERT_TRUE(storage->Append(3000 + rng(rd), data).ok());
      if (i % 1000 == 999) {
        // Sleep a little every 1000 iterations.
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }
  };

  // Spin up 10 threads to write numValues/10 integers each in parallel.
  const uint64_t numValues = 200000;
  std::vector<std::thread> writerThreads;
  writerThreads.reserve(10);
  for (int i = 0; i < 10; ++i) {
    writerThreads.emplace_back(writer,
                               i * numValues / 10 + 1,
                               (i + 1) * numValues / 10);
  }

  std::this_thread::sleep_for(std::chrono::seconds(1));
  ASSERT_EQ(sum.get(), numValues * (numValues + 1) / 2);

  // Make sure all threads finish as they reference storage->
  for (auto& t : writerThreads) {
    t.join();
  }
}

TEST(LogDeviceStorageTest, SelectCloseRead) {
  LogDeviceStorage* storage;
  LogDeviceStorage::Create(MakeTestClient(), Env::Default(), &storage);

  std::vector<LogReader*> readers;
  storage->CreateReaders(1, &readers);
  readers[0]->Open(4001);

  LogDeviceSelector selector;
  selector.Register(readers[0]);

  // Check that calling Close between a call to Select and Read will correctly
  // remove the pending record from the reader.
  storage->Append(4001, "Hello, world!");
  std::vector<LogReader*> selected;
  selector.Select(&selected, std::chrono::seconds(1));
  ASSERT_EQ(selected.size(), 1);  // readers[0] should have a record
  readers[0]->Close(4001);  // but now that record should be removed
  std::vector<LogRecord> records;
  readers[0]->Read(&records, 1);
  ASSERT_EQ(records.size(), 0);  // should be no records read
}*/

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
