//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma GCC diagnostic ignored "-Wshadow"

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include "logdevice/include/AsyncReader.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/ClientSettings.h"
#ifdef USE_LOGDEVICE
# include "logdevice/test/utils/IntegrationTestUtils.h"
#endif
#include "src/util/testharness.h"
#include "src/util/testutil.h"
#include "src/port/port.h"

namespace rocketspeed {

using facebook::logdevice::logid_t;
using facebook::logdevice::lsn_t;
using facebook::logdevice::LSN_INVALID;
using facebook::logdevice::LSN_MAX;
using facebook::logdevice::LSN_OLDEST;

class MockLogDeviceTest : public ::testing::Test : public ::testing::Test { };

facebook::logdevice::Payload payload(std::string s) {
  return facebook::logdevice::Payload(s.c_str(), s.size() + 1);
}

#ifdef USE_LOGDEVICE

std::shared_ptr<facebook::logdevice::Client> MakeTestClient() {
  // Make the cluster static so that it stays alive for the whole test.
  static auto cluster =
    facebook::logdevice::IntegrationTestUtils::ClusterFactory().create(5);
  auto client = cluster->createClient();
  lsn_t now_lsn = client->appendSync(logid_t(1), payload(""));
  ASSERT_NE(now_lsn, LSN_INVALID);
  ASSERT_EQ(client->trimSync(logid_t(1), now_lsn), 0);
  return client;
}

#else

std::shared_ptr<facebook::logdevice::Client> MakeTestClient() {
  // Clean up any existing logs to isolate the tests.
  std::unique_ptr<facebook::logdevice::ClientSettings> settings(
    facebook::logdevice::ClientSettings::create());
  return facebook::logdevice::Client::create(
    "",
    "",
    "",
    std::chrono::milliseconds(1000),
    std::move(settings));
}

#endif  // USE_LOGDEVICE

TEST_F(MockLogDeviceTest, Basic) {
  auto client = MakeTestClient();

  // Write a bunch of messages to a single log.
  logid_t logid(1);
  lsn_t lsn[6];
  ASSERT_NE(lsn[0] = client->appendSync(logid, payload("test0")), LSN_INVALID);
  ASSERT_NE(lsn[1] = client->appendSync(logid, payload("test1")), LSN_INVALID);
  ASSERT_NE(lsn[2] = client->appendSync(logid, payload("test2")), LSN_INVALID);
  ASSERT_NE(lsn[3] = client->appendSync(logid, payload("test3")), LSN_INVALID);
  ASSERT_NE(lsn[4] = client->appendSync(logid, payload("test4")), LSN_INVALID);
  ASSERT_NE(lsn[5] = client->appendSync(logid, payload("test5")), LSN_INVALID);

  // Create two readers from that log.
  auto reader1 = client->createAsyncReader();
  auto reader2 = client->createAsyncReader();

  port::Semaphore checkpoint1;
  port::Semaphore checkpoint2;

  // Use the reader callbacks to count the number of messages received
  // and verify the expected contents.
  std::atomic<int> count1{0};
  std::atomic<int> count2{0};
  reader1->setRecordCallback(
    [&] (std::unique_ptr<facebook::logdevice::DataRecord>& rec) {
      ASSERT_EQ(std::string(reinterpret_cast<const char*>(rec->payload.data)),
                "test" + std::to_string(count1));
      ++count1;
      if (count1 == 6 && count2 == 4) {
        checkpoint1.Post();
      }
      if (count1 == 8 && count2 == 4) {
        checkpoint2.Post();
      }
      return true;
    });
  reader2->setRecordCallback(
    [&] (std::unique_ptr<facebook::logdevice::DataRecord>& rec) {
      ASSERT_EQ(std::string(reinterpret_cast<const char*>(rec->payload.data)),
                "test" + std::to_string(count2 + 1));
      ++count2;
      if (count1 == 6 && count2 == 4) {
        checkpoint1.Post();
      }
      return true;
    });

  reader1->startReading(logid, lsn[0]);
  reader2->startReading(logid, lsn[1], lsn[4]);

  // Check that all messages were recevied.
  ASSERT_TRUE(checkpoint1.TimedWait(std::chrono::seconds(10)));
  ASSERT_EQ(count1, 6);
  ASSERT_EQ(count2, 4);
  // Add a couple more.
  ASSERT_NE(client->appendSync(logid, payload("test6")), LSN_INVALID);
  ASSERT_NE(client->appendSync(logid, payload("test7")), LSN_INVALID);

  // Reader 1 should have received two more messages, while reader 2
  // should receive no more because it only subscribed up to lsn[5].
  ASSERT_TRUE(checkpoint2.TimedWait(std::chrono::seconds(1)));
  ASSERT_EQ(count1, 8);
  ASSERT_EQ(count2, 4);

  // Stop reading and check that no more messages are received on reader 1.
  reader1->stopReading(logid, nullptr);
  ASSERT_NE(client->appendSync(logid, payload("test8")), LSN_INVALID);
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  ASSERT_EQ(count1, 8);
}

TEST_F(MockLogDeviceTest, FindTime) {
  auto client = MakeTestClient();

  // Write a bunch of messages to a single log.
  // Wait for 10ms in between otherwise mutliple records will have the same
  // timestamp and the test will fail.
  logid_t logid(1);
  lsn_t lsn[3];
  ASSERT_NE(lsn[0] = client->appendSync(logid, payload("test0")), LSN_INVALID);
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  ASSERT_NE(lsn[1] = client->appendSync(logid, payload("test1")), LSN_INVALID);
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  ASSERT_NE(lsn[2] = client->appendSync(logid, payload("test2")), LSN_INVALID);

  auto reader = client->createAsyncReader();

  port::Semaphore checkpoint;

  // Read all the timestamps from the records.
  std::chrono::milliseconds timestamps[3];
  std::atomic<int> count{0};
  reader->setRecordCallback(
    [&] (std::unique_ptr<facebook::logdevice::DataRecord>& rec) {
      ASSERT_LT(count, 3);
      timestamps[count] = rec->attrs.timestamp;
      ++count;
      if (count == 3) {
        checkpoint.Post();
      }
      return true;
    });
  reader->startReading(logid, lsn[0]);

  // Let the readers catch up.
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));
  ASSERT_EQ(count, 3);

  // Check that using findTimeSync on the timestamps matches up with
  // the LSNs that we know from the appendSync calls.
  for (int i = 0; i < 3; ++i) {
    facebook::logdevice::Status status;
    lsn_t result = client->findTimeSync(logid, timestamps[i], &status);
    ASSERT_EQ(result, lsn[i]);
    ASSERT_TRUE(status == facebook::logdevice::E::OK ||
                status == facebook::logdevice::E::PARTIAL);
  }

  // Check using findTime as well
  count = 0;
  for (int i = 0; i < 3; ++i) {
    client->findTime(logid, timestamps[i],
      [i, &count, &lsn, &checkpoint](facebook::logdevice::Status err, lsn_t l) {
        ASSERT_EQ(l, lsn[i]);
        ASSERT_TRUE(err == facebook::logdevice::E::OK ||
                    err == facebook::logdevice::E::PARTIAL);
        ++count;
        if (count == 3) {
          checkpoint.Post();
        }
      });
  }

  // Let the callbacks happen
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));
  ASSERT_EQ(count, 3);  // ensure they were eventually called
}

TEST_F(MockLogDeviceTest, Trim) {
  auto client = MakeTestClient();

  // Write a bunch of messages to a single log.
  // Wait for 10ms in between otherwise mutliple records will have the same
  // timestamp and the test will fail.
  logid_t logid(1);
  lsn_t lsn[4];
  ASSERT_NE(lsn[0] = client->appendSync(logid, payload("test0")), LSN_INVALID);
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  ASSERT_NE(lsn[1] = client->appendSync(logid, payload("test1")), LSN_INVALID);
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  ASSERT_NE(lsn[2] = client->appendSync(logid, payload("test2")), LSN_INVALID);
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  ASSERT_NE(lsn[3] = client->appendSync(logid, payload("test3")), LSN_INVALID);

  // Trim away the first two messages.
  client->trimSync(logid, lsn[1]);

  auto reader = client->createAsyncReader();
  port::Semaphore checkpoint;

  // Read all the timestamps from the records.
  std::atomic<int> count{0};
  reader->setRecordCallback(
    [&] (std::unique_ptr<facebook::logdevice::DataRecord>& rec) {
      ASSERT_EQ(std::string(reinterpret_cast<const char*>(rec->payload.data)),
                "test" + std::to_string(count + 2));
      ++count;
      if (count == 2) {
        checkpoint.Post();
      }
      return true;
    });
  reader->startReading(logid, lsn[0]);

  // Let the readers catch up.
  ASSERT_TRUE(checkpoint.TimedWait(std::chrono::seconds(1)));
  ASSERT_EQ(count, 2);  // should only have read the last two messages
}

TEST_F(MockLogDeviceTest, ConcurrentReadsWrites) {
  auto client = MakeTestClient();

  // Write a bunch of messages to a single log.
  logid_t logid(1);
  const int numMessages = 1000;

  port::Semaphore checkpoint1;
  port::Semaphore checkpoint2;

  // Create two readers from that log.
  std::atomic<int> count1{0};
  std::atomic<lsn_t> lsn1{LSN_OLDEST};  // last LSN read
  auto reader1 = client->createAsyncReader();
  reader1->setRecordCallback(
    [&] (std::unique_ptr<facebook::logdevice::DataRecord>& rec) {
      ASSERT_EQ(std::string(reinterpret_cast<const char*>(rec->payload.data)),
                "test" + std::to_string(count1));
      ++count1;
      lsn1 = rec->attrs.lsn;
      if (count1 == numMessages) {
        checkpoint1.Post();
      }
      return true;
    });
  reader1->startReading(logid, LSN_OLDEST, LSN_MAX);


  std::atomic<int> count2{0};
  std::atomic<lsn_t> lsn2{LSN_OLDEST};
  auto reader2 = client->createAsyncReader();
  reader2->setRecordCallback(
    [&] (std::unique_ptr<facebook::logdevice::DataRecord>& rec) {
      ASSERT_EQ(std::string(reinterpret_cast<const char*>(rec->payload.data)),
                "test" + std::to_string(count2));
      ++count2;
      lsn2 = rec->attrs.lsn;
      if (count2 == numMessages) {
        checkpoint2.Post();
      }
      return true;
    });
  reader2->startReading(logid, LSN_OLDEST, LSN_MAX);

  // Write 1000 messages to the log while occasionally waiting.
  for (int i = 0; i < numMessages; ++i) {
    ASSERT_NE(client->appendSync(logid, payload("test" + std::to_string(i))),
              LSN_INVALID);
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::microseconds(100));
    if (i % 25 == 0) {
      // Trim the log every so often to test reading while trimming also.
      client->trimSync(logid, std::min(lsn1, lsn2) - 1);
    }
  }

  // Check that all messages were recevied.
  ASSERT_TRUE(checkpoint1.TimedWait(std::chrono::seconds(1)));
  ASSERT_TRUE(checkpoint2.TimedWait(std::chrono::seconds(1)));
  ASSERT_EQ(count1, numMessages);
  ASSERT_EQ(count2, numMessages);
}

}  // namespace rocketspeed
