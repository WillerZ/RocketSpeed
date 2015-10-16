//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include <chrono>
#include <memory>

#include "src/logdevice/storage.h"
#include "src/test/test_cluster.h"
#include "src/util/common/random.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"
#include "src/port/port.h"

using namespace rocketspeed;

class LogDeviceStorageTest {
 public:
  // This needs to be large, see INITIAL_REDELIVERY_DELAY in
  // ClientReadStream.cpp
  std::chrono::milliseconds client_redelivery_timeout_{2000};

  LogDeviceStorageTest() : env_(Env::Default()) {
    ASSERT_OK(test::CreateLogger(env_, "LogDeviceStorageTest", &info_log));
  }

 protected:
  Env* env_;
  std::shared_ptr<rocketspeed::Logger> info_log;
};

TEST(LogDeviceStorageTest, FlowControlWithRecordStealing) {
  LocalTestCluster cluster(info_log);
  ASSERT_OK(cluster.GetStatus());

  port::Semaphore publish_sem;

  Topic topic = "LogDeviceStorageTest";
  NamespaceID namespace_id = GuestNamespace;
  std::string payload = "test_message";

  auto publish_cb = [&](std::unique_ptr<ResultStatus> rs) {
    ASSERT_OK(rs->GetStatus());

    // Find the log ID for published topic.
    LogID log_id;
    ASSERT_OK(cluster.GetLogRouter()->GetLogID(
        Slice(namespace_id), Slice(topic), &log_id));

    // No gap expected.
    auto gap_cb = [](const GapRecord& r) {
      assert(false);
      ASSERT_TRUE(false);
      return true;
    };

    const auto seqno = rs->GetSequenceNumber();
    // In read callback, we will request backoff but steal the record.
    // We expect to receive an empty record with the same LSN on the next retry.
    size_t delivery_attempt = 0;
    LogRecord stolen_record;
    port::Semaphore record_sem;
    auto record_cb = [&](LogRecord& r) mutable {
      ++delivery_attempt;
      if (delivery_attempt == 1) {
        ASSERT_TRUE(!r.payload.empty());
        ASSERT_EQ(seqno, r.seqno);
        ASSERT_EQ(log_id, r.log_id);
        // Steal the record.
        stolen_record = std::move(r);
        ASSERT_TRUE(!r.context);
        // Apply flow control.
        return false;
      } else if (delivery_attempt == 2) {
        // We should receive an empty record.
        ASSERT_TRUE(r.payload.empty());
        ASSERT_EQ(seqno, r.seqno);
        ASSERT_EQ(log_id, r.log_id);
        record_sem.Post();
        // Do not apply flow control.
        return true;
      } else {
        assert(false);
        ASSERT_TRUE(false);
        return true;
      }
    };

    // Create readers and read the appended message.
    std::vector<AsyncLogReader*> readers;
    ASSERT_OK(cluster.GetLogStorage()->CreateAsyncReaders(
        1, record_cb, gap_cb, &readers));
    ASSERT_OK(readers.front()->Open(log_id, seqno, seqno));

    // Wait for read callback to be invoked twice.
    ASSERT_TRUE(record_sem.TimedWait(client_redelivery_timeout_));

    for (auto r : readers) {
      r->Close(log_id);
      delete r;
    }

    // Everything went fine.
    publish_sem.Post();
  };

  // Create RocketSpeed client.
  ClientOptions options;
  options.config = cluster.GetConfiguration();
  options.info_log = info_log;
  std::unique_ptr<Client> client;
  ASSERT_OK(Client::Create(std::move(options), &client));

  // Send a message.
  auto ps = client->Publish(GuestTenant,
                            topic,
                            namespace_id,
                            TopicOptions(),
                            Slice(payload),
                            publish_cb);
  ASSERT_OK(ps.status);

  // Wait on publish callbuck to finish.
  ASSERT_TRUE(publish_sem.TimedWait(client_redelivery_timeout_));
}

TEST(LogDeviceStorageTest, AsyncReaderCleanup) {
  // Checks that deleting an AsyncReader with all logs closed does not
  // cause segfaults.
  LocalTestCluster cluster(info_log);
  ASSERT_OK(cluster.GetStatus());
  auto storage = cluster.GetLogStorage();

  const size_t kNumMessages = 100;
  const size_t kIterations = 10000;

  for (size_t i = 0; i < kNumMessages; ++i) {
    storage->AppendAsync(1, "data",
      [] (Status st, SequenceNumber) { ASSERT_OK(st); });
  }
  for (size_t i = 0; i < kIterations; ++i) {
    std::vector<AsyncLogReader*> readers;
    Status st = storage->CreateAsyncReaders(
      1,
      [] (LogRecord&) {
        return true;
      },
      [] (const GapRecord&) {
        return true;
      },
      &readers);
    ASSERT_OK(st);
    readers[0]->Open(1);
    /* sleep override */
    std::this_thread::sleep_for(
      std::chrono::microseconds(ThreadLocalPRNG()() % 1000));
    if (ThreadLocalPRNG()() & 1) {
      // Sometimes close the log, sometimes don't.
      readers[0]->Close(1);
    }
    delete readers[0];
  }
}

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
