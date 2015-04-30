//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <chrono>
#include <memory>
#include <thread>

#include "include/Status.h"
#include "include/Types.h"
#include "src/messages/msg_loop.h"
#include "src/port/Env.h"
#include "src/client/storage/file_storage.h"
#include "src/client/topic_id.h"
#include "src/util/common/coding.h"
#include "src/util/auto_roll_logger.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

namespace rocketspeed {

class FileStorageTest {
 public:
  std::chrono::seconds timeout;
  std::string file_path;

  Env* env;
  std::shared_ptr<rocketspeed::Logger> info_log;
  std::unique_ptr<FileStorage> storage;
  std::unique_ptr<MsgLoopBase> msg_loop;

  SnapshotCallback snapshot_callback;

  port::Semaphore loaded_sem;
  port::Semaphore loaded_empty_sem;
  port::Semaphore snapshot_sem;

  std::unordered_map<TopicID, SequenceNumber> loaded_entries;
  std::mutex loaded_entries_mutex;

  FileStorageTest()
      : timeout(5)
      , file_path(test::TmpDir() + "/FileStorageTest-file_storage_data")
      , env(Env::Default()) {
    ASSERT_OK(test::CreateLogger(env, "FileStorageTest", &info_log));

    env->DeleteFile(file_path);

    SetupStorage();
  }

  ~FileStorageTest() {
    StopMsgLoop();
  }

  void SetupStorage() {
    storage.reset(new FileStorage(env, file_path, info_log));

    msg_loop.reset(new MsgLoop(env, EnvOptions(), 0, 3, info_log, "client"));

    snapshot_callback = [this](Status status) {
      ASSERT_OK(status);
      snapshot_sem.Post();
    };

    auto load_callback =
        [this](const std::vector<SubscriptionRequest>& restored) {
      if (restored.empty()) {
        loaded_empty_sem.Post();
      } else {
        std::lock_guard<std::mutex> lock(loaded_entries_mutex);
        for (const auto& request : restored) {
          LOG_INFO(info_log,
                   "Loaded request for topic (%s, %s)",
                   request.namespace_id.c_str(),
                   request.topic_name.c_str());
          if (request.start) {
            TopicID topic_id(request.namespace_id, request.topic_name);
            loaded_entries[topic_id] = request.start.get();
          }
          loaded_sem.Post();
        }
      }
    };
    storage->Initialize(load_callback, msg_loop.get());
  }

  Status StartMsgLoop() {
    Status st = msg_loop->Initialize();
    if (!st.ok()) {
      return st;
    }

    env->StartThread([this]() {
      msg_loop->Run();
    });

    return msg_loop->WaitUntilRunning();
  }

  void StopMsgLoop() {
    msg_loop.reset();
    env->WaitForJoin();
  }

  bool WaitForLoad(size_t count) {
    for (; count > 0; --count) {
      if (!loaded_sem.TimedWait(timeout)) {
        return false;
      }
    }
    return true;
  }

  bool WaitForLoadEmpty(size_t count) {
    for (; count > 0; --count) {
      if (!loaded_empty_sem.TimedWait(timeout)) {
        return false;
      }
    }
    return true;
  }
};

TEST(FileStorageTest, UpdatesAndLoads) {
  ASSERT_OK(StartMsgLoop());

  // Test scenario.
  TopicID topic1("ns1", "StoreAndLoad_1");
  TopicID topic2("ns2", "StoreAndLoad_2");

  // Generate some state.
  ASSERT_OK(storage->Update(
      SubscriptionRequest(topic1.namespace_id, topic1.topic_name, true, 100)));
  ASSERT_OK(storage->Update(
      SubscriptionRequest(topic2.namespace_id, topic2.topic_name, true, 101)));

  {  // Load one subscription from the storage.
    std::vector<SubscriptionRequest> requests = {
        SubscriptionRequest(
            topic1.namespace_id, topic1.topic_name, true, SubscriptionStart()),
    };
    ASSERT_OK(storage->Load(requests));
    ASSERT_TRUE(WaitForLoad(1));
  }

  {  // Verify state.
    std::lock_guard<std::mutex> lock(loaded_entries_mutex);
    ASSERT_EQ(1, loaded_entries.size());
    ASSERT_EQ(100, loaded_entries[topic1]);
    loaded_entries.clear();
  }

  // Load all subscriptions from the storage.
  ASSERT_OK(storage->LoadAll());
  ASSERT_TRUE(WaitForLoad(2));

  {  // Verify state.
    std::lock_guard<std::mutex> lock(loaded_entries_mutex);
    ASSERT_EQ(2, loaded_entries.size());
    ASSERT_EQ(100, loaded_entries[topic1]);
    ASSERT_EQ(101, loaded_entries[topic2]);
    loaded_entries.clear();
  }

  // Remove one subscription and bump seqno in the other.
  ASSERT_OK(storage->Update(
      SubscriptionRequest(topic1.namespace_id, topic1.topic_name, false, 0)));
  ASSERT_OK(storage->Update(
      SubscriptionRequest(topic2.namespace_id, topic2.topic_name, true, 102)));

  // Load all subscriptions.
  ASSERT_OK(storage->LoadAll());
  ASSERT_TRUE(WaitForLoad(1));

  {  // Verify state.
    std::lock_guard<std::mutex> lock(loaded_entries_mutex);
    ASSERT_EQ(1, loaded_entries.size());
    ASSERT_EQ(102, loaded_entries[topic2]);
    loaded_entries.clear();
  }
}

TEST(FileStorageTest, SnapshotAndRead) {
  ASSERT_OK(StartMsgLoop());

  // Test scenario.
  std::vector<SubscriptionRequest> requests = {
      SubscriptionRequest("ns1", "SnapshotAndRead_1", true, 101),
      SubscriptionRequest("ns1", "SnapshotAndRead_2", true, 102),
      SubscriptionRequest("ns2", "SnapshotAndRead_2", true, 103),
  };

  auto verify_state = [this, &requests]() {
    // Verify state.
    std::lock_guard<std::mutex> lock(loaded_entries_mutex);
    ASSERT_EQ(requests.size(), loaded_entries.size());
    for (const auto& request : requests) {
      TopicID topic_id(request.namespace_id, request.topic_name);
      ASSERT_EQ(request.start.get(), loaded_entries[topic_id]);
    }
    loaded_entries.clear();
  };

  // Generate some state.
  for (auto& request : requests) {
    ASSERT_OK(storage->Update(request));
  }

  // Load all subscriptions.
  ASSERT_OK(storage->LoadAll());
  ASSERT_TRUE(WaitForLoad(requests.size()));

  verify_state();

  // Persist the state.
  storage->WriteSnapshot(snapshot_callback);
  ASSERT_TRUE(snapshot_sem.TimedWait(timeout));

  // Restart the storage and retrieve state from a file.
  StopMsgLoop();
  SetupStorage();

  ASSERT_OK(storage->ReadSnapshot());

  ASSERT_OK(StartMsgLoop());

  // Load all subscriptions.
  ASSERT_OK(storage->LoadAll());
  ASSERT_TRUE(WaitForLoad(requests.size()));

  verify_state();
}

TEST(FileStorageTest, MissingSubscription) {
  ASSERT_OK(StartMsgLoop());

  // What if we ask for a nonexistent subscription?
  std::vector<SubscriptionRequest> requests = {
      SubscriptionRequest(
          "ns1", "MissingSubscription", true, SubscriptionStart()),
  };

  ASSERT_OK(storage->Load(requests));
  ASSERT_TRUE(WaitForLoad(requests.size()));

  {  // Verify state.
    std::lock_guard<std::mutex> lock(loaded_entries_mutex);
    ASSERT_EQ(0, loaded_entries.size());
  }
}

TEST(FileStorageTest, MissingFile) {
  // Make sure that there is not file.
  env->DeleteFile(file_path);

  // Try to read it.
  ASSERT_TRUE(storage->ReadSnapshot().IsIOError());
}

TEST(FileStorageTest, CorruptedFile) {
  // Create corrupted snapshot file.
  {
    std::unique_ptr<WritableFile> file_handle;
    ASSERT_OK(env->NewWritableFile(file_path, &file_handle, EnvOptions()));

    std::string buffer;

    // Some proper subscription
    PutFixed64(&buffer, 101);
    std::string topic_id;
    PutTopicID(&topic_id, "ns1", "CorruptedFile");
    PutFixed32(&buffer, static_cast<uint32_t>(topic_id.size()));
    buffer.append(topic_id);
    // But no topic name at all :(
    ASSERT_OK(file_handle->Append(Slice(buffer)));

    // And some corrupted one.
    // Sequence number.
    PutFixed64(&buffer, 101);
    // Non-zero topic ID size.
    PutFixed32(&buffer, 11);
    // But no topic ID at all :(

    ASSERT_OK(file_handle->Append(Slice(buffer)));
  }

  // Try to read it.
  ASSERT_TRUE(storage->ReadSnapshot().IsInternal());

  // Verify that nothing got added.
  ASSERT_OK(StartMsgLoop());

  ASSERT_OK(storage->LoadAll());
  ASSERT_TRUE(WaitForLoadEmpty(msg_loop->GetNumWorkers()));

  {  // Verify state
    std::lock_guard<std::mutex> lock(loaded_entries_mutex);
    ASSERT_EQ(0, loaded_entries.size());
  }
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
