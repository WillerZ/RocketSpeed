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
#include "src/util/common/coding.h"
#include "src/util/auto_roll_logger.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

namespace rocketspeed {

class FileStorageTest {
 public:
  FileStorageTest()
      : timeout(5),
        file_path(test::TmpDir() + "/FileStorageTest-file_storage_data"),
        env(Env::Default()) {
    ASSERT_OK(test::CreateLogger(env, "FileStorageTest", &info_log));
    // Make sure there is no stale data.
    env->DeleteFile(file_path);
  }

 protected:
  const std::chrono::seconds timeout;
  const std::string file_path;

  Env* const env;
  std::shared_ptr<rocketspeed::Logger> info_log;
};

TEST(FileStorageTest, GenericSubscriptionStorage) {
  FileStorage storage(env, info_log, file_path);

  // Sample subscription data.
  const TenantID tenant_id = Tenant::GuestTenant;
  const NamespaceID namespace_id = GuestNamespace;

  const Topic topic_name0 = "SnapshotAndRestore_0";
  const SequenceNumber seqno0 = 123;
  const Topic topic_name1 = "SnapshotAndRestore_1";
  const SequenceNumber seqno1 = 0;
  // Intentionally same as above, we might have multiple subscriptions on the
  // same topic, and storage shouldn't attempt to deduplicate them.
  const Topic topic_name2 = "SnapshotAndRestore_1";
  const SequenceNumber seqno2 = 123123;

  // Create snapshot 1 with subscriptions 0 and 1.
  std::shared_ptr<SubscriptionStorage::Snapshot> snapshot1;
  ASSERT_OK(storage.CreateSnapshot(1, &snapshot1));
  ASSERT_OK(snapshot1->Append(0, tenant_id, namespace_id, topic_name0, seqno0));
  ASSERT_OK(snapshot1->Append(0, tenant_id, namespace_id, topic_name1, seqno1));

  // Create snapshot 2 with subscriptiosn 0, 1 and 2.
  std::shared_ptr<SubscriptionStorage::Snapshot> snapshot2;
  ASSERT_OK(storage.CreateSnapshot(2, &snapshot2));
  ASSERT_OK(snapshot2->Append(0, tenant_id, namespace_id, topic_name0, seqno0));
  ASSERT_OK(snapshot2->Append(1, tenant_id, namespace_id, topic_name1, seqno1));
  ASSERT_OK(snapshot2->Append(1, tenant_id, namespace_id, topic_name2, seqno2));

  // Commit snapshot 2 first.
  ASSERT_OK(snapshot2->Commit());
  // Verify stored subscriptions.
  std::vector<SubscriptionParameters> restored;
  std::vector<SubscriptionParameters> expected = {
      SubscriptionParameters(tenant_id, namespace_id, topic_name0, seqno0),
      SubscriptionParameters(tenant_id, namespace_id, topic_name1, seqno1),
      SubscriptionParameters(tenant_id, namespace_id, topic_name2, seqno2), };
  ASSERT_OK(storage.RestoreSubscriptions(&restored));
  ASSERT_TRUE(restored == expected);

  // Finally commit snapshot 1.
  ASSERT_OK(snapshot1->Commit());
  // Verify stored subscriptions.
  expected = {
      SubscriptionParameters(tenant_id, namespace_id, topic_name0, seqno0),
      SubscriptionParameters(tenant_id, namespace_id, topic_name1, seqno1), };
  ASSERT_OK(storage.RestoreSubscriptions(&restored));
  ASSERT_TRUE(restored == expected);
}

TEST(FileStorageTest, MissingFile) {
  FileStorage storage(env, info_log, file_path);

  // Make sure that there is not file.
  env->DeleteFile(file_path);

  // Try to read it.
  std::vector<SubscriptionParameters> restored;
  ASSERT_TRUE(storage.RestoreSubscriptions(&restored).IsIOError());
}

TEST(FileStorageTest, CorruptedFile) {
  // Create corrupted snapshot file.
  {
    std::unique_ptr<WritableFile> file_handle;
    ASSERT_OK(env->NewWritableFile(file_path, &file_handle, EnvOptions()));

    std::string buffer;

    // Some proper subscription entry.
    PutFixed16(&buffer, Tenant::GuestTenant);
    PutFixed64(&buffer, 101);
    std::string topic_id;
    PutTopicID(&topic_id, GuestNamespace, "CorruptedFile");
    PutFixed32(&buffer, static_cast<uint32_t>(topic_id.size()));
    buffer.append(topic_id);
    ASSERT_OK(file_handle->Append(Slice(buffer)));

    // And some corrupted one.
    PutFixed16(&buffer, Tenant::GuestTenant);
    PutFixed64(&buffer, 101);
    // No topic ID.
    ASSERT_OK(file_handle->Append(Slice(buffer)));
  }

  // Try to read it.
  FileStorage storage(env, info_log, file_path);
  std::vector<SubscriptionParameters> restored;
  ASSERT_TRUE(storage.RestoreSubscriptions(&restored).IsIOError());
}

}  // namespace rocketspeed

int main(int argc, char** argv) { return rocketspeed::test::RunAllTests(); }
