//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include <unistd.h>
#include <chrono>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "src/controltower/tower.h"
#include "src/controltower/options.h"
#include "src/copilot/copilot.h"
#include "src/copilot/worker.h"
#include "src/copilot/options.h"
#include "src/test/test_cluster.h"
#include "src/util/testharness.h"
#include "src/datastore/datastore_impl.h"

namespace rocketspeed {

class DataStoreTest {
 public:
  // Create a new instance of the copilot
  DataStoreTest() : env_(Env::Default()) {
    // Create Logger
    ASSERT_OK(test::CreateLogger(env_, "DataStoreTest", &info_log_));
  }

  virtual ~DataStoreTest() {
    env_->WaitForJoin();  // This is good hygine
  }

 protected:
  Env* env_;
  EnvOptions env_options_;
  std::shared_ptr<Logger> info_log_;

  void WaitForData(DataStore* store,
                   const std::string& key,
                   const std::string& value) {
    port::Semaphore waiter_;
    std::string val;
    while (true) {
      Status st = store->Get(key, &val);
      if (st.IsNotFound() || (st.ok() && val != value)) {
        continue;
      }
      break;
    }
  }
};

TEST(DataStoreTest, GetPut) {
  LocalTestCluster cluster(info_log_);
  ASSERT_OK(cluster.GetStatus());

  // get the name of the machine:port where the copilot is running
  HostId copilot = cluster.GetCopilot()->GetHostId();

  // create a new database
  unique_ptr<DataStore> handle;
  std::string value;
  bool create_new = true;
  ASSERT_TRUE(DataStore::Open(copilot, create_new,
                              info_log_, &handle).ok());
  DataStoreImpl* handleimpl = static_cast<DataStoreImpl *>(handle.get());
  ASSERT_EQ(handleimpl->NumRecords(), 0);

  ASSERT_TRUE(handle->Put("k1", "v1").ok());
  WaitForData(handle.get(), "k1", "v1");
  ASSERT_TRUE(handle->Get("k1", &value).ok());
  ASSERT_EQ(value, "v1");
  ASSERT_EQ(handleimpl->NumRecords(), 1);

  ASSERT_TRUE(handle->Put("k2", "v2").ok());
  WaitForData(handle.get(), "k2", "v2");
  ASSERT_TRUE(handle->Get("k2", &value).ok());
  ASSERT_EQ(value, "v2");
  ASSERT_EQ(handleimpl->NumRecords(), 2);
  handle.release();

  // reopen the database and create new database. Older data
  // should not reapppear.
  ASSERT_TRUE(DataStore::Open(copilot, create_new,
                              info_log_, &handle).ok());
  handleimpl = static_cast<DataStoreImpl *>(handle.get());
  ASSERT_EQ(handleimpl->NumRecords(), 0);

  // check that keys starting with '_' fails
  ASSERT_TRUE(!handle->Put("_k1", "v1").ok());
}

TEST(DataStoreTest, Iteration) {
  LocalTestCluster cluster(info_log_);
  ASSERT_OK(cluster.GetStatus());

  // get the name of the machine:port where the copilot is running
  HostId copilot = cluster.GetCopilot()->GetHostId();

  // create a new database
  unique_ptr<DataStore> handle;
  std::string value;
  bool create_new = true;
  ASSERT_TRUE(DataStore::Open(copilot, create_new,
                              info_log_, &handle).ok());
  DataStoreImpl* handleimpl = static_cast<DataStoreImpl *>(handle.get());
  ASSERT_EQ(handleimpl->NumRecords(), 0);

  // insert two records into the database
  LOG_INFO(info_log_, "Inserting %s:%s", "k1", "v1");
  ASSERT_TRUE(handle->Put("k1", "v1").ok());
  LOG_INFO(info_log_, "Inserting %s:%s", "k2", "v2");
  ASSERT_TRUE(handle->Put("k2", "v2").ok());
  WaitForData(handle.get(), "k1", "v1");
  WaitForData(handle.get(), "k2", "v2");
  ASSERT_EQ(handleimpl->NumRecords(), 2);

  // seek to the start of the database
  std::unique_ptr<Iterator> it = handle->CreateNewIterator();
  ASSERT_OK(it->Seek());

  // check both values
  ASSERT_EQ(it->key(), "k1");
  ASSERT_EQ(it->value(), "v1");
  ASSERT_TRUE(it->Next().ok());
  ASSERT_EQ(it->key(), "k2");
  ASSERT_EQ(it->value(), "v2");

  // seek to the second value
  ASSERT_TRUE(it->Seek("k2").ok());
  ASSERT_EQ(it->key(), "k2");
  ASSERT_EQ(it->value(), "v2");

  // no more entries
  ASSERT_TRUE(it->Next().IsNotFound());

  // add new record
  ASSERT_TRUE(handle->Put("k3", "v3").ok());
  WaitForData(handle.get(), "k3", "v3");
  ASSERT_EQ(handleimpl->NumRecords(), 3);

  // verify that both k2 and k3 appears in the scan
  ASSERT_TRUE(it->Seek("k2").ok());
  ASSERT_EQ(it->key(), "k2");
  ASSERT_TRUE(it->Next().ok());
  ASSERT_EQ(it->key(), "k3");
  ASSERT_TRUE(it->Next().IsNotFound());

  // verify that k4 does not exist
  ASSERT_TRUE(it->Seek("k4").IsNotFound());
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
