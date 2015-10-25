//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "src/controltower/data_cache.h"
#include "src/test/test_cluster.h"
#include "src/util/testharness.h"

#include <random>

namespace rocketspeed {

class DataCacheTest {
};

// Check that data_cache is sane
TEST(DataCacheTest, Check) {
  size_t cache_size = 10 * 1024 * 1024; // 10 MB
  bool cache_data_from_system_namespaces = false;
  int bloom_bits_per_msg = 10;

  // create a cache
  DataCache cache(cache_size, cache_data_from_system_namespaces,
                  bloom_bits_per_msg);
  ASSERT_EQ(cache.GetCapacity(), cache_size);
  ASSERT_EQ(cache.GetUsage(), 0);

  std::string namespaceid("dhruba");
  Slice ns(namespaceid);
  int num_logs = 3;
  int num_topics = 10;
  size_t num_messages = 10000;

  // generate a set of logids
  std::vector<LogID> logids;
  for (int i = 0; i < num_logs; i++) {
    logids.push_back(std::rand());
  }

  // generate a set of topics
  std::vector<std::string> topics;
  for (int i = 0; i < num_topics; i++) {
    topics.push_back("topic" + std::to_string(std::rand()));
  }

  // generate a set of data
  std::vector<std::string> payloads;
  for (size_t i = 0; i < num_messages; i++) {
    payloads.push_back(std::to_string(std::rand()));
  }

  // generate a lookaside database
  std::vector<unique_ptr<MessageData>> database;

  // insert data into cache
  for (size_t i = 0; i < num_messages; i++) {
    Slice topicname(topics[i % num_topics]);
    LogID logid = logids[i % num_logs];
    Slice payload(payloads[i % num_messages]);

    // create random data message
    std::unique_ptr<MessageData> data(new MessageData(
                            MessageType::mDeliver,
                            Tenant::GuestTenant,
                            topicname,
                            ns,
                            payload));
    data->SetSequenceNumbers(i, i+1);

    // Make a copy of this message
    Message* msg = Message::Copy(*data).release();
    MessageData* mdata = static_cast<MessageData*>(msg);
    std::unique_ptr<MessageData> replica(mdata);

    // Insert copied message into cache
    cache.StoreData(ns,
                    topicname,
                    logid,
                    std::move(replica));

    // Insert message into lookaside array
    database.push_back(std::move(data));
  }
  ASSERT_GE(cache.stats_.bloom_inserts->Get(), 0);

  // loop though all the elements in the database and
  // verify that it exists in the cache
  for (size_t i = 0; i < database.size(); i++) {
    LogID logid = logids[i % num_logs];
    Slice topicname = database[i]->GetTopicName();
    SequenceNumber seqno = database[i]->GetSequenceNumber();
    Slice payload = database[i]->GetPayload();

    int count = 0;
    auto visit =
      [&] (MessageData* data_raw, bool* processed) {
           if (data_raw->GetSequenceNumber() == seqno) {
             ASSERT_EQ(topicname.compare(data_raw->GetTopicName()), 0);
             ASSERT_EQ(payload.compare(data_raw->GetPayload()), 0);
             count++;
           }
           *processed = true;
           return true;
    };
    cache.VisitCache(logid, seqno, topicname, visit);
    ASSERT_EQ(count, 1);
  }
  // all messages found in cache
  ASSERT_EQ(cache.stats_.cache_hits->Get(), database.size());
}

// Check that blooms are working fine
TEST(DataCacheTest, CheckBloom) {
  size_t cache_size = 50 * 1024 * 1024; // 50 MB
  bool cache_data_from_system_namespaces = false;
  int bloom_bits_per_msg = 10;

  // create a cache
  DataCache cache(cache_size, cache_data_from_system_namespaces,
                  bloom_bits_per_msg);
  ASSERT_EQ(cache.GetCapacity(), cache_size);
  ASSERT_EQ(cache.GetUsage(), 0);

  size_t block_size = cache.GetBlockSize();
  ASSERT_GT(block_size, 0);

  std::string namespaceid("dhruba");
  Slice ns(namespaceid);
  int num_logs = 1;
  int num_topics = 2;
  size_t num_blocks = 100;
  size_t num_messages = num_blocks * block_size;

  // we want to produce more messages that a single block size
  // so that all messages in a single block may not contain all
  // the topics

  // generate a set of logids
  std::vector<LogID> logids;
  for (int i = 0; i < num_logs; i++) {
    logids.push_back(std::rand());
  }

  // generate a set of topics
  std::vector<std::string> topics;
  for (int i = 0; i < num_topics; i++) {
    topics.push_back("topic" + std::to_string(std::rand()));
  }

  // generate a set of data
  std::vector<std::string> payloads;
  for (size_t i = 0; i < num_messages; i++) {
    payloads.push_back(std::to_string(std::rand()));
  }

  // generate a lookaside database
  std::vector<unique_ptr<MessageData>> database;

  // insert data into cache
  for (size_t i = 0; i < num_messages; i++) {
    // Generate two-blocks worth of contiguous messages that have
    // the same topicname
    size_t topicindex = i / (2 * block_size);
    Slice topicname(topics[topicindex % num_topics]);
    LogID logid = logids[i % num_logs];
    Slice payload(payloads[i % num_messages]);

    // create random data message
    std::unique_ptr<MessageData> data(new MessageData(
                            MessageType::mDeliver,
                            Tenant::GuestTenant,
                            topicname,
                            ns,
                            payload));
    data->SetSequenceNumbers(i, i+1);

    // Make a copy of this message
    Message* msg = Message::Copy(*data).release();
    MessageData* mdata = static_cast<MessageData*>(msg);
    std::unique_ptr<MessageData> replica(mdata);

    // Insert copied message into cache
    cache.StoreData(ns,
                    topicname,
                    logid,
                    std::move(replica));

    // Insert message into lookaside array
    database.push_back(std::move(data));
  }
  // assert that all full blocks have blooms.
  ASSERT_EQ(cache.stats_.bloom_inserts->Get() + 1, num_blocks);

  // loop though all the elements in the database and
  // verify that it exists in the cache
  for (unsigned int i = 0; i < database.size(); i++) {
    LogID logid = logids[i % num_logs];
    Slice topicname = database[i]->GetTopicName();
    SequenceNumber seqno = database[i]->GetSequenceNumber();
    Slice payload = database[i]->GetPayload();

    int count = 0;
    auto visit =
      [&] (MessageData* data_raw, bool* processed) {
           if (data_raw->GetSequenceNumber() == seqno) {
             ASSERT_EQ(topicname.compare(data_raw->GetTopicName()), 0);
             ASSERT_EQ(payload.compare(data_raw->GetPayload()), 0);
             count++;
           }
           *processed = true;
           return true;
    };
    cache.VisitCache(logid, seqno, topicname, visit);
    ASSERT_EQ(count, 1);
  }
  // All messages found in cache.
  ASSERT_GE(cache.stats_.cache_hits->Get(), database.size());

  // Some bloom hits were made
  ASSERT_GT(cache.stats_.bloom_hits->Get(), 0);

  // No blooms were false positive
  ASSERT_EQ(cache.stats_.bloom_falsepositives->Get(), 0);
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
