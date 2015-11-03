#!/bin/bash

# Tests are described here
# https://our.intern.facebook.com/intern/wiki/Core_Data/RocketSpeed/Benchmarking/

#Aim: To measure the rate of subscription and unsubscription requests that can be sustained
#Step 5.1: Publish 1 message each to 10M topics.
#Step 5.2: Create a total of 10M subscription requests (1 subscriptions per topic) starting at the only message in this topic. Read the message received via each subscription.
#Result: Report the rate ( subscriptions/second) and the total elapsed time needed to complete Step 5.2

base=`dirname $0`
BENCHMARK=$base/benchmark.sh

# Define the number of cockpits and control towers for this test
COCKPITS=2
TOWERS=1
CACHESIZE=50000000000   # 50 GB
TOPICS=10000000         # 10 million
BENCH=1                 # 1 instance of rocketbench
RATE=500000             # max of 500K subscribe requests/sec

if [ ! -f $BENCHMARK ]; then
  echo "Unable to find benchmark script at $BENCHMARK
  echo "cd to fbcode and then run benchmark
  exit 1
fi

#
# Produce a data set with 1 message each on a 10 M topics
#
cmd="$BENCHMARK --messages $TOPICS --progress_period=10000 --rate 150000 --max-inflight 7000 --topics $TOPICS --size 100 --num_messages_per_topic=$TOPICS  --topics_distribution=fixed --namespaceid_dynamic --remote --deploy --start-servers --stop-servers --cockpits=$COCKPITS --towers=$TOWERS --remote-bench $BENCH produce"
echo $cmd
eval $cmd

# Subscribe to each of those topics and read one message from each of those
# topics. 
#
cmd="$BENCHMARK --messages $TOPICS --progress_period=10000 --subscribe-rate $RATE --max-inflight 20000 --namespaceid_dynamic --topics $TOPICS --remote --deploy --start-servers --stop-servers --cockpits=$COCKPITS --cache-size $CACHESIZE --towers=$TOWERS --subscription-backlog-distribution=fixed --remote-bench $BENCH --client-threads=80 --producer=false --collect-stats consume"
echo $cmd
eval $cmd
