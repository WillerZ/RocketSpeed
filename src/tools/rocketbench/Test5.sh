#!/bin/bash

# Tests are described here
# https://our.intern.facebook.com/intern/wiki/Core_Data/RocketSpeed/Benchmarking/

#Aim: To measure the rate of subscription and unsubscription requests that can be sustained
#Step 5.1: Publish 1 message each to 10M topics.
#Step 5.2: Create a total of 10M subscription requests (1 subscriptions per topic) starting at the only message in this topic. Read the message received via each subscription.
#Result: Report the rate ( subscriptions/second) and the total elapsed time needed to complete Step 5.2

base=`dirname $0`
BENCHMARK=$base/benchmark.sh

COCKPITS=${COCKPITS:-2}               # Number of pilot/copilot servers
TOWERS=${TOWERS:-1}                   # Number of control tower servers
CACHESIZE=${CACHESIZE:-50000000000}   # Control tower cache size (50 GB)
NUMTOPICS=${NUMTOPICS:-2500000}       # Number of topics to publish to
BENCH=${BENCH:-4}                     # Number of rocketbench instances
RATE=${RATE:-300000}                  # max of 300K subscribe requests/sec/bench

if [ ! -f $BENCHMARK ]; then
  echo "Unable to find benchmark script at $BENCHMARK"
  echo "cd to fbcode and then run benchmark"
  exit 1
fi

cleanup() {
  # Stop servers
  echo "Stopping servers......"
  cmd="$BENCHMARK --remote --stop-servers --cockpits=$COCKPITS --towers=$TOWERS --remote-bench $BENCH"
  echo $cmd
  eval $cmd
}

trap cleanup 0

#
# Produce a data set with 1 message each on a 10 M topics
#
cmd="$BENCHMARK --messages $NUMTOPICS --progress_period=1000 --progress_per_line --rate 150000 --max-inflight 2000 --topics $NUMTOPICS --size 100 --num_messages_per_topic=$NUMTOPICS  --topics_distribution=fixed --namespaceid_dynamic --remote --deploy --start-servers --stop-servers --cockpits=$COCKPITS --towers=$TOWERS --remote-bench $BENCH produce"
echo $cmd
eval $cmd || { echo "Failed to write all messages"; exit 1; }

# Start servers with a 50 GB cache on each control tower.
echo "Starting server..."
cmd="$BENCHMARK --remote --deploy --start-servers --cache-size $CACHESIZE --cockpits=$COCKPITS --towers=$TOWERS"
echo $cmd
eval $cmd || { echo "Failed to start servers"; exit 1; }

# Subscribe to each of those topics and read one message from each of those
# topics to fill the cache.
echo "Filling cache..."
cmd="$BENCHMARK --messages $NUMTOPICS --progress_period=1000 --progress_per_line --subscribe-rate $((RATE/2)) --max-inflight 20000 --namespaceid_dynamic --topics $NUMTOPICS --remote --cockpits=$COCKPITS --cache-size $CACHESIZE --towers=$TOWERS --subscription-backlog-distribution=fixed --remote-bench $BENCH --client-threads=80 --producer=false --collect-stats consume"
echo $cmd
eval $cmd || { echo "Failed to receive all messages"; exit 1; }

# Subscribe to each of those topics and read one message from each of those
# topics.
echo "Measuring subscription throughput..."
cmd="$BENCHMARK --messages $NUMTOPICS --progress_period=1000 --progress_per_line --subscribe-rate $RATE --max-inflight 20000 --namespaceid_dynamic --topics $NUMTOPICS --remote --cockpits=$COCKPITS --cache-size $CACHESIZE --towers=$TOWERS --subscription-backlog-distribution=fixed --remote-bench $BENCH --client-threads=80 --producer=false --collect-stats consume"
echo $cmd
eval $cmd || { echo "Failed to receive all messages"; exit 1; }

# Stop servers explicitly (even though the bash-trap stops them too).
echo Stopping Servers...
cmd="$BENCHMARK --remote --stop-servers --cockpits=$COCKPITS --towers=$TOWERS --remote-bench $BENCH"
eval $cmd
