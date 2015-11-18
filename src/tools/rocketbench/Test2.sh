#!/bin/bash

# Tests are described here
# https://our.intern.facebook.com/intern/wiki/Core_Data/RocketSpeed/Benchmarking/

#Test 2: Publishing Throughput for multiple topics
#Aim: To measure the number of messages that can be published per second to multiple topics.
#Parameters: R = 0, T = 1 billion
#Steps: Publish a total of 1B messages to 10M topics.
#Result: Report the rate (messages/second) and total elapsed time needed to complete the above Steps. Report the p99.9 latency of each publish.

base=`dirname $0`
BENCHMARK=$base/benchmark.sh

COCKPITS=${COCKPITS:-5}           # Number of pilot/copilot servers
TOWERS=${TOWERS:-1}               # Number of control tower servers
BENCH=${BENCH:-16}                # Number of rocketbench instances
NUMMSG=${NUMMSG:-62500000}        # Messages to publish per rocketbench instance
NUMTOPICS=${NUMTOPICS:-30000000}  # Number of topics to publish to

if [ ! -f $BENCHMARK ]; then
  echo "Unable to find benchmark script at $BENCHMARK"
  echo "cd to fbcode and then run benchmark"
  exit 1
fi

cleanup() {
  # Stop servers
  echo "Stopping servers......"
  cmd="$BENCHMARK --remote --stop-servers --cockpits=$COCKPITS"
  echo $cmd
  eval $cmd
}

trap cleanup 0

echo Starting Servers...
cmd="$BENCHMARK --remote --deploy --start-servers --buffered_storage_max_latency_us=10000 --cockpits=$COCKPITS"
eval $cmd || { echo "Failed to start servers"; exit 1; }

cmd="$BENCHMARK --messages $NUMMSG --rate 500000 --max-inflight 7000 --topics $NUMTOPICS --size 100 --progress_period=10000 --cockpits=$COCKPITS --remote-bench $BENCH produce"
echo $cmd
eval $cmd || { echo "Failed to write all messages"; exit 1; }

echo Stopping Servers...
cmd="$BENCHMARK --remote --stop-servers --cockpits=$COCKPITS"
eval $cmd
