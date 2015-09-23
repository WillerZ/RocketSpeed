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

if [ ! -f $BENCHMARK ]; then
  echo "Unable to find benchmark script at $BENCHMARK
  echo "cd to fbcode and then run benchmark
  exit 1
fi

echo Starting Servers...
cmd="$BENCHMARK --remote --deploy --start-servers --cache-size 1000000000 --cockpits=5"
eval $cmd

cmd="$BENCHMARK --messages 250000000 --rate 300000 --max-inflight 7000 --topics 1000000 --size 100 --cockpits=5 --remote-bench 4 produce"

echo $cmd
eval $cmd

echo Stopping Servers...
cmd="$BENCHMARK --remote --stop-servers --cockpits=5"
eval $cmd
