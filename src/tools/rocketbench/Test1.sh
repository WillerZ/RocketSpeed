#!/bin/bash

# Tests are described here
# https://our.intern.facebook.com/intern/wiki/Core_Data/RocketSpeed/Benchmarking/

# Test 1: Publishing Throughput for a single topic
# Aim: To measure the number of messages that can be published per second to a single topic. The Publish call is supposed to 'reliably' store the message in a store.
# Steps : Publish 1B messages to a single topic. One can use as many Publishers as needed to achieve the highest throughput.
# Result: Report the rate (messages/second) achieved and the total elapsed time for the Steps above.

base=`dirname $0`
BENCHMARK=$base/benchmark.sh

if [ ! -f $BENCHMARK ]; then
  echo "Unable to find benchmark script at $BENCHMARK
  echo "cd to fbcode and then run benchmark
  exit 1
fi

echo Starting Servers...
cmd="$BENCHMARK --remote --deploy --start-servers --cache-size 1000000000 --buffered_storage_max_latency_us=1000 --cockpits=1"
eval $cmd

cmd="$BENCHMARK --messages 1000000000 --rate 500000 --max-inflight 7000 --topics 1 --size 100 --progress_period=10000 --cockpits=1 --remote-bench 1 produce"
echo $cmd
eval $cmd

echo Stopping Servers...
cmd="$BENCHMARK --remote --stop-servers --cockpits=1"
eval $cmd
