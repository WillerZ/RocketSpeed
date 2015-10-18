#!/bin/bash

# Tests are described here
# https://our.intern.facebook.com/intern/wiki/Core_Data/RocketSpeed/Benchmarking/

base=`dirname $0`
BENCHMARK=$base/benchmark.sh

if [ ! -f $BENCHMARK ]; then
  echo "Unable to find benchmark script at $BENCHMARK
  echo "cd to fbcode and then run benchmark
  exit 1
fi

# Produce a data set with 1 billion messages spread across 10K topics
cmd="$BENCHMARK --messages 250000000 --rate 150000 --max-inflight 7000 --topics 10000 --size 100 --remote --deploy --start-servers --stop-servers --progress_period=10000 --buffered_storage_max_latency_us=0 --cockpits=4 --towers=1 --remote-bench 4 produce"
echo $cmd
eval $cmd

# Subscribe with a backlog, do not produce any new data
cmd="$BENCHMARK --messages 250000000 --subscribe-rate 10000 --max-inflight 7000 --topics 10000 --size 100 --remote --deploy --start-servers --stop-servers --progress_period=10000 --cache-size 1000000000 --socket-buffer-size=157286400 --cockpits=4 --towers=1 --subscription-backlog-distribution=uniform --remote-bench 4 --producer=false consume"
echo $cmd
eval $cmd
