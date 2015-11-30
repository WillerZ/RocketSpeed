#!/bin/bash

# Tests are described here
# https://our.intern.facebook.com/intern/wiki/Core_Data/RocketSpeed/Benchmarking/

base=`dirname $0`
BENCHMARK=$base/benchmark.sh

COCKPITS=${COCKPITS:-2}       # Number of pilot/copilots servers
TOWERS=${TOWERS:-3}           # Number of control tower servers
NUMMSG=${NUMMSG:-250000000}   # Message to publish per rocketbench instance
NUMTOPICS=${NUMTOPICS:-7500}  # Topics to publish to per rocketbench instance
BENCH=${BENCH:-4}             # Number of rocketbench instances

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

# Produce a data set with 1 billion messages spread across 30K topics
cmd="$BENCHMARK --collect-stats --progress_per_line --messages $NUMMSG --rate 150000 --max-inflight 7000 --topics $NUMTOPICS --size 100 --remote --deploy --start-servers --stop-servers --progress_period=10000 --buffered_storage_max_latency_us=0 --cockpits=$COCKPITS --towers=$TOWERS --namespaceid_dynamic --remote-bench $BENCH produce"
echo $cmd
eval $cmd || { echo "Produce stage failed"; exit 1; }

# Start servers with a 50 GB cache on each control tower.
echo "Starting server......"
cmd="$BENCHMARK --remote --deploy --start-servers --cache-size 50000000000 --socket-buffer-size=157286400 --cockpits=$COCKPITS --towers=$TOWERS "
echo $cmd
eval $cmd || { echo "Failed to start servers"; exit 1; }

# Subscribe with a backlog, do not produce any new data. Fill up the cache.
# Use a fixed distribution and read in all topics.
echo "Filling cache...."
cmd="$BENCHMARK --collect-stats --progress_per_line --messages $NUMMSG --subscribe-rate 10000 --max-inflight 7000 --topics $NUMTOPICS --size 100 --remote --progress_period=10000 --socket-buffer-size=157286400 --cockpits=$COCKPITS --towers=$TOWERS --namespaceid_dynamic --subscription-backlog-distribution=fixed --remote-bench $BENCH --producer=false consume"
echo $cmd
eval $cmd || { echo "Failed to fill up cache"; exit 1; }

# Subscribe again with a backlog, Use data from cache.
# Use a uniform distribution for selectively picking data within a topic.
# Selectively pick only 50% subset of all topics.
echo "Measurement started...."
cmd="$BENCHMARK --collect-stats --progress_per_line --messages $NUMMSG --subscribe-rate 10000 --max-inflight 7000 --topics $NUMTOPICS --size 100 --remote --progress_period=10000 --socket-buffer-size=157286400 --cockpits=$COCKPITS --towers=$TOWERS --namespaceid_dynamic --subscription-backlog-distribution=uniform --subscription-topic-ratio 2 --remote-bench $BENCH --producer=false consume"
echo $cmd
eval $cmd || { echo "Failed to read backlog"; exit 1; }

# Stop servers explicitly (even though the bash-trap stops them too).
echo Stopping Servers...
cmd="$BENCHMARK --remote --stop-servers --cockpits=$COCKPITS --towers=$TOWERS --remote-bench $BENCH"
eval $cmd

