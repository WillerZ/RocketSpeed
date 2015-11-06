#!/bin/bash

# Tests are described here
# https://our.intern.facebook.com/intern/wiki/Core_Data/RocketSpeed/Benchmarking/

base=`dirname $0`
BENCHMARK=$base/benchmark.sh

# Number of instances of each
COCKPITS=1
TOWERS=1
BENCH=8

# Each client receives the same published message because everybody
# uses the same topic.
CLIENTS_PER_BENCH=10000
NUM_MESSAGES_PUBLISHED=1
NUM_MESSAGES_RECEIVED=$((CLIENTS_PER_BENCH * NUM_MESSAGES_PUBLISHED))

if [ ! -f $BENCHMARK ]; then
  echo "Unable to find benchmark script at $BENCHMARK
  echo "cd to fbcode and then run benchmark
  exit 1
fi

# Subscribe to a single topic from 80K clients
# then publish 1 message to that topic and wait to read all messages
# from all the clients.
cmd="$BENCHMARK --messages $NUM_MESSAGES_PUBLISHED --num_messages_to_receive $NUM_MESSAGES_RECEIVED --subscribe-rate 1000000 --rate 150000 --max-inflight 7000 --topics 1 --size 100 --remote --deploy --start-servers --stop-servers --progress_period=10000 --subscription-backlog-distribution=fixed --client-threads=$CLIENTS_PER_BENCH --remote-bench $BENCH --max_file_descriptors=1000000 --namespaceid_dynamic --idle-timeout 10 --delay_after_subscribe_seconds=10 --max-inflight 7000 --towers=$TOWERS --cockpits=$COCKPITS readwrite"
echo $cmd
eval $cmd
