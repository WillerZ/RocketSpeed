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

# Subscribe to a single topic from 80K clients
# then publish 1 message to that topic and wait to read all messages
# from all the clients.
cmd="$BENCHMARK --messages 1 --subscribe-rate 1000000 --rate 150000 --max-inflight 7000 --topics 1 --size 100 --remote --deploy --start-servers --stop-servers --progress_period=10000 --subscription-backlog-distribution=fixed --client-threads=10000 --remote-bench 8 --max_file_descriptors=1000000 --namespaceid_dynamic --towers=1 --cockpits=1 readwrite"
echo $cmd
eval $cmd
