#!/bin/bash

# Tests are described here
# https://our.intern.facebook.com/intern/wiki/Core_Data/RocketSpeed/Benchmarking/

#Aim: To measure the rate of subscription and unsubscription requests that can be sustained
#Step 5.1: Publish 1 message each to 10M topics.
#Step 5.2: Create a total of 10M subscription requests (1 subscriptions per topic) starting at the only message in this topic. Read the message received via each subscription.
#Result: Report the rate ( subscriptions/second) and the total elapsed time needed to complete Step 5.2

base=`dirname $0`
BENCHMARK=$base/benchmark.sh

if [ ! -f $BENCHMARK ]; then
  echo "Unable to find benchmark script at $BENCHMARK
  echo "cd to fbcode and then run benchmark
  exit 1
fi

# Produce a data set with 1 message each on a 10 M topics
cmd="$BENCHMARK --messages 10000000 --progress_period=10000 --rate 150000 --max-inflight 7000 --topics 10000000 --size 100 --num_messages_per_topic=10000000  --topics_distribution=fixed --remote --deploy --start-servers --stop-servers --cockpits=4 --cache-size 100000000000 --towers=1  --remote-bench 1 produce"
echo $cmd
eval $cmd

# Subscribe to each of those topics and read one message from each of those topics
cmd="$BENCHMARK --messages 10000000 --progress_period=10000 --subscribe-rate 10000 --max-inflight 7000 --topics 10000000 --remote --deploy --start-servers --stop-servers --cockpits=4 --cache-size 100000000000 --towers=1 --subscription-backlog-distribution=fixed --remote-bench 1 --producer=false consume"
echo $cmd
eval $cmd
