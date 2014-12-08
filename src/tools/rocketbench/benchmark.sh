#!/bin/bash

# size constants
K=1024
M=$((1024 * K))
G=$((1024 * M))

message_size=100
num_messages=$((1 * M))
message_rate=$((50 * K))
remote=''
deploy=''
client_workers=32
num_topics=1000000
storage_url=${STORAGE_URL:-\"configerator:logdevice/rocketspeed.logdevice.primary.conf\"}
cockpits=( rocketspeed001.11.lla1 rocketspeed002.11.lla1 rocketspeed003.11.lla1 )
control_towers=( rocketspeed004.11.lla1 rocketspeed005.11.lla1 ) #rocketspeed006.11.lla1 rocketspeed007.11.lla1 )
all_hosts=( "${cockpits[@]}" "${control_towers[@]}" )
remote_path="/usr/local/bin"

function join {
  # joins elements of an array with $1
  local IFS="$1"; shift; echo "$*";
}

# Comma separate control_towers
towers_csv=$(join , ${control_towers[@]})

# Use the 2 lower order bytes from the UID to generate a namespace id.
# The hope is that this will be sufficiently unique so that concurrent
# runs of this benchmark do not pollute one another.
namespaceid=`id -u`

# If you want to use the debug build, then set an environment varibale
# called DBG=dbg. By default, pick the optimized build.
part=${DBG:-opt}

# Parse flags
while getopts ':b:c:dn:r:st:' flag; do
  case "${flag}" in
    b) message_size=${OPTARG} ;;
    c) client_workers=${OPTARG} ;;
    d) deploy='true' ;;
    n) num_messages=${OPTARG} ;;
    r) message_rate=${OPTARG} ;;
    s) remote='true' ;;
    t) num_topics=${OPTARG} ;;
    *) echo "Unexpected option ${OPTARG}"; exit 1 ;;
  esac
done

# Shift remaining arguments into place
shift $((OPTIND-1))

function stop_servers {
  if [ $remote ]; then
    echo "Stopping remote servers..."
    for host in ${all_hosts[@]}; do
      ssh root@$host 'pkill -f rocketspeed'
    done
  fi
}

# Make sure there are no old servers running before we start.
stop_servers

# Do the server deployment if specified.
if [ $deploy ]; then
  # Setup remote server
  # Check the binary is built
  if [ -f _build/$part/rocketspeed/github/src/tools/rocketbench/rocketbench ]; then
    server=_build/$part/rocketspeed/github/src/server/rocketspeed
  else
    echo "Must have: "
    echo "  _build/$part/rocketspeed/github/src/server/rocketspeed"
    echo "from current directory"
    exit 1
  fi

  # Deploy to remote hosts
  echo "Deploying $server to $remote_path..."

  for host in ${all_hosts[@]}; do
    echo "$host"
    if ! scp $server root@$host:$remote_path; then
      echo "Error deploying to $host"
      exit 1
    fi
  done
fi

if [ $# -ne 1 ]; then
  echo "./benchmark.sh [-bcdnrst] [produce|readwrite|consume]"
  echo
  echo "-b  Message size (bytes)."
  echo "-c  Number of client threads."
  echo "-d  Deploy the rocketspeed binary to remote servers."
  echo "-n  Number of messages to send."
  echo "-r  Messages to send per second."
  echo "-s  Use remote server(s) for pilot, copilot, control towers."
  echo "-t  Number of topics."
  exit 1
fi

output_dir=${OUTPUT_DIR:-/tmp}
if [ ! -d $output_dir ]; then
  mkdir -p $output_dir
fi

const_params="
  --storage_url=$storage_url \
  --namespaceid=$namespaceid \
  --message_size=$message_size \
  --num_messages=$num_messages \
  --message_rate=$message_rate \
  --client_workers=$client_workers \
  --num_topics=$num_topics"

if [ -f rocketbench ]; then
  bench=./rocketbench
elif [ -f _build/$part/rocketspeed/github/src/tools/rocketbench/rocketbench ]; then
  bench=_build/$part/rocketspeed/github/src/tools/rocketbench/rocketbench
else
  echo "Must have either: "
  echo "  rocketbench, or"
  echo "  _build/$part/rocketspeed/github/src/tools/rocketbench/rocketbench"
  echo "from current directory"
  exit 1
fi

# Setup const params
if [ $remote ]; then
  pilot_host=${cockpits[0]}
  copilot_host=${cockpits[1]}
  const_params+=" --start_local_server=false"
  const_params+=" --pilot_hostname=$pilot_host"
  const_params+=" --copilot_hostname=$copilot_host"
else
  const_params+=" --start_local_server=true"
fi

function start_servers {
  if [ $remote ]; then
    echo "Starting remote servers..."
    for host in ${cockpits[@]}; do
      cmd="rocketspeed --pilot --copilot --control_towers=$towers_csv"
      echo "$host: $cmd"
      if ! eval "ssh -f root@$host '${remote_path}/${cmd}'"; then
        echo "Failed to start rocketspeed on $host."
        exit 1
      fi
    done
    for host in ${control_towers[@]}; do
      cmd="rocketspeed --tower"
      echo "$host: $cmd"
      if ! eval "ssh -f root@$host '${remote_path}/${cmd}'"; then
        echo "Failed to start rocketspeed on $host."
        exit 1
      fi
    done
    sleep 3  # give servers time to start
    echo
  fi
}

function collect_logs {
  if [ $remote ]; then
    echo "Merging remote logs into LOG.remote..."
    rm -f LOG.remote
    rm -f LOG.tmp
    touch LOG.tmp
    for host in ${all_hosts[@]}; do
      # merge remote LOG file into LOG.remote
      sort -m <(ssh root@$host 'cat LOG') LOG.tmp > LOG.remote
      cp LOG.remote LOG.tmp
    done
    rm LOG.tmp
  fi
}

function run_produce {
  start_servers
  echo "Burst writing $num_messages messages into log storage..."
  cmd="$bench $const_params \
       --start_producer=true \
       --start_consumer=false \
       --delay_subscribe=false \
       2>&1 | tee $output_dir/benchmark_produce.log"
  echo $cmd | tee $output_dir/benchmark_produce.log
  eval $cmd
  echo
  collect_logs
  stop_servers
}

function run_readwrite {
  start_servers
  echo "Writing and reading $num_messages simultaneously..."
  cmd="$bench $const_params \
       --start_producer=true \
       --start_consumer=true \
       --delay_subscribe=false \
       2>&1 | tee $output_dir/benchmark_readwrite.log"
  echo $cmd | tee $output_dir/benchmark_readwrite.log
  eval $cmd
  echo
  collect_logs
  stop_servers
}

function run_consume {
  start_servers
  echo "Reading a backlog of $num_messages..."
  cmd="$bench $const_params \
       --start_producer=true \
       --start_consumer=true \
       --delay_subscribe=true \
       2>&1 | tee $output_dir/benchmark_consume.log"
  echo $cmd | tee $output_dir/benchmark_consume.log
  eval $cmd
  echo
  collect_logs
  stop_servers
}

function now() {
  echo `date +"%s"`
}

report="$output_dir/report.txt"

# print start time
echo "===== Benchmark ====="

# Run!!!
IFS=',' read -a jobs <<< $1
for job in ${jobs[@]}; do
  echo "Start $job at `date`" | tee -a $report
  echo
  start=$(now)
  if [ $job = produce ]; then
    run_produce
  elif [ $job = readwrite ]; then
    run_readwrite
  elif [ $job = consume ]; then
    run_consume
  else
    echo "unknown job $job"
    exit 1
  fi
  end=$(now)

  echo "Complete $job in $((end-start)) seconds" | tee -a $report
done
