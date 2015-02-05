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
num_pilots=1
num_copilots=1
num_towers=1
storage_url=${STORAGE_URL:-\"configerator:logdevice/rocketspeed.logdevice.primary.conf\"}
logdevice_cluster=${LOGDEVICE_CLUSTER:-\"rocketspeed.logdevice.primary\"}
remote_path="/usr/local/bin"

# Use the 2 lower order bytes from the UID to generate a namespace id.
# The hope is that this will be sufficiently unique so that concurrent
# runs of this benchmark do not pollute one another.
namespaceid=`id -u`

# If you want to use the debug build, then set an environment varibale
# called DBG=dbg. By default, pick the optimized build.
part=${DBG:-opt}

server=${SERVER:-_build/$part/rocketspeed/github/src/server/rocketspeed}

# Argument parsing
OPTS=`getopt -o b:c:dn:r:st:x:y:z: \
             -l size:,client-threads:,deploy,messages:,rate:,remote,topics:,pilots:,copilots:,towers: \
             -n 'rocketbench' -- "$@"`

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

eval set -- "$OPTS"

while true; do
  case "$1" in
    -b | --size )
      message_size="$2"; shift 2 ;;
    -c | --client-threads )
      client_workers="$2"; shift 2 ;;
    -d | --deploy )
      deploy='true'; shift ;;
    -n | --messages )
      num_messages="$2"; shift 2 ;;
    -r | --rate )
      message_rate="$2"; shift 2 ;;
    -s | --remote )
      remote='true'; shift ;;
    -t | --topics )
      num_topics="$2"; shift 2 ;;
    -x | --pilots )
      num_pilots="$2"; shift 2 ;;
    -y | --copilots )
      num_copilots="$2"; shift 2 ;;
    -z | --towers )
      num_towers="$2"; shift 2 ;;
    -- )
      shift; break ;;
    * )
      exit 1 ;;
  esac
done

available_hosts=( rocketspeed001.11.lla1 \
                  rocketspeed002.11.lla1 \
                  rocketspeed003.11.lla1 \
                  rocketspeed004.11.lla1 \
                  rocketspeed005.11.lla1 \
                  rocketspeed006.11.lla1 \
                  rocketspeed007.11.lla1 )

pilots=("${available_hosts[@]::num_pilots}")
available_hosts=("${available_hosts[@]:num_pilots}")  # pop num_pilots off

copilots=("${available_hosts[@]::num_copilots}")
available_hosts=("${available_hosts[@]:num_copilots}")  # pop num_copilots off

control_towers=("${available_hosts[@]::num_towers}")
available_hosts=("${available_hosts[@]:num_towers}")  # pop num_towers off

cockpits=("${pilots[@]} ${copilots[@]}")
all_hosts=("${cockpits[@]} ${control_towers[@]}")

function join {
  # joins elements of an array with $1
  local IFS="$1"; shift; echo "$*";
}

pilots_csv=$(join , ${pilots[@]})
copilots_csv=$(join , ${copilots[@]})
towers_csv=$(join , ${control_towers[@]})

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
  if [ ! -f $server ]; then
    echo "Must have: "
    echo "  $server"
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
  echo "./benchmark.sh [-bcdnrstxyz] [produce|readwrite|consume]"
  echo
  echo "-b --size            Message size (bytes)."
  echo "-c --client_threads  Number of client threads."
  echo "-d --deploy          Deploy the rocketspeed binary to remote servers."
  echo "-n --num_messages    Number of messages to send."
  echo "-r --rate            Messages to send per second."
  echo "-s --remote          Use remote server(s) for pilot, copilot, control towers."
  echo "-t --topics          Number of topics."
  echo "-x --pilots          Number of pilots to use."
  echo "-y --copilots        Number of copilots to use."
  echo "-z --towers          Number of control towers to use."
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
  const_params+=" --start_local_server=false"
  const_params+=" --pilot_hostnames=$pilots_csv"
  const_params+=" --copilot_hostnames=$copilots_csv"
else
  const_params+=" --start_local_server=true"
fi

function start_servers {
  if [ $remote ]; then
    echo "Starting remote servers..."
    for host in ${cockpits[@]}; do
      cmd="${remote_path}/rocketspeed \
        --pilot \
        --copilot \
        --control_towers=$towers_csv \
        --storage_url=$storage_url \
        --logdevice_cluster=$logdevice_cluster \
        2>&1 | sed 's/^/${host}: /'"
      echo "$host: $cmd"
      if ! ssh -f root@$host -- "${cmd}"; then
        echo "Failed to start rocketspeed on $host."
        exit 1
      fi
    done
    for host in ${control_towers[@]}; do
      cmd="${remote_path}/rocketspeed \
        --tower \
        --storage_url=$storage_url \
        --logdevice_cluster=$logdevice_cluster \
        2>&1 | sed 's/^/${host}: /'"
      echo "$host: $cmd"
      if ! ssh -f root@$host -- "${cmd}"; then
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
      ssh root@$host 'rm -f LOG*'  # tidy up
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
