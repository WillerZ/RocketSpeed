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
cockpit_host=''
controltower_host=''
pilot_port=''
copilot_port=''
controltower_port=''
log_dir=''
collect_logs=''

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
             -l size:,client-threads:,deploy,start-servers,stop-servers,collect-logs,messages:,rate:,remote,topics:,pilots:,copilots:,towers:,pilot-port:,copilot-port:,controltower-port:,cockpit-host:,controltower-host:,remote-path:,log-dir:, \
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
    -e | --start-servers )
      start_servers='true'; remote='true'; shift ;;
    -l | --collect-logs )
      collect_logs='true'; remote='true'; shift ;;
    -n | --messages )
      num_messages="$2"; shift 2 ;;
    -q | --stop-servers )
      stop_servers='true'; remote='true'; shift ;;
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
    --pilot-port )
      pilot_port="$2"; shift 2 ;;
    --copilot-port )
      copilot_port="$2"; shift 2 ;;
    --controltower-port )
      controltower_port="$2"; shift 2 ;;
    --cockpit-host )
      cockpit_host="$2"; shift 2 ;;
    --controltower-host )
      controltower_host="$2"; shift 2 ;;
    --remote-path )
      remote_path="$2"; shift 2 ;;
    --log-dir )
      log_dir="$2"; shift 2 ;;
    -- )
      shift; break ;;
    * )
      exit 1 ;;
  esac
done

available_hosts=( rocketspeed001.11.lla1.facebook.com \
                  rocketspeed002.11.lla1.facebook.com \
                  rocketspeed003.11.lla1.facebook.com \
                  rocketspeed004.11.lla1.facebook.com \
                  rocketspeed005.11.lla1.facebook.com \
                  rocketspeed006.11.lla1.facebook.com \
                  rocketspeed007.11.lla1.facebook.com )

pilots=("${available_hosts[@]::num_pilots}")
available_hosts=("${available_hosts[@]:num_pilots}")  # pop num_pilots off

copilots=("${available_hosts[@]::num_copilots}")
available_hosts=("${available_hosts[@]:num_copilots}")  # pop num_copilots off

control_towers=("${available_hosts[@]::num_towers}")
available_hosts=("${available_hosts[@]:num_towers}")  # pop num_towers off

# Override default machines with specific ones
if [ $cockpit_host ]; then
  cockpits=( $cockpit_host )
  pilots=( $cockpit_host )
  copilots=( $cockpit_host )
else
  cockpits=("${pilots[@]} ${copilots[@]}")
fi
if [ $controltower_host ]; then
  control_towers=( $controltower_host )
fi
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
      echo "Stopping server:" $host
      ssh root@$host 'pkill -f ${remote_path}/rocketspeed'
    done
  fi
}

function start_servers {
  if [ $remote ]; then
    echo "Starting remote servers..."
    for host in ${cockpits[@]}; do
      cmd="${remote_path}/rocketspeed \
        --pilot \
        --copilot \
        --control_towers=$towers_csv \
        --storage_url=$storage_url \
        --logdevice_cluster=$logdevice_cluster "
      if [ $pilot_port ]; then
        cmd="${cmd} --pilot_port=$pilot_port"
      fi
      if [ $copilot_port ]; then
        cmd="${cmd} --copilot_port=$copilot_port"
      fi
      if [ $log_dir ]; then
        cmd="${cmd} --rs_log_dir=$log_dir"
      fi
      cmd="${cmd} 2>&1 | sed 's/^/${host}: /'"
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
        --logdevice_cluster=$logdevice_cluster "
      if [ $controltower_port ]; then
        cmd="${cmd} --tower_port=$controltower_port"
      fi
      if [ $log_dir ]; then
        cmd="${cmd} --rs_log_dir=$log_dir"
      fi
      cmd="${cmd} 2>&1 | sed 's/^/${host}: /'"
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

function deploy_servers {
  # Setup remote server
  # Check the binary is built
  if [ ! -f $server ]; then
    echo "Must have: "
    echo "  $server"
    echo "from current directory"
    exit 1
  fi

  # Deploy to remote hosts
  echo "Deploying $server to ${remote_path}..."

  for host in ${all_hosts[@]}; do
    echo "$host"
    if ! scp $server root@$host:${remote_path}; then
      echo "Error deploying to $host"
      exit 1
    fi
  done
}

function collect_logs {
  if [ $remote ]; then
    echo "Merging remote logs into LOG.remote..."
    rm -f LOG.remote
    rm -f LOG.tmp
    touch LOG.tmp
    for host in ${all_hosts[@]}; do
      cmd="cat ${log_dir}/LOG"
      # merge remote LOG file into LOG.remote
      sort -m <(ssh root@$host "${cmd}") LOG.tmp > LOG.remote
      cmd="rm -f ${log_dir}/LOG*"
      ssh root@$host "${cmd}"  # tidy up
      cp LOG.remote LOG.tmp
    done
    rm LOG.tmp
  fi
}

# Have we processed anything yet
progress='false'

# Do the server deployment if specified.
if [ $deploy ]; then
  # Deploy new binaries to remote servers
  deploy_servers
  progress='true'
fi

# Start servers if specified
if [ $start_servers ]; then
  start_servers
  progress='true'
fi

if [ $collect_logs ]; then
  progress='true'
fi
if [ $stop_servers ]; then
  progress='true'
fi

if [ $# -ne 1 ]; then
 if [ $progress == "false" ]; then
  echo "./benchmark.sh [-bcdnrstxyz] [produce|readwrite|consume]"
  echo
  echo "-b --size            Message size (bytes)."
  echo "-c --client_threads  Number of client threads."
  echo "-d --deploy          Deploy the rocketspeed binary to remote servers."
  echo "-e --start-servers   Start the rocketspeed binary on remote servers."
  echo "-l --collect-logs    Collect all logs from remote servers."
  echo "-n --num_messages    Number of messages to send."
  echo "-q --stop-servers    Stop the rocketspeed binary on remote servers."
  echo "-r --rate            Messages to send per second."
  echo "-s --remote          Use remote server(s) for pilot, copilot, control towers."
  echo "-t --topics          Number of topics."
  echo "-x --pilots          Number of pilots to use."
  echo "-y --copilots        Number of copilots to use."
  echo "-z --towers          Number of control towers to use."
  echo "--pilot-port         The port number for the pilot."
  echo "--copilot-port       The port number for the copilot."
  echo "--controltower-port  The port number for the control tower"
  echo "--cockpit-host       The machine name that runs the pilot and the copilot."
  echo "--controltower-host  The machine name that runs the control tower."
  echo "--remote-path        The directory where the rocketspeed binary is installed."
  echo "--log-dir            The directory for server logs."
  exit 1
 fi
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
if [ $pilot_port ]; then
  const_params+=" --pilot_port=$pilot_port"
fi
if [ $copilot_port ]; then
  const_params+=" --copilot_port=$copilot_port"
fi

function run_produce {
  echo "Burst writing $num_messages messages into log storage..."
  cmd="$bench $const_params \
       --start_producer=true \
       --start_consumer=false \
       --delay_subscribe=false \
       2>&1 | tee $output_dir/benchmark_produce.log"
  echo $cmd | tee $output_dir/benchmark_produce.log
  eval $cmd
  echo
}

function run_readwrite {
  echo "Writing and reading $num_messages simultaneously..."
  cmd="$bench $const_params \
       --start_producer=true \
       --start_consumer=true \
       --delay_subscribe=false \
       2>&1 | tee $output_dir/benchmark_readwrite.log"
  echo $cmd | tee $output_dir/benchmark_readwrite.log
  eval $cmd
  echo
}

function run_consume {
  echo "Reading a backlog of $num_messages..."
  cmd="$bench $const_params \
       --start_producer=true \
       --start_consumer=true \
       --delay_subscribe=true \
       2>&1 | tee $output_dir/benchmark_consume.log"
  echo $cmd | tee $output_dir/benchmark_consume.log
  eval $cmd
  echo
}

function now() {
  echo `date +"%s"`
}

report="$output_dir/report.txt"


# Run!!!
IFS=',' read -a jobs <<< $1

# special syntax to find size of array
numjobs=${#jobs[@]}
if [ "$numjobs" -ne "0" ]; then
  echo "===== Benchmark ====="
fi

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

# Stop servers if specified
if [ $stop_servers ]; then
  stop_servers
fi

# Collect all logs
if [ $collect_logs ]; then
  collect_logs
fi
