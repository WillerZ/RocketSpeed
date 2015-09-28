#!/bin/bash

# size constants
K=1024
M=$((1024 * K))
G=$((1024 * M))

message_size=100
num_messages=1000
message_rate=100
subscribe_rate=10
max_inflight=1000
remote=''
deploy=''
client_workers=32
num_topics=100
num_pilots=1
num_copilots=1
num_towers=1
remote_path="/tmp"
cockpit_host=''
controltower_host=''
pilot_port=''
copilot_port=''
controltower_port=''
log_dir="/tmp"
collect_logs=''
collect_stats=''
strip=''
rollcall='false'  # disable rollcall for benchmarks by default
cache_size=''     # use the default set by the control tower
num_bench=''
remote_bench=''
idle_timeout="5"  # if no new messages within this time, then declare done
max_latency_micros=10000

if [ -z ${ROCKETSPEED_ARGS+x} ]; then
  logdevice_cluster=${LOGDEVICE_CLUSTER:-rocketspeed.logdevice.primary}
  storage_url="configerator:logdevice/${logdevice_cluster}.conf"
  rocketspeed_args="--storage_url=$storage_url --logdevice_cluster=$logdevice_cluster"
else
  rocketspeed_args=$ROCKETSPEED_ARGS
fi

# Use the 2 lower order bytes from the UID to generate a namespace id.
# The hope is that this will be sufficiently unique so that concurrent
# runs of this benchmark do not pollute one another.
namespaceid=`id -u`

# If you want to use the debug build, then set an environment variable
# called DBG=dbg. By default, pick the optimized build.
part=${DBG:-opt}

server=${SERVER:-_build/$part/rocketspeed/github/src/server/rocketspeed}
bench=_build/$part/rocketspeed/github/src/tools/rocketbench/rocketbench

# Argument parsing
OPTS=`getopt -o b:c:dn:r:st:x:y:z: \
             -l size:,client-threads:,deploy,start-servers,stop-servers,collect-logs,collect-stats,messages:,rate:,remote,topics:,pilots:,copilots:,towers:,pilot-port:,copilot-port:,controltower-port:,cockpit-host:,controltower-host:,remote-path:,log-dir:,strip,rollcall:,remote-bench:,subscribe-rate:,cache-size:,idle-timeout:,max-inflight:,max_latency_micros:weibull_scale:,weibull_shape:,weibull_max_time: \
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
    --collect-stats )
      collect_stats='true'; shift ;;
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
    --cache-size )
      cache_size="$2"; shift 2 ;;
    --idle-timeout )
      idle_timeout="$2"; shift 2 ;;
    --pilot-port )
      pilot_port="$2"; shift 2 ;;
    --weibull_scale )
      weibull_scale="$2"; shift 2;;
    --weibull_shape )
      weibull_shape="$2"; shift 2;;
    --weibull_max_time )
      weibull_max_time="$2"; shift 2;;
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
    --strip )
      strip='true'; shift ;;
    --rollcall )
      rollcall="$2"; shift 2 ;;
    --remote-bench )
      num_bench="$2"; remote_bench='true'; remote='true'; shift 2 ;;
    --subscribe-rate )
      subscribe_rate="$2"; shift 2 ;;
    --max-inflight )
      max_inflight="$2"; shift 2 ;;
    --max_latency_micros )
      max_latency_micros="$2"; shift 2 ;;
    -- )
      shift; break ;;
    * )
      echo "Unknown option to command"
      exit 1 ;;
  esac
done

if [ ! -f $bench ]; then
  echo "Must have: "
  echo "  $bench"
  echo "from current directory"
  exit 1
fi

# rocketspeed binaries on these hosts
#
if [ $ROCKETSPEED_HOSTS ]; then
  IFS=',' read -a available_hosts <<< "$ROCKETSPEED_HOSTS"
else
  if [ $USER == "pja" ]; then
    available_hosts=( \
                    rocketspeed001.11.lla1.facebook.com \
                    rocketspeed002.11.lla1.facebook.com \
                    rocketspeed003.11.lla1.facebook.com \
                    rocketspeed004.11.lla1.facebook.com \
                    rocketspeed005.11.lla1.facebook.com \
                    rocketspeed006.11.lla1.facebook.com \
                    rocketspeed007.11.lla1.facebook.com \
                    rocketspeed009.11.lla1.facebook.com \
                    rocketspeed010.11.lla1.facebook.com \
                    rocketspeed011.11.lla1.facebook.com )
  elif [ $USER == "dhruba" ]; then
    available_hosts=( \
                    rocketspeed012.11.lla1.facebook.com \
                    rocketspeed013.11.lla1.facebook.com \
                    rocketspeed014.11.lla1.facebook.com \
                    rocketspeed019.11.lla1.facebook.com \
                    rocketspeed020.11.lla1.facebook.com \
                    rocketspeed021.11.lla1.facebook.com \
                    rocketspeed022.11.lla1.facebook.com \
                    rocketspeed023.11.lla1.facebook.com \
                    rocketspeed024.11.lla1.facebook.com \
                    rocketspeed030.11.lla1.facebook.com )
  elif [ $USER == "stupaq" ]; then
    available_hosts=( \
                    rocketspeed031.11.lla1.facebook.com \
                    rocketspeed032.11.lla1.facebook.com \
                    rocketspeed033.11.lla1.facebook.com )
  else
    available_hosts=( \
                    rocketspeed034.11.lla1.facebook.com \
                    rocketspeed036.11.lla1.facebook.com )
  fi
fi

if [ $strip ]; then
  echo
  echo "===== Stripping binary ====="

  if [ ! -f $server ]; then
    echo "Must have: "
    echo "  $server"
    echo "from current directory"
    exit 1
  fi

  TMPFILE=`mktemp /tmp/rocketspeed.XXXXXXXX`
  du -h $server
  cp -p $server $TMPFILE
  server=$TMPFILE
  strip $TMPFILE
  du -h $TMPFILE

  if [ $remote_bench ]; then
    TMPFILE=`mktemp /tmp/rocketbench.XXXXXXXX`
    du -h $bench
    cp -p $bench $TMPFILE
    bench=$TMPFILE
    strip $TMPFILE
    du -h $TMPFILE
  fi
fi

if [ $remote_bench ]; then
  bench_cmd="$remote_path/rocketbench"
else
  bench_cmd="$bench"
fi
bench_cmd="${bench_cmd} ${ROCKETBENCH_ARGS}"

pilots=("${available_hosts[@]::num_pilots}")
available_hosts=("${available_hosts[@]:num_pilots}")  # pop num_pilots off

copilots=("${available_hosts[@]::num_copilots}")
available_hosts=("${available_hosts[@]:num_copilots}")  # pop num_copilots off

control_towers=("${available_hosts[@]::num_towers}")
available_hosts=("${available_hosts[@]:num_towers}")  # pop num_towers off

# rocketbench binary on these hosts
#
rocketbench_hosts=""
if [ $remote_bench ]; then
  bench_cmd="$remote_path/rocketbench"
  if [ $ROCKETBENCH_HOSTS ]; then
    IFS=',' read -a rocketbench_hosts <<< "$ROCKETBENCH_HOSTS"
  else
    rocketbench_hosts=("${available_hosts[@]::num_bench}")
    available_hosts=("${available_hosts[@]:num_bench}")  # pop num_bench off
  fi
fi

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

function collect_stats {
  if [ $remote ]; then
    echo
    echo "===== Collecting server stats ====="
    for host in ${pilots[@]}; do
      echo stats pilot | nc $host 58800 > "pilot.$host.stats"
    done
    for host in ${copilots[@]}; do
      echo stats copilot | nc $host 58800 > "copilot.$host.stats"
    done
    for host in ${control_towers[@]}; do
      echo stats tower | nc $host 58800 > "tower.$host.stats"
    done
  fi
}

function stop_servers {
  if [ $remote ]; then
    echo
    echo "===== Stopping remote servers ====="
    for host in ${all_hosts[@]}; do
      echo "Stopping server:" $host
      ssh root@$host 'pkill -f ${remote_path}/rocketspeed'
    done
  fi
}

function start_servers {
  if [ $remote ]; then
    echo
    echo "===== Starting remote servers ====="
    for host in ${cockpits[@]}; do
      cmd="${remote_path}/rocketspeed \
        ${rocketspeed_args} \
        --pilot \
        --copilot \
        --control_towers=$towers_csv \
        --copilot_towers_per_log=1 \
        --rollcall=$rollcall \
        --storage_workers=40"
      if [ $pilot_port ]; then
        cmd="${cmd} --pilot_port=$pilot_port"
      fi
      if [ $copilot_port ]; then
        cmd="${cmd} --copilot_port=$copilot_port"
      fi
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
    for host in ${control_towers[@]}; do
      cmd="${remote_path}/rocketspeed \
        ${rocketspeed_args} \
        --tower"
      if [ $controltower_port ]; then
        cmd="${cmd} --tower_port=$controltower_port"
      fi
      if [ $log_dir ]; then
        cmd="${cmd} --rs_log_dir=$log_dir"
      fi
      if [ $cache_size ]; then
        cmd="${cmd} --tower_cache_size=$cache_size"
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

  # Deploy rocketspeed to remote hosts
  echo
  echo "===== Deploying $server to ${remote_path} on remote hosts ====="

  src=$server
  for host in ${all_hosts[@]}; do
    echo "$host"
    dest=root@$host:${remote_path}/rocketspeed
    if ! rsync -az $src $dest; then
      echo "Error deploying to $host"
      exit 1
    fi
  done
}

function collect_logs {
  if [ $remote ]; then
    echo
    echo "===== Merging remote logs into LOG.remote ====="
    rm -f LOG.remote
    rm -f LOG.tmp
    touch LOG.tmp
    for host in ${all_hosts[@]}; do
      cmd="cat ${log_dir}/LOG"
      # merge remote LOG file into LOG.remote
      # -k 2                       merge by second field (timestamp)
      # sed "s/^/${host:0:22}: /   prefix 22 chars of hostname (ignored in sort)
      sort -k 2 -m <(ssh root@$host "${cmd}" | sed "s/^/${host:0:22}: /") LOG.tmp > LOG.remote
      cmd="rm -f ${log_dir}/LOG*"
      if [ $stop_servers ]; then
        ssh root@$host "${cmd}"  # tidy up
      fi
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
  stop_servers
  deploy_servers
  progress='true'
fi

if [ $remote_bench ]; then
  # Deploy rocketbench to remote host
  for host in ${rocketbench_hosts[@]}; do

    echo
    echo "===== Deploying $bench to ${remote_path} on remote host ====="

    src=$bench
    echo "$host"
    dest=root@$host:${remote_path}/rocketbench
    if ! rsync -az $src $dest; then
      echo "Error deploying $bench to $host"
      exit 1
    fi
  done
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
  echo "./benchmark.sh [-bcdnrstxyz] [tunelatency|produce|readwrite|consume|subscriptionchurn]"
  echo
  echo "-b --size            Message size (bytes)."
  echo "-c --client_threads  Number of client threads."
  echo "-d --deploy          Deploy the rocketspeed binary to remote servers."
  echo "-e --start-servers   Start the rocketspeed binary on remote servers."
  echo "-l --collect-logs    Collect all logs from remote servers."
  echo "-n --num_messages    Number of messages to send."
  echo "-q --stop-servers    Stop the rocketspeed binary on remote servers."
  echo "-r --rate            Messages to send per second."
  echo "--subscribe-rate     Number of subscriptions to send per second."
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
  echo "--idle-timeout       Exit test if a message is not received for this time suration."
  echo "--remote-path        The directory where the rocketspeed binary is installed."
  echo "--log-dir            The directory for server logs."
  echo "--strip              Strip rocketspeed (and rocketbench) binaries for deploying."
  echo "--rollcall           Enable/disable RollCall."
  echo "--remote_bench       Use specified number of remote machines to run rocketbench."
  echo "--max_latency_micros Maximum average latency in microseconds for tunelatency."
  echo "--weibull_scale      Scale for weibull distribution."
  echo "--weibull_shape      Shape for weibull distribution."
  echo "--weibull_max_time   Max time to be generated by weibull distribution"
  exit 1
 fi
fi

output_dir=${OUTPUT_DIR:-/tmp}
if [ ! -d $output_dir ]; then
  mkdir -p $output_dir
fi

const_params="
  --namespaceid=$namespaceid \
  --message_size=$message_size \
  --num_messages=$num_messages \
  --message_rate=$message_rate \
  --subscribe_rate=$subscribe_rate \
  --client_workers=$client_workers \
  --num_topics=$num_topics"

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

# setup parameters to rocketbench
bench_param=" --idle_timeout=$idle_timeout"

function run_tunelatency {
  # Binary searches the --max_inflight parameter to achieve a mean latency
  # bound of max_latency_micros. This essentially runs the produce benchmark
  # a number of times, with different --max_inflight values until it converges
  # on the correct latency (retrieved from stdout).
  echo "Tuning in-flight messages to achieve latency bound..."
  min_inflight=1
  while [ $max_inflight -gt $min_inflight ]; do
    inflight=$(((max_inflight + min_inflight) / 2))
    cmd="$bench_cmd $const_params $bench_param \
         --start_producer=true \
         --start_consumer=false \
         --delay_subscribe=false \
         --max_inflight=$inflight \
         2>&1 | tee $output_dir/benchmark_tunelatency.log"
    if [ $remote_bench ]; then
      for host in ${rocketbench_hosts[@]}; do
        cmdnew="ssh root@$host -- $cmd"
        echo $cmdnew >> $output_dir/benchmark_produce.log
        eval $cmdnew >  $output_dir/result &
        # output file clobbered if more than one remote_bench?
      done
      # wait for all jobs to finish
      fail=0
      for bench_job in `jobs -p`
      do
        wait $bench_job || let "fail+=1"   # ignore race condition here
      done
      if [ "$fail" != "0" ];
      then
        echo "Remote rocketbench failed to complete successfully."
      fi
    else
      echo $cmd >> $output_dir/benchmark_produce.log
      eval $cmd > $output_dir/result
    fi
    latency=`grep "ack-latency" $output_dir/result | awk '{ print $3 }' | xargs printf "%.0f\n"`
    throughput=`grep "messages/s" $output_dir/result | awk '{ print $1 }'`
    echo "max_inflight: ${inflight}  latency(micros): ${latency}  throughput(qps): ${throughput}"
    if [ $latency -ge $max_latency_micros ]; then
      max_inflight=$((inflight))
    else
      min_inflight=$((inflight + 1))
    fi
  done
  echo
}

function run_produce {
  echo "Burst writing $num_messages messages into log storage..."
  rm -rf $output_dir/benchmark_produce.log
  cmd="$bench_cmd $const_params $bench_param \
       --start_producer=true \
       --start_consumer=false \
       --delay_subscribe=false \
       --subscriptionchurn=false \
       --max_inflight=$max_inflight \
       2>&1 | tee -a $output_dir/benchmark_produce.log"
  if [ $remote_bench ]; then
    # start all remote benchmark clients
    for host in ${rocketbench_hosts[@]}; do
      cmdnew="ssh root@$host -- $cmd"
      echo $cmdnew | tee -a $output_dir/benchmark_produce.log
      eval $cmdnew | sed "s/^/${host:0:22}: /" &
    done
    # wait for all jobs to finish
    fail=0
    for bench_job in `jobs -p`
    do
      wait $bench_job || let "fail+=1"   # ignore race condition here
    done
    if [ "$fail" != "0" ]; then
      echo "Remote rocketbench failed to complete successfully."
    fi
  else
    echo $cmd | tee -a $output_dir/benchmark_produce.log
    eval $cmd
  fi
  echo
}

function run_readwrite {
  echo "Writing and reading $num_messages simultaneously..."
  cmd="$bench_cmd $const_params $bench_param \
       --start_producer=true \
       --start_consumer=true \
       --delay_subscribe=false \
       --subscriptionchurn=false \
       --max_inflight=$max_inflight \
       2>&1 | tee $output_dir/benchmark_readwrite.log"
  if [ $remote_bench ]; then
    for host in ${rocketbench_hosts[@]}; do
      cmdnew="ssh root@$host -- $cmd"
      echo $cmdnew | tee -a $output_dir/benchmark_readwrite.log
      eval $cmdnew| sed "s/^/${host:0:22}: /" &
    done
    # wait for all jobs to finish
    fail=0
    for bench_job in `jobs -p`
    do
      wait $bench_job || let "fail+=1"   # ignore race condition here
    done
    if [ "$fail" != "0" ]; then
      echo "Remote rocketbench failed to complete successfully."
    fi
  else
    echo $cmd | tee $output_dir/benchmark_readwrite.log
    eval $cmd
  fi
  echo
}

function run_subscriptionchurn {
  echo "Writing and reading $num_messages with subscriber churn..."
  cmd="$bench_cmd $const_params \
       --start_producer=true \
       --start_consumer=true \
       --subscriptionchurn=true \
       --delay_subscribe=false \
       2>&1 | tee $output_dir/benchmark_subscriptionchurn.log"
  if [ $remote_bench ]; then
    cmd="ssh root@$rocketbench_host -- $cmd"
  fi
  echo $cmd | tee $output_dir/benchmark_subscriptionchurn.log
  eval $cmd
  echo
}
function run_consume {
  echo "Reading a backlog of $num_messages..."
  cmd="$bench_cmd $const_params $bench_param\
       --start_producer=true \
       --start_consumer=true \
       --delay_subscribe=true \
       --subscriptionchurn=false \
       --max_inflight=$max_inflight \
       2>&1 | tee $output_dir/benchmark_consume.log"
  if [ $remote_bench ]; then
    for host in ${rocketbench_hosts[@]}; do
      cmdnew="ssh root@$host -- $cmd"
      echo $cmdnew | tee -a $output_dir/benchmark_readwrite.log
      eval $cmdnew | sed "s/^/${host:0:22}: /" &
    done
  else
    echo $cmd | tee $output_dir/benchmark_consume.log
    eval $cmd
  fi
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
  if [ $job = tunelatency ]; then
    run_tunelatency
  elif [ $job = produce ]; then
    run_produce
  elif [ $job = readwrite ]; then
    run_readwrite
  elif [ $job = subscriptionchurn ]; then
    run_subscriptionchurn
  elif [ $job = consume ]; then
    run_consume
  else
    echo "unknown job $job"
    exit 1
  fi
  end=$(now)

  echo "Complete $job in $((end-start)) seconds" | tee -a $report
done

# Collect stats
if [ $collect_stats ]; then
  collect_stats
fi

# Stop servers if specified
if [ $stop_servers ]; then
  stop_servers
fi

# Collect all logs
if [ $collect_logs ]; then
  collect_logs
fi
