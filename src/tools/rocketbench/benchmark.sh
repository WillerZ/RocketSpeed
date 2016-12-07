#!/bin/bash

# Makes cmd1 | cmd2 return non-zero if either command returns non-zero.
# Useful for when we are piping through sed.
set -o pipefail

# size constants
K=1024
M=$((1024 * K))
G=$((1024 * M))

message_size=100
num_messages=1000
message_rate=100
subscribe_rate=10
subscription_backlog_distribution='fixed' # subscribe from start of topic
subscription_topic_ratio='' # subscribe to a subset of all topics
topics_distribution=''
num_messages_per_topic=''
max_inflight=1000
remote=''
deploy=''
client_workers=32
num_topics=100
num_cockpits=1
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
cache_block_size='' # number of messages in a cache block
num_bench=''
remote_bench=''
idle_timeout="5"  # if no new messages within this time, then declare done
max_latency_micros=10000
start_producer="true"
socket_buffer_size=''
buffered_storage_max_latency_us='' #max batching time on client in micro seconds
progress_period='' # time in ms between updates to progress bar
progress_per_line=''
max_file_descriptors=''
num_messages_to_receive=''        # expected #msg to arrive via subscriptions
delay_after_subscribe_seconds=''  # wait after issuing subscriptions

# Use the 2 lower order bytes from the UID to generate a namespace id.
# The hope is that this will be sufficiently unique so that concurrent
# runs of this benchmark do not pollute one another.
namespaceid=`id -u`

# Vary the namespace for every concurrent rocketbench run.
# If this is not set, then all rocketbench runs uses $namespaceid.
namespaceid_dynamic=''

# If you want to use the debug build, then set an environment variable
# called DBG=dbg. By default, pick the optimized build.
part=${DBG:-opt}

server=${SERVER:-_build/$part/rocketspeed/github/src/server/rocketspeed}
bench=_build/$part/rocketspeed/github/src/tools/rocketbench/rocketbench

# Argument parsing
OPTS=`getopt -o b:c:dn:r:st:x:y:z: \
             -l size:,client-threads:,deploy,start-servers,stop-servers,collect-logs,collect-stats,messages:,rate:,remote,topics:,cockpits:,towers:,pilot-port:,copilot-port:,controltower-port:,cockpit-host:,controltower-host:,remote-path:,log-dir:,strip,rollcall:,remote-bench:,subscription-backlog-distribution:,subscription-topic-ratio:,subscribe-rate:,cache-size:,cache-block-size:,idle-timeout:,max-inflight:,max_latency_micros:weibull_scale:,weibull_shape:,weibull_max_time:,producer:,socket-buffer-size:,buffered_storage_max_latency_us:,progress_period:,progress_per_line,max_file_descriptors:,namespaceid:,namespaceid_dynamic,topics_distribution:,num_messages_per_topic:,num_messages_to_receive:,delay_after_subscribe_seconds: \
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
    -x | --cockpits )
      num_cockpits="$2"; shift 2 ;;
    -z | --towers )
      num_towers="$2"; shift 2 ;;
    --cache-size )
      cache_size="$2"; shift 2 ;;
    --cache-block-size )
      cache_block_size="$2"; shift 2 ;;
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
    --subscription-backlog-distribution )
      subscription_backlog_distribution="$2"; shift 2 ;;
    --subscription-topic-ratio )
      subscription_topic_ratio="$2"; shift 2 ;;
    --max-inflight )
      max_inflight="$2"; shift 2 ;;
    --max_latency_micros )
      max_latency_micros="$2"; shift 2 ;;
    --producer )
      start_producer="$2"; shift 2 ;;
    --socket-buffer-size )
      socket_buffer_size="$2"; shift 2 ;;
    --buffered_storage_max_latency_us )
      buffered_storage_max_latency_us="$2"; shift 2 ;;
    --progress_period )
      progress_period="$2"; shift 2 ;;
    --progress_per_line )
      progress_per_line='true'; shift ;;
    --max_file_descriptors )
      max_file_descriptors="$2"; shift 2 ;;
    --namespaceid )
      namespaceid="$2"; shift 2 ;;
    --namespaceid_dynamic )
      namespaceid_dynamic=true; shift ;;
    --topics_distribution )
      topics_distribution="$2"; shift 2 ;;
    --num_messages_per_topic )
      num_messages_per_topic="$2"; shift 2 ;;
    --num_messages_to_receive )
      num_messages_to_receive="$2"; shift 2 ;;
    --delay_after_subscribe_seconds )
      delay_after_subscribe_seconds="$2"; shift 2 ;;
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
fi

if [ -z ${ROCKETSPEED_ARGS+x} ]; then
  logdevice_cluster=${LOGDEVICE_CLUSTER:-rocketspeed.logdevice.primary}
  storage_url="configerator:logdevice/${logdevice_cluster}.conf"
  rocketspeed_args="--storage_url=$storage_url --logdevice_cluster=$logdevice_cluster --storage_workers=40"
  if [ $buffered_storage_max_latency_us ]; then
    rocketspeed_args="${rocketspeed_args} --buffered_storage_max_latency_us=$buffered_storage_max_latency_us"
    rocketspeed_args="${rocketspeed_args} --buffered_storage_max_messages=255"
  fi
else
  rocketspeed_args=$ROCKETSPEED_ARGS
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

required_hosts=$((num_cockpits+num_towers+num_bench))
if [ $required_hosts -gt ${#available_hosts[@]} ]; then
  echo
  echo "FATAL: not enough available hosts to continue (required: $required_hosts, available: ${#available_hosts[@]})"
  echo
  exit 1
fi

cockpits=("${available_hosts[@]::num_cockpits}")
available_hosts=("${available_hosts[@]:num_cockpits}")  # pop num_cockpits off

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
fi
if [ $controltower_host ]; then
  control_towers=( $controltower_host )
fi
all_hosts=("${cockpits[@]} ${control_towers[@]}")

function join {
  # joins elements of an array with $1
  local IFS="$1"; shift; echo "$*";
}

cockpits_csv=$(join , ${cockpits[@]})
towers_csv=$(join , ${control_towers[@]})

# Increase max_file_descriptors of the rocketspeed
if [ $max_file_descriptors ]; then
  limitcmd="ulimit -n $max_file_descriptors"
else
  limitcmd="echo -n " # do nothing
fi

function collect_stats {
  if [ $remote ]; then
    echo
    echo "===== Collecting server stats ====="
    for host in ${cockpits[@]}; do
      echo stats pilot | nc $host 58800 > "pilot.$host.$(date +%s).stats"
      echo stats copilot | nc $host 58800 > "copilot.$host.$(date +%s).stats"
    done
    for host in ${control_towers[@]}; do
      echo stats tower | nc $host 58800 > "tower.$host.$(date +%s).stats"
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
    for host in ${rocketbench_hosts[@]}; do
      echo "Stopping server:" $host
      ssh root@$host 'pkill -f ${remote_path}/rocketbench'
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
        --rollcall=$rollcall"
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
      if [ $socket_buffer_size ]; then
        cmd="${cmd} --socket_buffer_size=$socket_buffer_size"
      fi
      cmd="$limitcmd && $cmd"
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
      if [ $cache_block_size ]; then
        cmd="${cmd} --tower_cache_block_size=$cache_block_size"
      fi
      cmd="$limitcmd && $cmd"
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
    cmd="rsync -az $src $dest"
    eval $cmd &
  done

  # wait for all jobs to finish
  fail=0
  for bench_job in `jobs -p`
  do
    wait $bench_job || let "fail+=1"   # ignore race condition here
  done
  if [ "$fail" != "0" ];
  then
    echo "Error in deploying rocketspeed binary"
    exit 1
  fi
}

function deploy_rocketbench {
  # Deploy rocketbench to remote host
  echo "===== Deploying $bench to ${remote_path} on remote host ====="
  for host in ${rocketbench_hosts[@]}; do
    src=$bench
    echo "$host"
    dest=root@$host:${remote_path}/rocketbench
    cmd="rsync -az $src $dest"
    eval $cmd &
  done
  # wait for all jobs to finish
  fail=0
  for bench_job in `jobs -p`
  do
    wait $bench_job || let "fail+=1"   # ignore race condition here
  done
  if [ "$fail" != "0" ];
  then
    echo "Error in deploying rocketbench binary"
    exit 1
  fi
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
  deploy_rocketbench
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
  echo "--subscribe-backlog  Subsciption requests read backlog data."
  echo "--subscription-backlog-distribution "Backlog reads use this among data for a topic."
  echo "--subscription-topic-ratio "Ratio of number of total topics to number of subscribed topics."
  echo "-s --remote          Use remote server(s) for pilot, copilot, control towers."
  echo "-t --topics          Number of topics."
  echo "-x --cockpits        Number of cockpits to use."
  echo "-z --towers          Number of control towers to use."
  echo "--pilot-port         The port number for the pilot."
  echo "--copilot-port       The port number for the copilot."
  echo "--controltower-port  The port number for the control tower"
  echo "--cockpit-host       The machine name that runs the pilot and the copilot."
  echo "--controltower-host  The machine name that runs the control tower."
  echo "--idle-timeout       Exit test if a message is not received for this time suration."
  echo "--remote-path        The directory where the rocketspeed binary is installed."
  echo "--log-dir            The directory for server logs."
  echo "--progress_period    The time in milliseconds between updates to progress bar."
  echo "--progress_per_line  Print progress on separate line, rather that on one line."
  echo "--strip              Strip rocketspeed (and rocketbench) binaries for deploying."
  echo "--rollcall           Enable/disable RollCall."
  echo "--remote_bench       Use specified number of remote machines to run rocketbench."
  echo "--max_latency_micros Maximum average latency in microseconds for tunelatency."
  echo "--max_file_descriptors Maximum number of file descriptors for rocketspeed daemons."
  echo "--producer           Set to true if this run should produce data, otherwise false."
  echo "--socket-buffer-size The size of the send or receive buffer associated with a socket."
  echo "--weibull_scale      Scale for weibull distribution."
  echo "--weibull_shape      Shape for weibull distribution."
  echo "--weibull_max_time   Max time to be generated by weibull distribution"
  echo "--buffered_storage_max_latency_us  Time in microseconds that a message will be buffered on the client for efficient batching."
  echo "--namespaceid        The namespaceid used for the workload."
  echo "--namespaceid_dynamic Dynamically generate different namespaceids for each simultaneous workload."
  echo "--topics_distribution Distribution for generating topic names."
  echo "--num_messages_per_topic Number of messages per topic."
  echo "--num_messages_to_receive Expected number of messages to be received."
  echo -n "--delay_after_subscribe_seconds Wait period between issuing "
  echo "all subscriptions and starting measurement timer."
  echo "--cache-size          Size in bytes to be cached in control tower."
  echo "--cache-block-size    Number of messages in a cache block."
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
  const_params+=" --pilot_hostnames=$cockpits_csv"
  const_params+=" --copilot_hostnames=$cockpits_csv"
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
if [ $progress_period ]; then
  bench_param+=" --progress_period=$progress_period"
fi
if [ $progress_per_line ]; then
  bench_param+=" --progress_per_line"
fi
if [ $topics_distribution ]; then
  bench_param+=" --topics_distribution=$topics_distribution"
fi
if [ $num_messages_per_topic ]; then
  bench_param+=" --num_messages_per_topic=$num_messages_per_topic"
fi
if [ $num_messages_to_receive ]; then
  bench_param+=" --num_messages_to_receive=$num_messages_to_receive"
fi
if [ $delay_after_subscribe_seconds ]; then
  bench_param+=" --delay_after_subscribe_seconds=$delay_after_subscribe_seconds"
fi


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
         --max_inflight=$inflight "
    if [ $remote_bench ]; then
      cmd="\" $limitcmd &&  $cmd\""
      hostnum=0
      for host in ${rocketbench_hosts[@]}; do
        # create dynamic namespaceid if specified
        hostnum=$(($hostnum + 1))
        nsid=""
        if [ $namespaceid_dynamic ]; then
          nsid="--namespaceid=$namespaceid.$hostnum"
        else
          nsid="--namespaceid=$namespaceid"
        fi
        cmdn="$cmd $nsid 2>&1 | tee $output_dir/benchmark_produce.log"
        cmdnew="ssh root@$host -- $cmdn"
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
       --max_inflight=$max_inflight "
  if [ $remote_bench ]; then
    cmd="\" $limitcmd && $cmd\""
    # start all remote benchmark clients
    hostnum=0
    for host in ${rocketbench_hosts[@]}; do
      # create dynamic namespaceid if specified
      hostnum=$(($hostnum + 1))
      nsid=""
      if [ $namespaceid_dynamic ]; then
        nsid="--namespaceid=$namespaceid.$hostnum"
      else
        nsid="--namespaceid=$namespaceid"
      fi
      cmdn="$cmd $nsid 2>&1 | tee $output_dir/benchmark_produce.log"
      cmdnew="ssh root@$host -- $cmdn"
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
      return 1
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
       --max_inflight=$max_inflight "
  if [ $remote_bench ]; then
    cmd="\" $limitcmd && $cmd\""
    hostnum=0
    for host in ${rocketbench_hosts[@]}; do
      # create dynamic namespaceid if specified
      hostnum=$(($hostnum + 1))
      nsid=""
      if [ $namespaceid_dynamic ]; then
        nsid="--namespaceid=$namespaceid.$hostnum"
      else
        nsid="--namespaceid=$namespaceid"
      fi
      cmdn="$cmd $nsid 2>&1 | tee $output_dir/benchmark_readwrite.log"
      cmdnew="ssh root@$host -- $cmdn"
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
      return 1
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
       --delay_subscribe=false "
  if [ $remote_bench ]; then
    cmd="\" $limitcmd && $cmd\""
    hostnum=0
    for host in ${rocketbench_hosts[@]}; do
      # create dynamic namespaceid if specified
      hostnum=$(($hostnum + 1))
      nsid=""
      if [ $namespaceid_dynamic ]; then
        nsid="--namespaceid=$namespaceid.$hostnum"
      else
        nsid="--namespaceid=$namespaceid"
      fi
      cmdn="$cmd $nsid 2>&1 | tee $output_dir/benchmark_subscriptionchurn.log"
      cmdnew="ssh root@$host -- $cmdn"
      echo $cmdnew | tee -a $output_dir/benchmark_subscriptionchurn.log
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
      return 1
    fi
  else
    echo $cmd | tee $output_dir/benchmark_subscriptionchurn.log
    eval $cmd
  fi
  echo
}

function run_consume {
  echo "Reading a backlog of $num_messages..."
  cmd="$bench_cmd $const_params $bench_param\
       --start_producer=$start_producer \
       --start_consumer=true \
       --delay_subscribe=true \
       --subscriptionchurn=false \
       --subscription_backlog_distribution=$subscription_backlog_distribution \
       --max_inflight=$max_inflight "
  if [ $subscription_topic_ratio ]; then
    cmd+=" --subscription_topic_ratio=$subscription_topic_ratio "
  fi
  if [ $remote_bench ]; then
    cmd="\" $limitcmd && $cmd\""
    hostnum=0;
    for host in ${rocketbench_hosts[@]}; do
      # create dynamic namespaceid if specified
      hostnum=$(($hostnum + 1))
      nsid=""
      if [ $namespaceid_dynamic ]; then
        nsid="--namespaceid=$namespaceid.$hostnum"
      else
        nsid="--namespaceid=$namespaceid"
      fi
      cmdn="$cmd $nsid 2>&1 | tee $output_dir/benchmark_readwrite.log"
      cmdnew="ssh root@$host -- $cmdn"
      echo $cmdnew | tee -a $output_dir/benchmark_readwrite.log
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
      return 1
    fi
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

result=0
for job in ${jobs[@]}; do
  echo "Start $job at `date`" | tee -a $report
  echo
  start=$(now)
  if [ $job = tunelatency ]; then
    run_tunelatency || let "result=1"
  elif [ $job = produce ]; then
    run_produce || let "result=1"
  elif [ $job = readwrite ]; then
    run_readwrite || let "result=1"
  elif [ $job = subscriptionchurn ]; then
    run_subscriptionchurn || let "result=1"
  elif [ $job = consume ]; then
    run_consume || let "result=1"
  else
    echo "unknown job $job"
    let "result=1"
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

exit "$result"
