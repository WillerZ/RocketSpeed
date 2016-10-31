namespace py runner_thrift.runner

enum HealthStatus {
  HEALTHY,
  UNHEALTHY,
}

enum ProcessStatus {
  NO_PROCESS,                   //if nothing has started up yet
  RUNNING,
  STOPPED,
  FAILED,
  INVARIANT_FAILED,
}

struct ProcessState {
  1: required ProcessStatus status;
  2: required string message = "";
}

service ProcessRunner {

  HealthStatus ping(),

  // return true when the client has begun running successfully
  bool run(1: string proc, 2: i32 nth, 3: string key),

  // idempotently stop process for key
  bool stop(1: string key),

  ProcessState poll_running_proc(1: string key),

}