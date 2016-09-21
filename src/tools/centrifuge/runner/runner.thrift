namespace py runner_thrift.runner

enum HealthStatus {
  HEALTHY,
  UNHEALTHY,
}

enum ClientStatus {
  NO_PROCESS,                   //if nothing has started up yet
  RUNNING,
  STOPPED,
  FAILED,
  TIMED_OUT,
}

struct ClientState {
  1: required ClientStatus status;
  2: required string message = "";
}

service ClientRunner {

  HealthStatus ping(),

  // return true when the client has begun running successfully
  bool run(1: string client),

  ClientState poll_running_client()
}

service ServerRunner {

  HealthStatus ping(),

  // kills any previous server that was running and starts a new one
  // return true when the server is up and running, false otherwise
  bool run(1: string server_key),
}
