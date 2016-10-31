from runner_thrift.runner import ProcessRunner
import runner_thrift.runner.ttypes as Svc

import centrifuge.log as log
from centrifuge.backoff import exponential_retry

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

import sys
import traceback
import time
import functools
import threading
import subprocess as sp

def kill_proc(proc):
    if not proc:
        return
    # poll returns None if still running
    if proc.poll() is not None:
        return
    proc.terminate()
    time.sleep(5)              # you get 5 secs to shutdown
    if proc.poll() is None:
        proc.kill()
    return proc.wait()

class ProcessRunnerHandler:
    def __init__(self, config):
        self.config = config
        self.procs = {}         # key -> (proc, start time, config)

    def ping(self):
        return Svc.HealthStatus.HEALTHY

    def run(self, proc, nth, key):
        config = self.config.get(proc, {})
        args = config.get('cmd')
        if not args:
            return False
        if callable(args):
            args = args(nth)
        self.stop(key)
        p = sp.Popen(args, shell=True)
        self.procs[key] = (p, time.time(), config)
        return config.get('is_started', lambda _: True)(p)

    def stop(self, key):
        kill_proc(self.procs.get(key, (None,))[0])
        return True

    def poll_running_proc(self, key):
        def interpret_inv(result):
            if type(result) is str:
                return (Svc.ProcessStatus.INVARIANT_FAILED, result)
            if not result:
                return (Svc.ProcessStatus.INVARIANT_FAILED, 'No message provided')
            return (Svc.ProcessStatus.RUNNING, '')

        def get_status(proc, start_time, config):
            if not proc:
                return (Svc.ProcessStatus.NO_PROCESS, '')
            else:
                ret = proc.poll()
                if ret is None:
                    inv = config.get('invariant')
                    if not inv is None:
                        return interpret_inv(inv(proc, time.time() - start_time))
                    return (Svc.ProcessStatus.RUNNING, '')
                if ret > 0:
                    return (Svc.ProcessStatus.FAILED, 'Failed with exit code %s' % ret)
                return (Svc.ProcessStatus.STOPPED, '')

        state = Svc.ProcessState()
        state.status, state.message = get_status(*self.procs.get(key, (None, 0, {})))
        return state

def start_runner(port, handler, processor):
    transport = TSocket.TServerSocket(port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    t = threading.Thread(target=server.serve)
    t.start()
    return t

def create_client(host, port, runner):
    transport = TSocket.TSocket(host, port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = runner.Client(protocol)
    return (transport, client)

@exponential_retry(TTransport.TTransportException, multiplier=30000,
                   delay_limit=5 * 60000, max_attempts=10)
def connect(transport, client):
    transport.open()

def close(transport, client):
    transport.close()

def connect_all(clients):
    for host, client in clients:
        connect(*client)
        response = client[1].ping()
        if response != Svc.HealthStatus.HEALTHY:
            raise (BlockingIOError("%s returned %s from ping" % (host, response)))

def close_all(clients):
    for k, v in clients:
        close(*v)

def run_everywhere(runners):
    for host, runner, proc, nth, key in runners:
        if not runner[1].run(proc, nth, key):
            raise Exception('Running %s on host %s failed.' % (proc, host))

def stop_everywhere(runners):
    for host, runner, _, _, key in runners:
        if not runner[1].stop(key):
            raise Exception('Stopping %s on host %s failed.' % (key, host))

def check_proc_everywhere(runners):
    return [
        (host, runner[1].poll_running_proc(key))
        for host, runner, _, _, key in runners
    ]

def states_in_state(states, state):
    return [
        (host, s) for host, s in states if s.status == state
    ]

def nothing_failed(states):
    count = len(states)
    count -= len(states_in_state(states, Svc.ProcessStatus.RUNNING))
    count -= len(states_in_state(states, Svc.ProcessStatus.STOPPED))
    return count == 0

def all_clients_finished_successfully(states):
    return len(states) == len(states_in_state(states, Svc.ProcessStatus.STOPPED))

def summarize_test(states):
    def data(state):
        filtered = states_in_state(states, state)
        msg = ['[%s - %s]' % (host, state.message) for host, state in filtered]
        return (len(filtered), ' '.join(msg))
    stopped = data(Svc.ProcessStatus.STOPPED)
    running = data(Svc.ProcessStatus.RUNNING)
    failed = data(Svc.ProcessStatus.FAILED)
    invariant_failed = data(Svc.ProcessStatus.INVARIANT_FAILED)
    log.orchestrate('Summary: %s clients finished successfully %s' % stopped)
    log.orchestrate('Summary: %s clients still running %s' % running)
    log.orchestrate('Summary: %s clients failed %s' % failed)
    log.orchestrate('Summary: %s clients with invariant failures %s' % invariant_failed)

def log_stopped_clients(stopped_clients, states):
    if stopped_clients is None:
        ret = {}
        for host, _ in states:
            ret[host] = ret.get(host, 0) + 1
        return ret

    for host, _ in states_in_state(states, Svc.ProcessStatus.STOPPED):
        new_count = stopped_clients.get(host, 1) - 1
        stopped_clients[host] = new_count
        if new_count < 1:
            log.info("%s finished successfully" % host)

def orchestrate_test(clients, servers):
    try:
        run_everywhere(servers)
        log.info("All servers started")

        start = time.localtime()

        run_everywhere(clients)
        log.info("All clients started")

        states = check_proc_everywhere(clients)
        stopped_clients = log_stopped_clients(None, states)
        while nothing_failed(states):
            if all_clients_finished_successfully(states):
                log.orchestrate('TEST SUCCEEDED -- all clients finished gracefully')
                return (True, start)
            time.sleep(1)
            states = check_proc_everywhere(clients)
            log_stopped_clients(stopped_clients, states)
        log.orchestrate('TEST FAILED')
        summarize_test(states)
    except Exception as e:
        log.orchestrate('TEST EXPLODED')
        traceback.print_exc(file=sys.stdout)
    finally:
        stop_everywhere(servers)
        log.info("Stopped servers")
        stop_everywhere(clients)
        log.info("Stopped clients")
    return (False, start)

def summarize_tests(test_results):
    count_succeeded = sum(1 for x in test_results if x['success'])
    log.orchestrate('-' * 80)
    log.orchestrate('')
    log.orchestrate('Ran %s tests. %s succeeded. %s failed.' %
                    (len(test_results),
                     count_succeeded,
                     len(test_results) - count_succeeded))

def partition(hosts, config):
    clients = config.get('client_count')
    servers = config.get('server_count')
    if not clients and not servers:
        # split 50:50
        clients = len(hosts) / 2
        servers = len(hosts) - clients
    if not clients:
        clients = len(hosts) - servers
    if not servers:
        servers = len(hosts) - clients

    return (hosts[0:clients], hosts[clients:][-servers:])

def orchestrate(clients, tests):
    # block until everyone is up and ready to take requests
    connect_all(clients)

    log.info('All runners up and responding')

    for i, test in enumerate(tests):
        name = test.get('test_name', '<name not provided>')
        server = test['server']
        client = test['client']

        log.orchestrate('-' * 50)
        log.orchestrate('Starting test \'%s\' (%s of %s)' %
                        (name, i+1, len(tests)))
        log.orchestrate('server: %s client: %s' % (server, client))

        start = time.time()

        client_runners, server_runners = partition(clients, test.get('hosts', {}))
        clients_per_host = test.get('hosts', {}).get('clients_per_host', 1)
        servers_per_host = test.get('hosts', {}).get('servers_per_host', 1)
        clients_to_run = [(*runner, client, i, 'client-%s' % i)
                          for i in range(clients_per_host)
                          for runner in client_runners]
        servers_to_run = [(*runner, server, i, 'server-%s' % i)
                          for i in range(servers_per_host)
                          for runner in server_runners]

        log.info("Will attempt to start %s clients on %s hosts" %
                 (len(clients_to_run), len(client_runners)))
        log.info("Will attempt to start %s servers on %s hosts" %
                 (len(servers_to_run), len(server_runners)))

        test['success'], test['start_time'] = orchestrate_test(
            clients_to_run, servers_to_run)

        test['test_duration'] = int(time.time() - start)
        yield test

    summarize_tests(tests)
    close_all(clients)

def create_runner(env, config):
    runner_port = 8090

    log.info("Acting as a process runner")
    handler = ProcessRunnerHandler(config.get('processes', {}))
    processor = ProcessRunner.Processor(handler)
    thread = start_runner(runner_port, handler, processor)

    def f():
        log.info("Starting a test sequence run")

        if env.is_orchestrator():
            log.info("Acting as the orchestrator")
            hosts = env.hosts()
            # list of pairs - multiset
            clients = [
                (host, create_client(host, runner_port, ProcessRunner)) for host in hosts
            ]
            if len(clients) < 2:
                raise Exception('Need at least one runner for each clients and servers: %s' %
                                clients)
            for test in orchestrate(clients, config.get('tests', [])):
                yield test

        else:
            thread.join()

    return f
