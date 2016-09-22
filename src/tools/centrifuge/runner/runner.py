from runner_thrift.runner import ClientRunner, ServerRunner
import runner_thrift.runner.ttypes as Svc

import centrifuge.log as log
from centrifuge.backoff import exponential_retry

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

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

class ClientRunnerHandler:
    def __init__(self, config):
        self.config = config
        self.proc = None

    def ping(self):
        return Svc.HealthStatus.HEALTHY

    def run(self, client):
        args = self.config.get('clients', {}).get(client, {}).get('cmd')
        if not args:
            return False
        kill_proc(self.proc)
        self.proc = sp.Popen(args, shell=True)
        return True

    def poll_running_client(self):
        def get_status(proc):
            if not self.proc:
                return Svc.ClientStatus.NO_PROCESS
            else:
                ret = self.proc.poll()
                if ret is None:
                    return Svc.ClientStatus.RUNNING
                if ret > 0:
                    return Svc.ClientStatus.FAILED
                return Svc.ClientStatus.STOPPED

        state = Svc.ClientState()
        state.status = get_status(self.proc)
        state.message = ''      # TODO:
        return state

class ServerRunnerHandler:
    def __init__(self, config):
        self.config = config
        self.proc = None

    def ping(self):
        return Svc.HealthStatus.HEALTHY

    def run(self, server_key):
        server_config = self.config.get('servers', {}).get(server_key, {})
        args = server_config.get('cmd')
        if not args:
            return False
        kill_proc(self.proc)
        self.proc = sp.Popen(args, shell=True)
        return server_config.get('is_started', lambda p: True)(self.proc)

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
    for k, v in clients.items():
        connect(*v)
        response = v[1].ping()
        if response != Svc.HealthStatus.HEALTHY:
            raise (BlockingIOError("%s returned %s from ping" % (k, response)))

def close_all(clients):
    for k, v in clients.items():
        close(*v)

def run_everywhere(runners, test_key):
    for host, runner in runners.items():
        if not runner.run(test_key):
            raise Exception('Running %s on host %s failed.' % (test_key, host))

def check_client_everywhere(client_runners):
    return {
        host: client_runner.poll_running_client()
        for host, client_runner in client_runners.items()
    }

def states_in_state(states, state):
    return {
        host: s for host, s in states.items() if s.status == state
    }

def nothing_failed(states):
    count = len(states)
    count -= len(states_in_state(states, Svc.ClientStatus.RUNNING))
    count -= len(states_in_state(states, Svc.ClientStatus.STOPPED))
    return count == 0

def all_clients_finished_successfully(states):
    return len(states) == len(states_in_state(states, Svc.ClientStatus.STOPPED))

def summarize_test(states):
    def data(state):
        filtered = states_in_state(states, state)
        return (len(filtered), ' '.join(filtered.keys()))
    stopped = data(Svc.ClientStatus.STOPPED)
    running = data(Svc.ClientStatus.RUNNING)
    failed = data(Svc.ClientStatus.FAILED)
    timed_out = data(Svc.ClientStatus.TIMED_OUT)
    log.orchestrate('Summary: %s clients finished successfully [%s]' % stopped)
    log.orchestrate('Summary: %s clients still running [%s]' % running)
    log.orchestrate('Summary: %s clients failed [%s]' % failed)
    log.orchestrate('Summary: %s clients timed out [%s]' % timed_out)

def orchestrate_test(peers, client, server):
    try:
        log.orchestrate('-' * 80)
        log.orchestrate('Starting test -- server: %s client: %s' % (server, client))
        client_runners = {
            host: peer[1] for host, peer in peers['client_runners'].items()
        }
        server_runners = {
            host: peer[1] for host, peer in peers['server_runners'].items()
        }
        run_everywhere(server_runners, server)
        log.info("Server is up on %s host(s)" % len(server_runners))
        run_everywhere(client_runners, client)
        log.info("Client has started on %s host(s)" % len(client_runners))
        states = check_client_everywhere(client_runners)
        while nothing_failed(states):
            if all_clients_finished_successfully(states):
                log.orchestrate('TEST SUCCEEDED -- all clients finished gracefully')
                return True
            time.sleep(1)
            states = check_client_everywhere(client_runners)
        log.orchestrate('TEST FAILED')
        summarize_test(states)
    except Exception as e:
        log.orchestrate('TEST EXPLODED')
        traceback.print_exc()
    return False

def summarize_tests(test_results):
    count_succeeded = sum(1 for x in test_results if x)
    log.orchestrate('-' * 80)
    log.orchestrate('')
    log.orchestrate('Ran %s tests. %s succeeded. %s failed.' %
                    (len(test_results),
                     count_succeeded,
                     len(test_results) - count_succeeded))

def orchestrate(peers, config):
    def tests(config):
        for client in config.get('clients', {}).keys():
            for server in config.get('servers', {}).keys():
                yield (client, server)

    # block until everyone is up and ready to take requests
    connect_all(peers['server_runners'])
    connect_all(peers['client_runners'])

    log.info('All runners up and responding')

    test_results = [orchestrate_test(peers, *test) for test in tests(config)]
    summarize_tests(test_results)

    close_all(peers['client_runners'])
    close_all(peers['server_runners'])

    return 0 if all(test_results) else 1

def run(env, config):
    client_port = 8090
    server_port = 8091
    log.info("Starting a test sequence run")
    thread = None
    if env.is_client():
        log.info("Acting as a client runner")
        handler = ClientRunnerHandler(config)
        processor = ClientRunner.Processor(handler)
        thread = start_runner(client_port, handler, processor)

    if env.is_server():
        log.info("Acting as a server runner")
        handler = ServerRunnerHandler(config)
        processor = ServerRunner.Processor(handler)
        thread = start_runner(server_port, handler, processor)

    if env.is_orchestrator():
        log.info("Acting as the orchestrator")
        hosts = env.hosts()
        peers = {
            'client_runners': {
                host: create_client(host, client_port, ClientRunner)
                for host in hosts['clients']
            },
            'server_runners': {
                host: create_client(host, server_port, ServerRunner)
                for host in hosts['servers']
            }
        }
        if len(peers['client_runners']) == 0 or len(peers['server_runners']) == 0:
            raise Exception('Need at least one runner for each clients and servers: %s' %
                            peers)
        return orchestrate(peers, config)
    else:
        if thread is not None:
            thread.join()
    return 0
