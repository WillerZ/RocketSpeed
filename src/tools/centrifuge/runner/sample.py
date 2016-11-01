import centrifuge.runner as runner
from centrifuge.envs import SingleBoxEnv

import sys
import time

# is started predicate takes a process and returns true or false
def wait(_):
    time.sleep(1)
    return True

# invariant: may return true to suggest everything is ok, false or
# string indicates failure. string is used to provide an error
def timeout(proc, runtime_in_secs):
    if runtime_in_secs > 2:
        return 'Timed out error msg'
    return True

config = {
    'tests': [
        {
            'test_name': 'basic startup and shutdown',
            'client': {
                'cmd': lambda x: 'echo ' + str(x),
            },
            'server': {
                'cmd': 'echo server',
                # obviously this makes no sense in real life..
                'is_started': lambda proc: proc.wait() == 0
            },
            'hosts': {
                'clients_per_host': 8,
                'client_host_count': 1, # you may specify either client or
                # server count, neither or both
            },
        },

        {
            'test_name': 'client timeout; server hangs',
            'client': {
                'cmd': 'cat',
                'invariant': timeout,
            },
            'server': {
                'cmd': 'cat',
                'is_started': wait, # can be specified for servers or clients
            },
            'hosts': {
                'server_host_count': 1,
            },
        },

        {
            'test_name': 'client hangs; server timeout',
            'client': {
                'cmd': 'cat',
            },
            'server': {
                'cmd': 'cat',
                'is_started': wait, # can be specified for servers or clients
                'invariant': timeout,
            },
            'hosts': {
                'server_host_count': 1,
            },
        },

        {
            'test_name': 'client fails; server hangs',
            'client': {
                'cmd': 'false'
            },
            'server': {
                'cmd': 'cat',
                'is_started': wait, # can be specified for servers or clients
            },
            'hosts': {
                'client_host_count': 3,
                'server_host_count': 1,
            },
        },
    ],
}

if __name__ == '__main__':
    test_runner = runner.create_runner(SingleBoxEnv(), config)
    for test in test_runner():
        pass
