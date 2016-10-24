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
    'clients': {
        'foo': {'cmd': 'echo client'},
        'cat': {
            'cmd': 'cat',
            'invariant': timeout, # currently only works for clients
        },
        'fail': {'cmd': 'false'},
    },
    'servers': {
        'bar': {
            'cmd': 'echo server',
            # obviously this makes no sense in real life..
            'is_started': lambda proc: proc.wait() == 0
        },
        'quux': {
            'cmd': 'cat',
            'is_started': wait, # can be specified for servers or clients
        }
    },
    'tests': [
        {
            'client': 'foo',
            'server': 'bar',
            'hosts': {
                'client_count': 1, # you may specify either client or
                                   # server count, neither or both
            },
        },
        {
            'client': 'cat',
            'server': 'quux',
            'hosts': {
                'server_count': 1,
            },
        },
        {
            'client': 'fail',
            'server': 'quux',
            'hosts': {
                'server_count': 1,
            },
        },
    ],
}

if __name__ == '__main__':
    test_runner = runner.create_runner(SingleBoxEnv(), config)
    for test in test_runner():
        pass
