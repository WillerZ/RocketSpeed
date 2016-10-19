import centrifuge.runner as runner
from centrifuge.envs import SingleBoxEnv

import sys
import time

def wait(_):
    time.sleep(1)
    return True

config = {
    'clients': {
        'foo': {'cmd': 'echo client'},
        'fail': {'cmd': 'false'}
    },
    'servers': {
        'bar': {
            'cmd': 'echo server',
            # obviously this makes no sense in real life..
            'is_started': lambda proc: proc.wait() == 0
        },
        'quux': {
            'cmd': 'cat',
            'is_started': wait
        }
    },
    'tests': [
        {
            'client': 'foo',
            'server': 'bar',
            'hosts': {
                'client_count': 1,
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
        print (test)
