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
        'baz': {'cmd': 'false'}
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
    }
}

if __name__ == '__main__':
    sys.exit(runner.run(SingleBoxEnv(), config))
