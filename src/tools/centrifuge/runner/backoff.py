import time
import math

import centrifuge.log as log

def expo_delay(attempt, multiplier=100, base=2,
               allowed_attempts=5, delay_limit=10000):
    '''Return milliseconds to delay for or -1 if no more delay.'''
    if attempt > allowed_attempts:
        return -1

    x = multiplier * math.pow(base, attempt - 1)
    return min(delay_limit, x)

# Retry decorator with exponential backoff
def exponential_retry(exception, multiplier=100, base=2,
                      max_attempts=5, delay_limit=10000):
    '''Synchronously retries a function if it throws exception.'''
    def decorator(f):
        def f_retry(*args, **kwargs):
            attempt = 1

            while True:
                try:
                    return f(*args, **kwargs)   # first attempt
                except exception as e:
                    delay = expo_delay(attempt, multiplier, base,
                                       max_attempts, delay_limit)
                    if delay < 0:
                        raise e
                    log.warn(
                        'Caught [%s]. Delaying for %s millis before retrying.' %
                        (e, delay))
                    time.sleep(delay / 1000.0)
                    attempt += 1

        return f_retry
    return decorator
