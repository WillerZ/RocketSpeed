import time
import datetime

def log(severity, msg):
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime(
        '%Y-%m-%d %H:%M:%S:%f')
    print("[%s][%s]: %s" % (severity, timestamp, msg), flush=True)

def info(msg):
    log('INFO', msg)

def warn(msg):
    log('WARN', msg)

def error(msg):
    log('ERROR', msg)

def orchestrate(msg):
    log('RUN ', msg)

# TODO: these should be streams that can be passed to a process
# probably want to send output somewhere other than the console and
# capture output per test. This will make retrospective analysis
# easier.
def stdout():
    pass

def stderr():
    pass
