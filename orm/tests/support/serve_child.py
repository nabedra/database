#!/usr/bin/env python3
"""Independent executable wire peer. Records contain events and PID only."""
import json
import os
import signal
import socket
import struct
import sys

assert sys.argv[1:3] == ['api', '--config'] and len(sys.argv) == 4
assert os.path.isabs(sys.argv[3])
with open(sys.argv[3]) as f:
    config = json.load(f)
mode = config['mode']
record = config['record']
def event(name):
    with open(record, 'a') as f:
        f.write(json.dumps({'event': name, 'pid': os.getpid()}) + '\n')
def caught(signum, frame):
    event('signal_' + str(signum))
    if not mode.startswith('held'):
        sys.exit(0)
signal.signal(signal.SIGINT, caught)
signal.signal(signal.SIGTERM, caught)
s = socket.socket(fileno=int(os.environ['NABEDRA_SUPERVISOR_FD']))
def receive():
    def exact(n):
        data = b''
        while len(data) < n:
            chunk = s.recv(n - len(data))
            if not chunk:
                sys.exit(0)
            data += chunk
        return data
    return json.loads(exact(struct.unpack('!I', exact(4))[0]))
def send(obj):
    data = json.dumps(obj).encode()
    s.sendall(struct.pack('!I', len(data)) + data)
event('spawn')
init = receive()
assert init['type'] == 'init'
event('init')
if mode == 'flood':
    for _ in range(320):
        os.write(1, b'x' * 8192)
        os.write(2, b'y' * 8192)
    event('flooded')
if mode in ('partial', 'truncated', 'malformed', 'fatal'):
    if mode == 'fatal':
        send({'version': 1, 'type': 'fatal', 'code': 'bind_failed'})
    elif mode == 'malformed':
        data = b'{"version":1,"version":1}'
        s.sendall(struct.pack('!I', len(data)) + data)
    else:
        s.sendall(b'\x00\x00')
    if mode != 'partial':
        sys.exit(7)
    receive()
    sys.exit(0)
listener = socket.socket()
listener.bind(('127.0.0.1', 0))
listener.listen()
url = 'http://127.0.0.1:' + str(listener.getsockname()[1])
cluster = '00000000-0000-0000-0000-000000000001'
prepared = dict(version=1, type='prepared', cluster_id=cluster, bound_url=url,
                authorize_url=(url + '/private/v1/authorize' if init['bridge'] else None))
if mode == 'bad_url':
    prepared['authorize_url'] = url + '/private/v1/authorize?'
send(prepared)
assert receive() == {'version': 1, 'type': 'activate'}
event('activate')
send(dict(version=1, type='ready', cluster_id=cluster,
          bound_url=url if mode != 'mismatch' else url + '/wrong'))
if mode in ('eof', 'replaced', 'stop_interrupt'):
    s.close()
    sys.exit(0)
if mode == 'exit':
    sys.exit(9)
if mode == 'forward_race':
    # EOF initiates failure cleanup, but only a forwarded signal releases us.
    s.close()
    while True:
        signal.pause()
if mode == 'lifetime_partial':
    s.sendall(b'\x00')
if receive() == {'version': 1, 'type': 'shutdown'}:
    event('shutdown')
if mode.startswith('held'):
    # A held gate never opens in this fixture. Only the owner's kill ends it.
    while True:
        signal.pause()
