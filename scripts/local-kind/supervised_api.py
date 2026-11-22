"""Drive the native supervisor protocol in the separate API Pod."""
import json
import os
import signal
import socket
import struct
import subprocess


def send(channel, **message):
    body = json.dumps(dict(version=1, **message)).encode()
    channel.sendall(struct.pack('>I', len(body)) + body)


def receive(stream, message_type):
    header = stream.read(4)
    assert len(header) == 4, f'API exited before {message_type}'
    size, = struct.unpack('>I', header)
    assert 0 < size <= 16384, size
    message = json.loads(stream.read(size))
    assert message['type'] == message_type, message
    print(json.dumps(message), flush=True)


def main():
    parent, child = socket.socketpair()
    with parent, child:
        api = subprocess.Popen(['nabedra-db', 'api', '--config', '/config/api.json'],
                               env=dict(os.environ, NABEDRA_SUPERVISOR_FD=str(child.fileno())),
                               pass_fds=(child.fileno(),))
        child.close()

        def shutdown(_signum, _frame):
            send(parent, type='shutdown')

        signal.signal(signal.SIGTERM, shutdown)
        try:
            with parent.makefile('rb') as stream:
                send(parent, type='init', name='Live3', provisioning='external',
                     initial_policy=dict(mode='fixed', desired=1, min_nodes=1, max_nodes=1))
                receive(stream, 'prepared')
                send(parent, type='activate')
                receive(stream, 'ready')
            return api.wait()
        finally:
            if api.poll() is None:
                api.kill()
                api.wait(timeout=5)


if __name__ == '__main__':
    raise SystemExit(main())
