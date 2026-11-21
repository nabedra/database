"""Loopback ports for the process harnesses, without the free_port() race (PERF2e).

A port picked by binding port 0 and closing that socket can be taken by another
process before the child binds it ("worker HTTP bind failed" in run
44857409138). So:

- a child that reports its bound address (the API's `bound_url`) binds port 0
  itself;
- a port that peers or the harness must know before launch is picked here, and
  when the child exits during startup because another socket holds that port,
  the harness launches it again with a fresh port (`ATTEMPTS` in total).

No sleeps: a collision is recognised from the child's startup error or by
probing the port, then the next attempt starts at once.
"""
import errno
import os
import socket

ATTEMPTS = 5

# Startup errors of a lost bind: `nabedra-db serve` ("listener failed"), the
# registered worker's HTTP listener ("worker HTTP bind failed") and node
# listener (an io error: "Address already in use").
BIND_FAILURES = ('listener failed', 'bind failed', 'Address already in use', 'address already in use')

# Test hook: NABEDRA_TEST_PORT_COLLISIONS is a comma-separated list of pick
# numbers (1 is this process's first pick_port() call). Each of those ports is
# held by a listening socket, so the launch on it collides and exercises the
# retry.
_held = []
_picks = 0


def pick_port():
    global _picks
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    _picks += 1
    collisions = os.environ.get('NABEDRA_TEST_PORT_COLLISIONS', '')
    if str(_picks) in collisions.split(','):
        holder = socket.socket()
        holder.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        holder.bind(('127.0.0.1', port))
        holder.listen()
        _held.append(holder)
    return port


def port_taken(port):
    """True while another socket holds the port. TIME_WAIT is not a collision: the
    children's listeners (tokio) set SO_REUSEADDR, and so does this probe."""
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(('127.0.0.1', port))
        except OSError as error:
            return error.errno == errno.EADDRINUSE
        return False


def lost_bind(log_text, ports):
    """Whether a child that exited during startup lost one of `ports` to another process."""
    return any(marker in log_text for marker in BIND_FAILURES) or any(port_taken(port) for port in ports)


def release_held():
    while _held:
        _held.pop().close()
