"""Loopback ports for the process harnesses, without the free_port() race (PERF2e).

A port picked by binding port 0 and closing that socket can be taken by another
process before the child binds it ("worker HTTP bind failed" in run
44857409138). So:

- a child that reports its bound address (the API's `bound_url`) binds port 0
  itself;
- a port that peers or the harness must know before launch is picked here, never
  twice in one process. When the child exits during startup because another
  socket holds that port, the harness launches it again with a fresh port
  (`ATTEMPTS` in total).
- `listener` tells whether a child, and not another process, listens on its port.

No sleeps: a collision is recognised from the child's startup error or by
probing the port, then the next attempt starts at once.
"""
import errno
import os
from pathlib import Path
import socket
import sys
import threading

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

_picked = set()
_lock = threading.Lock()


def _unused_port():
    """A port this process has not been given before. The kernel can answer a bind to port 0 with the
    port of a closed socket again, so a repeated answer stays bound while it is asked again."""
    repeats = []
    try:
        while True:
            sock = socket.socket()
            repeats.append(sock)
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
            if port not in _picked:
                _picked.add(port)
                return port
            print(f'harness_ports: port {port} was already picked; picking another', file=sys.stderr, flush=True)
    finally:
        for sock in repeats:
            sock.close()


def pick_port():
    global _picks
    with _lock:
        port = _unused_port()
        _picks += 1
        pick = _picks
    print(f'harness_ports: pick {pick} is port {port}', file=sys.stderr, flush=True)
    collisions = os.environ.get('NABEDRA_TEST_PORT_COLLISIONS', '')
    if str(pick) in collisions.split(','):
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


def _process_tree(pid):
    """The process and its descendants."""
    children = {}
    for stat in Path('/proc').glob('[0-9]*/stat'):
        try:
            parent = int(stat.read_text().rsplit(')', 1)[1].split()[1])
        except (OSError, IndexError):
            continue
        children.setdefault(parent, []).append(int(stat.parent.name))
    tree, pending = [], [pid]
    while pending:
        process = pending.pop()
        tree.append(process)
        pending.extend(children.get(process, ()))
    return tree


def _socket_inodes(pid):
    """The sockets a process has open, or none when it exited or belongs to another user."""
    inodes = set()
    try:
        for fd in Path(f'/proc/{pid}/fd').iterdir():
            try:
                target = os.readlink(fd)
            except OSError:
                continue
            if target.startswith('socket:['):
                inodes.add(target[8:-1])
    except OSError:
        pass
    return inodes


def _listening_inodes(pid, host, port):
    """The sockets listening on an IPv4 address in a process's network namespace."""
    addresses = {f'{int.from_bytes(socket.inet_aton(ip), sys.byteorder):08X}:{port:04X}' for ip in (host, '0.0.0.0')}
    try:
        rows = Path(f'/proc/{pid}/net/tcp').read_text().splitlines()[1:]
    except OSError:
        return set()
    fields = (row.split() for row in rows)
    return {row[9] for row in fields if row[1] in addresses and row[3] == '0A'}


def listener(pid, host, port):
    """Who listens on `host:port`: ('own', pids) when only `pid` or its descendants do, ('other', pids) when
    another process does (its pid is unknown if it belongs to another user), ('none', []) when nobody does.
    None where /proc does not tell (not Linux)."""
    if not sys.platform.startswith('linux'):
        return None
    tree = _process_tree(pid)
    listening = set().union(*(_listening_inodes(process, host, port) for process in tree))
    if not listening:
        return 'none', []
    owners = [process for process in tree if _socket_inodes(process) & listening]
    if listening <= set().union(*(_socket_inodes(process) for process in owners)):
        return 'own', owners
    return 'other', [int(process.name) for process in Path('/proc').glob('[0-9]*')
                     if int(process.name) not in owners and _socket_inodes(process.name) & listening]


def release_held():
    while _held:
        _held.pop().close()
