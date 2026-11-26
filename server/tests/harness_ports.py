"""Loopback ports for the process harnesses, without the free_port() race (PERF2e).

A port picked by binding port 0 and closing that socket can be taken by another
process before the child binds it ("worker HTTP bind failed" in run
44857409138). So:

- a child that reports its bound address (the API's `bound_url`) binds port 0
  itself;
- a port that peers or the harness must know before launch is picked here, never
  twice in one process. When the child exits during startup because a process
  outside the cluster holds that port, the harness launches it again with a fresh
  port (`ATTEMPTS` in total). Any other exit fails, also when the child's log
  mentions a listener: see `lost_bind`.
- `listener` tells whether a child, and not another process, listens on its port.

No sleeps: a collision is recognised from the child's exit and from who holds the
port, then the next attempt starts at once.
"""
import errno
import os
from pathlib import Path
import re
import socket
import sys
import threading

ATTEMPTS = 5

# How an exit reports a failed bind. `nabedra-db serve` and the registered worker's HTTP
# listener print the same text for every bind error, EADDRNOTAVAIL too, so it proves nothing
# alone. The registered worker's node listener prints the io error.
BIND_FAILED = re.compile(r'(?m)(?:^|: "?)(?:worker HTTP bind failed|listener failed)"?$')
ADDRESS_IN_USE = re.compile(r'(?m)^nabedra-db: .*\bAddress already in use \(os error \d+\)$')

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


def port_taken(port, host='127.0.0.1'):
    """True while another socket holds the port. TIME_WAIT is not a collision: the
    children's listeners (tokio) set SO_REUSEADDR, and so does this probe."""
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, port))
        except OSError as error:
            return error.errno == errno.EADDRINUSE
        return False


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


def _readable(pids):
    """The processes whose sockets this user can see. Without `sudo`'s own process, which is in another
    network namespace than the worker it started."""
    return [pid for pid in pids if os.access(f'/proc/{pid}/fd', os.R_OK)]


def _sockets(pid, host, port):
    """{inode: (state, uid)} of the TCP sockets at a local IPv4 address, or at the wildcard address, in a
    process's network namespace."""
    def name(ip):
        return f'{int.from_bytes(socket.inet_aton(ip), sys.byteorder):08X}'

    hosts = {name(host), name('0.0.0.0'), '0' * 32, '0' * 16 + 'FFFF0000' + name(host)}
    found = {}
    for table in ('tcp', 'tcp6'):
        try:
            rows = Path(f'/proc/{pid}/net/{table}').read_text().splitlines()[1:]
        except OSError:
            continue
        for row in rows:
            fields = row.split()
            local, _, number = fields[1].rpartition(':')
            # Inode 0: no process has the socket any more (TIME_WAIT).
            if local in hosts and int(number, 16) == port and fields[9] != '0':
                found[fields[9]] = fields[3], int(fields[7])
    return found


def _owner(sockets, cluster):
    """Whose sockets they are: ('cluster', pids) if a process of the cluster has one, ('outside', pids) if
    another process does or another user's does (then without a pid), ('unknown', []) otherwise. A socket
    that is closing has no owner, so an unknown one may be the cluster's."""
    if not sockets:
        return 'none', []
    members = [pid for root in cluster for pid in _process_tree(root) if _socket_inodes(pid) & sockets.keys()]
    if members:
        return 'cluster', members
    pids = [int(process.name) for process in Path('/proc').glob('[0-9]*')
            if _socket_inodes(process.name) & sockets.keys()]
    if pids or any(uid != os.getuid() for _, uid in sockets.values()):
        return 'outside', pids
    return 'unknown', []


def holder(host, port, cluster=()):
    """Who has `host:port` now, as `_owner` says, or ('none', []). `cluster` has the pids of the cluster's
    processes. ('unknown', []) where /proc does not tell (not Linux)."""
    if not sys.platform.startswith('linux'):
        return 'unknown', []
    return _owner(_sockets('self', host, port), cluster)


def listener(pid, host, port, cluster=()):
    """Who listens on `host:port`: ('own', pids) when only `pid` or its descendants do, ('none', []) when
    nobody does, else as `_owner` says. None where /proc does not tell (not Linux)."""
    if not sys.platform.startswith('linux'):
        return None
    tree = _readable(_process_tree(pid))
    listening = {}
    for process in tree:
        listening.update((inode, found) for inode, found in _sockets(process, host, port).items() if found[0] == '0A')
    if not listening:
        return 'none', []
    owners = [process for process in tree if _socket_inodes(process) & listening.keys()]
    if listening.keys() <= set().union(*(_socket_inodes(process) for process in owners)):
        return 'own', owners
    return _owner({inode: found for inode, found in listening.items()
                   if not any(inode in _socket_inodes(process) for process in owners)}, cluster)


def split_address(address):
    host, port = address.rsplit(':', 1)
    return host, int(port)


def lost_bind(output, addresses, cluster=()):
    """Why a child that exited lost one of `addresses` ('host:port') to a process outside the cluster, or
    None. Its exit must report the failed bind, and EADDRINUSE must be confirmed: by that report, or by a
    process outside the cluster that has the address now. Never when a process of the cluster has it."""
    held = [(address, *holder(*split_address(address), cluster)) for address in addresses]
    if any(state == 'cluster' for _, state, _ in held):
        return None
    if ADDRESS_IN_USE.search(output):
        return 'its exit says the address is in use'
    if BIND_FAILED.search(output):
        for address, state, pids in held:
            host, port = split_address(address)
            if state == 'outside' and port_taken(port, host):
                return f'{address} is held by ' + (', '.join(f'pid {pid}' for pid in pids) or "another user's process")
    return None


def release_held():
    while _held:
        _held.pop().close()
