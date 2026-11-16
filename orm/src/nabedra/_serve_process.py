"""Private POSIX API-child owner. No public serve or import-time I/O."""
from __future__ import annotations

import base64
import json
import math
import os
import re
import selectors
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
from urllib.parse import urlsplit
from uuid import UUID

from ._binary import resolve_server_binary
from .errors import NabedraError

_STARTUP_SECONDS = 30.0
_FRAME_SECONDS = 2.0
_EXIT_SECONDS = 10.0
_DIAGNOSTIC_BYTES = 1024 * 1024
_LIMIT = 16 * 1024
_FATAL = {'invalid_config', 'incompatible_cluster', 'storage_unavailable',
          'bind_failed', 'protocol_error', 'startup_failed'}


def _failure(code='protocol_error'):
    return NabedraError('Nabedra Database API process failed.', code=code)


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise _failure()
        result[key] = value
    return result


def _float(value):
    number = float(value)
    if not math.isfinite(number):
        raise _failure()
    return number


def _decode(data):
    try:
        value = json.loads(data.decode('utf-8'), object_pairs_hook=_object, parse_float=_float,
                           parse_constant=lambda _: (_ for _ in ()).throw(_failure()))
        if type(value) is not dict:
            raise _failure()
        return value
    except (ValueError, UnicodeError, RecursionError):
        raise _failure() from None


def _fields(value, names):
    if type(value) is not dict or set(value) != set(names.split()):
        raise _failure()


def _url(value, private=None):
    if type(value) is not str or not value:
        raise _failure()
    try:
        if len(value.encode('utf-8')) > 4096:
            raise _failure()
        u = urlsplit(value)
        if (u.scheme not in ('http', 'https') or not u.hostname or
                u.username is not None or u.password is not None or u.fragment or
                any(ord(c) <= 32 or ord(c) == 127 for c in value) or '\\' in value or
                u.port == 0):
            raise _failure()
        if private is not None and (u.scheme != 'http' or
                u.netloc != '127.0.0.1:' + str(u.port) or
                u.port is None or value != f'http://127.0.0.1:{u.port}{private}'):
            raise _failure()
    except (ValueError, UnicodeError):
        raise _failure() from None


def _initial(value):
    _fields(value, 'version type name initial_policy provisioning bridge')
    if type(value['version']) is not int or value['version'] != 1 or value['type'] != 'init':
        raise _failure()
    if type(value['name']) is not str or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,63}', value['name']):
        raise _failure()
    p = value['initial_policy']
    _fields(p, 'mode desired min_nodes max_nodes')
    if any(type(p[k]) is not int or not 0 <= p[k] <= 1024
           for k in ('desired', 'min_nodes', 'max_nodes')):
        raise _failure()
    if not ((p['mode'] == 'fixed' and p['min_nodes'] == p['desired'] == p['max_nodes']) or
            (p['mode'] == 'autoscale' and 1 <= p['min_nodes'] == p['desired'] <= p['max_nodes'])):
        raise _failure()
    if value['provisioning'] == 'external':
        if value['bridge'] is not None or p['mode'] != 'fixed':
            raise _failure()
    elif value['provisioning'] == 'managed':
        b = value['bridge']
        _fields(b, 'url callback_token authorize_token')
        _url(b['url'], '/private/v1/callback')
        for k in ('callback_token', 'authorize_token'):
            token = b[k]
            if type(token) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{43}', token):
                raise _failure()
            if base64.urlsafe_b64encode(base64.urlsafe_b64decode(token + '=')).decode().rstrip('=') != token:
                raise _failure()
        if b['callback_token'] == b['authorize_token']:
            raise _failure()
    else:
        raise _failure()


def _message(value, expected, managed):
    if type(value.get('version')) is not int or value['version'] != 1:
        raise _failure()
    if value.get('type') == 'fatal':
        _fields(value, 'version type code')
        if type(value['code']) is not str or value['code'] not in _FATAL:
            raise _failure()
        raise _failure(value['code'])
    _fields(value, 'version type cluster_id bound_url' +
            (' authorize_url' if expected == 'prepared' else ''))
    if value['type'] != expected or type(value['cluster_id']) is not str:
        raise _failure()
    try:
        if str(UUID(value['cluster_id'])) != value['cluster_id']:
            raise _failure()
    except ValueError:
        raise _failure() from None
    _url(value['bound_url'])
    if expected == 'prepared':
        if managed:
            _url(value['authorize_url'], '/private/v1/authorize')
        elif value['authorize_url'] is not None:
            raise _failure()


class _Interrupted(Exception):
    pass


class _Owner:
    def __init__(self):
        self.child = None
        self.channel = None
        self.peer = None
        self.wake = None
        self.writer = None
        self.selector = None
        self._diagnostics = [bytearray(), bytearray()]
        self.signals = []
        self.saved = {}
        self.installed = None
        self.buffer = bytearray()
        self.frame_deadline = None
        self.eof = False

    def install(self):
        self.selector = selectors.DefaultSelector()
        self.wake, self.writer = socket.socketpair()
        self.wake.setblocking(False)
        self.writer.setblocking(False)
        self.selector.register(self.wake, selectors.EVENT_READ, 'wake')
        if threading.current_thread() is threading.main_thread():
            def handler(signum, frame):
                if len(self.signals) < 2:
                    self.signals.append(signum)
                try:
                    self.writer.send(b'x')
                except OSError:
                    pass
            self.installed = handler
            for signum in (signal.SIGINT, signal.SIGTERM):
                self.saved[signum] = signal.getsignal(signum)
                signal.signal(signum, handler)

    def spawn(self, config, *, cold_start=False, migrate_catalog=False, binary=None):
        self.channel, self.peer = socket.socketpair()
        self.channel.setblocking(False)
        env = os.environ.copy()
        env.pop('NABEDRA_SUPERVISOR_FD', None)
        env['NABEDRA_SUPERVISOR_FD'] = str(self.peer.fileno())
        self.child = subprocess.Popen(
            [binary if binary is not None else resolve_server_binary(), 'api', '--config', os.path.abspath(config)]
            + (['--cold-start'] if cold_start else []) + (['--migrate-catalog'] if migrate_catalog else []),
            env=env, pass_fds=(self.peer.fileno(),), close_fds=True,
            start_new_session=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.peer.close()
        self.peer = None
        self.selector.register(self.channel, selectors.EVENT_READ, 'channel')
        for index, pipe in enumerate((self.child.stdout, self.child.stderr)):
            os.set_blocking(pipe.fileno(), False)
            self.selector.register(pipe, selectors.EVENT_READ, index)

    def pump(self, deadline, *, stopping=False):
        if self.signals and not stopping:
            raise _Interrupted()
        now = time.monotonic()
        if deadline is not None and now >= deadline:
            if stopping:
                return
            raise _failure('startup_timeout')
        timeout = .05 if deadline is None else max(0, min(.05, deadline - now))
        for key, _ in self.selector.select(timeout):
            if key.data == 'wake':
                self.wake.recv(4096)
            elif key.data == 'channel':
                # At most one maximum frame plus its header is retained.
                chunk = self.channel.recv(max(1, _LIMIT + 4 - len(self.buffer)))
                if not chunk:
                    self.eof = True
                    self.selector.unregister(self.channel)
                elif not stopping:
                    if not self.buffer:
                        self.frame_deadline = time.monotonic() + _FRAME_SECONDS
                    self.buffer.extend(chunk)
            else:
                chunk = os.read(key.fd, 65536)
                if not chunk:
                    self.selector.unregister(key.fileobj)
                else:
                    sink = self._diagnostics[key.data]
                    sink.extend(chunk[:max(0, _DIAGNOSTIC_BYTES - len(sink))])
        if self.signals and not stopping:
            raise _Interrupted()

    def send(self, value, deadline, *, stopping=False):
        data = json.dumps(value, allow_nan=False, separators=(',', ':')).encode('utf-8')
        if not 0 < len(data) <= _LIMIT:
            raise _failure()
        data = memoryview(struct.pack('!I', len(data)) + data)
        while data:
            if self.signals and not stopping:
                raise _Interrupted()
            if time.monotonic() >= deadline:
                raise _failure('startup_timeout')
            try:
                count = self.channel.send(data)
                if not count:
                    raise _failure()
                data = data[count:]
            except BlockingIOError:
                self.pump(deadline, stopping=stopping)

    def receive(self, deadline):
        while True:
            if self.signals:
                raise _Interrupted()
            if deadline is not None and time.monotonic() >= deadline:
                raise _failure('startup_timeout')
            if len(self.buffer) >= 4:
                length = struct.unpack('!I', self.buffer[:4])[0]
                if not 0 < length <= _LIMIT:
                    raise _failure()
                if len(self.buffer) >= length + 4:
                    data = bytes(self.buffer[4:4 + length])
                    del self.buffer[:4 + length]
                    self.frame_deadline = (time.monotonic() + _FRAME_SECONDS if self.buffer else None)
                    return _decode(data)
            if self.eof:
                raise _failure()
            effective = deadline
            if effective is None:
                effective = self.frame_deadline
            if effective is not None and time.monotonic() >= effective:
                raise _failure()
            if self.child.poll() is not None:
                raise _failure('child_exited')
            self.pump(effective)

    def announce(self, url, deadline, *, event="nabedra.serve.ready"):
        data = memoryview((json.dumps({'event': event, 'bound_url': url},
                                     separators=(',', ':')) + '\n').encode())
        fd = sys.stdout.fileno()
        blocking = os.get_blocking(fd)
        try:
            os.set_blocking(fd, False)
            end = min(deadline, time.monotonic() + _FRAME_SECONDS)
            while data:
                if self.signals:
                    raise _Interrupted()
                if time.monotonic() >= end:
                    raise _failure('announcement_failed')
                try:
                    count = os.write(fd, data)
                    if not count:
                        raise _failure('announcement_failed')
                    data = data[count:]
                except BlockingIOError:
                    self.pump(end)
        finally:
            os.set_blocking(fd, blocking)

    def shutdown(self):
        if self.child is None:
            return False
        deadline = time.monotonic() + _EXIT_SECONDS
        forwarded = False
        if self.child.poll() is None:
            try:
                self.send({'version': 1, 'type': 'shutdown'},
                          min(deadline, time.monotonic() + _FRAME_SECONDS), stopping=True)
            except Exception:
                pass
            if self.signals and self.child.poll() is None:
                self.child.send_signal(self.signals[0])
                forwarded = True
        while self.child.poll() is None:
            if self.signals and not forwarded:
                self.child.send_signal(self.signals[0])
                forwarded = True
            if len(self.signals) > 1 or time.monotonic() >= deadline:
                # Popen owns the only reaper and checks returncode before kill.
                self.child.kill()
                self.child.wait(timeout=_EXIT_SECONDS)
                return True
            self.pump(deadline, stopping=True)
        self.child.wait(timeout=_EXIT_SECONDS)
        return False

    def reap(self):
        """Emergency cleanup of this Popen only; failed waits prove nothing."""
        if self.child is not None and self.child.returncode is None:
            if self.child.poll() is None:
                self.child.kill()
            self.child.wait(timeout=_EXIT_SECONDS)

    def close(self):
        failed = False
        for signum, saved in self.saved.items():
            try:
                if signal.getsignal(signum) is self.installed:
                    signal.signal(signum, saved)
            except BaseException:
                failed = True
        for resource in (self.selector, self.channel, self.peer, self.wake, self.writer,
                         self.child.stdout if self.child else None,
                         self.child.stderr if self.child else None):
            if resource is not None:
                try:
                    resource.close()
                except BaseException:
                    failed = True
        self._diagnostics.clear()
        if failed:
            raise _failure('shutdown_failed') from None


class _ExitReceipt:
    """One private lifetime, retained before startup, with independent exit proof.

    State inspection never performs OS operations. _retry is an explicit,
    serialized caller operation after run_process has left its finally block.
    It may also consume an unused receipt when construction failed before run.
    """
    def __init__(self):
        self._owner = None
        self._reservation = None
        self._used = False
        self._active = False
        self._exited = False
        self._cleaned = False

    def _begin(self):
        if self._used:
            raise _failure('startup_failed')
        self._used = True
        self._active = True
        # _Owner initialization has no resource side effects.
        self._owner = _Owner()
        return self._owner

    def _observe(self):
        owner = self._owner
        if owner is None or owner.child is None:
            self._exited = True
        elif owner.child.returncode is not None or owner.child.poll() is not None:
            self._exited = True

    def _ready(self):
        return self._used and not self._active and self._exited and self._cleaned

    def _retry(self):
        if self._active:
            raise RuntimeError('Serve runtime unavailable')
        self._used = True
        failed = False
        try:
            if self._owner is not None and not self._exited:
                self._owner.reap()
            self._observe()
        except BaseException:
            failed = True
        try:
            if self._owner is not None:
                self._owner.close()
            self._cleaned = True
        except BaseException:
            failed = True
        if failed or not self._ready():
            raise _failure('shutdown_failed') from None


def run_process(config_path, startup, *, prepared, stop, _receipt=None, cold_start=False, migrate_catalog=False):
    """Block owning one child; hooks install bridge admission and close it.

    `prepared(record)` runs before activate. `stop()` runs once on every exit,
    including failed spawn. Both are short internal bridge operations, never
    provider callbacks. Inputs are validated path and constructed init record.
    A private optional receipt retains the owner across exceptional cleanup.
    """
    receipt = _receipt if _receipt is not None else _ExitReceipt()
    if receipt._used:
        raise _failure('startup_failed')
    owner = None
    failure = None
    interrupted = None
    try:
        owner = receipt._begin()
        owner.install()
        deadline = time.monotonic() + _STARTUP_SECONDS
        _initial(startup)
        if cold_start or migrate_catalog:
            owner.spawn(config_path, cold_start=cold_start, migrate_catalog=migrate_catalog)
        else:
            owner.spawn(config_path)
        owner.send(startup, deadline)
        first = owner.receive(deadline)
        managed = startup['provisioning'] == 'managed'
        _message(first, 'prepared', managed)
        prepared(first.copy())
        owner.send({'version': 1, 'type': 'activate'}, deadline)
        ready = owner.receive(deadline)
        _message(ready, 'ready', managed)
        if any(ready[k] != first[k] for k in ('cluster_id', 'bound_url')):
            raise _failure()
        owner.announce(ready['bound_url'], deadline)
        # No child messages are permitted after ready, apart from fatal failure.
        unexpected = owner.receive(None)
        _message(unexpected, 'fatal', managed)
        raise _failure()
    except _Interrupted:
        pass
    except NabedraError as error:
        failure = _failure(error.code if error.code in _FATAL | {
            'startup_timeout', 'child_exited', 'announcement_failed'} else 'startup_failed')
    except (KeyboardInterrupt, SystemExit) as error:
        interrupted = error
    except Exception:
        failure = _failure('startup_failed')
    finally:
        try:
            try:
                stop()
            except (KeyboardInterrupt, SystemExit) as error:
                interrupted = error
            except Exception:
                failure = _failure('shutdown_failed')
            if owner is not None and owner.shutdown():
                failure = _failure('forced_termination')
            elif owner is not None and owner.child and owner.child.returncode not in (
                    0, -owner.signals[0] if owner.signals else 0):
                failure = failure or _failure('child_exited')
        except BaseException as error:
            if isinstance(error, (KeyboardInterrupt, SystemExit)):
                interrupted = error
            failure = _failure('shutdown_failed')
            try:
                if owner is not None:
                    owner.reap()
            except BaseException:
                pass
        finally:
            try:
                receipt._observe()
            except BaseException:
                failure = _failure('shutdown_failed')
            try:
                if owner is not None:
                    owner.close()
                receipt._cleaned = True
            except BaseException:
                failure = _failure('shutdown_failed')
            if not (receipt._exited and receipt._cleaned):
                failure = _failure('shutdown_failed')
    try:
        if interrupted is not None:
            raise interrupted from None
        if owner is not None and owner.signals:
            signum = owner.signals[0]
            saved = owner.saved.get(signum)
            if callable(saved):
                saved(signum, None)
            elif saved == signal.SIG_DFL and signum == signal.SIGINT:
                raise KeyboardInterrupt
        if failure:
            raise failure from None
    finally:
        receipt._active = False
