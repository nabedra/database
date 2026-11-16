"""Private finite callback ownership. No listener or process-global runtime guard."""
from __future__ import annotations

import copy
from dataclasses import dataclass
import json
import threading
import time

from ._callback_protocol import (
    _Command, _CallbackProtocolError, _authorize, _bounded_json, _decode_command, _encode_result,
)


@dataclass(eq=False, repr=False)
class _Call:
    command: _Command
    deadline: float
    key: tuple | None = None
    identity: str | None = None
    entered: bool = False
    response: tuple[int, bytes] | None = None


def _failure(call, status, code):
    # Inventory failure never supplies a usable observation of external state.
    outcome = ('unknown' if code == 'callback_in_progress' or
               (call.entered and call.command.value['action'] != 'inventory') else 'not_invoked')
    return status, _bounded_json(dict(
        version=1, invocation_id=call.command.value['invocation_id'],
        error=dict(code=code, outcome=outcome)))


class _CallbackPool:
    """Accept validated _Commands and a validated (start, stop, list) tuple.

    submit returns an opaque observation handle; wait returns (status, body).
    Admission requires activate(cluster_id, exact_authorize_url). stop_admission
    is terminal. close joins the original threads for at most two seconds total.
    alive reports whether any original thread still lives. A later runtime guard
    must retain this owner while alive, including after close returns.
    """

    def __init__(self, callbacks, token, *, _observation_timeout=30.0, _register_owner=None):
        self._callbacks = callbacks
        self.__token = token
        self._observation_timeout = _observation_timeout
        self._condition = threading.Condition()
        self._cluster = None
        self._url = None
        self._stopping = False
        self._active = {}
        # Each mailbox remains occupied until actual work and validation finish.
        self._mailboxes = [None] * 4
        self._threads = []
        if _register_owner is not None:
            _register_owner(self)
        failed = False
        try:
            for index in range(4):
                thread = threading.Thread(target=self._run, args=(index,),
                                          name=f'nabedra-callback-{index}', daemon=True)
                self._threads.append(thread)
                thread.start()
        except BaseException:
            failed = True
        if failed:
            self.close()
            raise RuntimeError('Callback pool startup failed') from None

    @property
    def alive(self):
        return any(thread.is_alive() for thread in self._threads)

    def activate(self, cluster_id, authorize_url):
        """Set prepared, already-validated lifetime binding exactly once."""
        with self._condition:
            if self._stopping or self._cluster is not None:
                raise RuntimeError('Callback pool activation rejected')
            self._cluster, self._url = cluster_id, authorize_url

    def submit(self, command):
        """Reserve an idle worker immediately; never enqueue behind busy work."""
        deadline = time.monotonic() + self._observation_timeout
        # Re-decode the immutable bytes that authorization will receive; the
        # caller's decoded dictionary may have changed since validation.
        # Snapshot outside the admission lock. JSON's
        # sorted keys ignore dict order and preserve bool/int/float distinctions.
        # Both copies/representation are bounded by the 1 MiB command source.
        snapshot = _decode_command(command.original_bytes)
        value = snapshot.value
        identity = json.dumps([value['target'], value['payload']], sort_keys=True,
                              ensure_ascii=False, separators=(',', ':'), allow_nan=False)
        key = (value['cluster_id'], value['action'],
               value['invocation_id'] if value['action'] == 'inventory' else value['intent_id'])
        call = _Call(snapshot, deadline, key, identity)
        with self._condition:
            if self._stopping or self._cluster is None:
                call.response = _failure(call, 503, 'bridge_not_active')
            elif value['cluster_id'] != self._cluster:
                call.response = _failure(call, 400, 'invalid_command')
            elif key in self._active:
                original = self._active[key]
                if identity != original.identity:
                    call.response = _failure(call, 409, 'identity_conflict')
                else:
                    call.response = _failure(call, 409, 'callback_in_progress')
            else:
                indices = (3,) if value['action'] == 'inventory' else range(3)
                index = next((i for i in indices if self._mailboxes[i] is None), None)
                if index is None:
                    call.response = _failure(call, 429, 'callback_capacity')
                else:
                    self._active[key] = call
                    self._mailboxes[index] = call
                    self._condition.notify_all()
        return call

    def wait(self, call):
        """Observe until the admission deadline; never transfer worker ownership."""
        with self._condition:
            while call.response is None:
                remaining = call.deadline - time.monotonic()
                if remaining <= 0:
                    call.response = _failure(call, 504, 'callback_timeout')
                    break
                self._condition.wait(remaining)
            return call.response

    def stop_admission(self):
        """Stop permanently, wake idle workers and resolve outstanding waiters."""
        with self._condition:
            self._stopping = True
            for call in self._active.values():
                if call.response is None:
                    expired = time.monotonic() >= call.deadline
                    call.response = _failure(call, 504 if expired else 503,
                                             'callback_timeout' if expired else 'bridge_not_active')
            self._condition.notify_all()

    def close(self):
        self.stop_admission()
        deadline = time.monotonic() + 2.0
        for thread in self._threads:
            if thread.ident is not None and thread is not threading.current_thread():
                thread.join(max(0, deadline-time.monotonic()))

    def _execute(self, call):
        value = call.command.value
        # Prepare the provider-owned copy before the fresh authorization read.
        argument = copy.deepcopy(value['payload']) if value['action'] == 'start' else None
        try:
            _authorize(self._url, self.__token, call.command.original_bytes,
                       invocation_id=value['invocation_id'], deadline=call.deadline)
        except _CallbackProtocolError as exc:
            if exc.status_code is not None and exc.code != 'authorization_denied':
                return _failure(call, exc.status_code, exc.code)
            return _failure(call, 503, 'ownership_unavailable')
        except BaseException:
            return _failure(call, 503, 'ownership_unavailable')
        with self._condition:
            if call.response is not None:
                return call.response
            if time.monotonic() >= call.deadline:
                return _failure(call, 504, 'callback_timeout')
            if self._stopping:
                return _failure(call, 503, 'bridge_not_active')
            call.entered = True
        try:
            if value['action'] == 'start':
                result = self._callbacks[0](argument)
            elif value['action'] == 'stop':
                result = self._callbacks[1](value['payload']['resource_id'])
            else:
                result = self._callbacks[2]()
        except BaseException:
            return _failure(call, 502, 'callback_failed')
        try:
            return 200, _encode_result(call.command, result)
        except _CallbackProtocolError as exc:
            if exc.code == 'limit_exceeded':
                return _failure(call, 413, 'limit_exceeded')
            return _failure(call, 502, 'invalid_callback_result')
        except BaseException:
            return _failure(call, 502, 'invalid_callback_result')

    def _run(self, index):
        while True:
            with self._condition:
                while self._mailboxes[index] is None and not self._stopping:
                    self._condition.wait()
                call = self._mailboxes[index]
                if call is None:
                    return
            response = self._execute(call)
            with self._condition:
                if call.response is None:
                    call.response = (_failure(call, 504, 'callback_timeout')
                                     if time.monotonic() >= call.deadline else response)
                del self._active[call.key]
                self._mailboxes[index] = None
                del call, response
                self._condition.notify_all()
                if self._stopping:
                    return
