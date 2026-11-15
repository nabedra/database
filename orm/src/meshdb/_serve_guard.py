"""Private process-wide serve reservation. No child supervision or import I/O.

P03d retains a caller-created process receipt before starting any resources.
Finish requires its inactive lifetime, child exit proof and completed cleanup.
Process observation and recovery remain explicit caller operations outside locks.
"""
from __future__ import annotations

import threading

from ._callback_bridge import _CallbackBridge

_lock = threading.Lock()
_current = None


def _unavailable():
    return RuntimeError('Serve runtime unavailable')


class _Reservation:
    def __init__(self):
        self._active = True
        self._creating = False
        self._finishing = False
        self._bridge = None
        self._process = None

    def _attach_process(self, receipt):
        """Retain one unused receipt before any process or bridge construction."""
        with _lock:
            if (_current is not self or not self._active or self._creating or
                    self._finishing or self._bridge is not None or
                    self._process is not None or receipt._used or
                    receipt._reservation is not None):
                raise _unavailable()
            receipt._reservation = self
            self._process = receipt

    def _retain_bridge(self, bridge):
        # Called only after safe cleanup/alive fields exist, before any socket
        # or thread is created. Construction remains outside the module lock.
        with _lock:
            if _current is not self or not self._active or not self._creating:
                raise _unavailable()
            self._bridge = bridge

    def _create_bridge(self, callbacks, callback_token, authorize_token):
        """Create at most one bridge, retaining even a partially started owner.

        Inputs have already been validated by the caller. A failed attempt
        still owns this reservation; the caller must finish its cleanup path.
        """
        with _lock:
            if (_current is not self or not self._active or self._creating or
                    self._finishing or self._bridge is not None):
                raise _unavailable()
            self._creating = True
        try:
            return _CallbackBridge(callbacks, callback_token, authorize_token,
                                   _register_owner=self._retain_bridge)
        finally:
            with _lock:
                self._creating = False

    def _finish(self):
        """After child cleanup, close the original bridge with its finite budgets.

        Repeated/stale finishes do nothing. Calls during construction fail
        locally and leave ownership active. Cleanup errors also leave it active.
        Surviving original threads retain the finished reservation until a later
        acquisition observes their exit. No provider callback is cancelled.
        """
        with _lock:
            if _current is not self or not self._active or self._finishing:
                return
            if self._creating:
                raise _unavailable()
            self._finishing = True
            bridge = self._bridge
            process = self._process
        failed = False
        try:
            if process is not None and not process._ready():
                raise _unavailable()
            if bridge is not None:
                bridge.close()
        except BaseException:
            failed = True
        finally:
            with _lock:
                self._finishing = False
                if not failed:
                    self._active = False
        if failed:
            raise _unavailable() from None


def _reserve():
    """Atomically claim the only runtime, without creating external resources.

    Exit checks run outside the transition lock. Failure to observe owner exit
    rejects acquisition. Identity is checked again before replacing a finished
    reservation, so competing callers cannot both acquire it.
    """
    global _current
    while True:
        with _lock:
            previous = _current
            if previous is None:
                _current = _Reservation()
                return _current
            if previous._active:
                raise _unavailable()
            bridge = previous._bridge
        exited = False
        try:
            exited = bridge is None or not bridge.alive
        except BaseException:
            pass
        if not exited:
            raise _unavailable() from None
        with _lock:
            if _current is previous:
                _current = _Reservation()
                return _current
