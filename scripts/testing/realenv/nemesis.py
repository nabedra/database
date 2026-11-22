#!/usr/bin/env python3
"""Inject faults into processes and Docker containers owned by a test."""

from contextlib import contextmanager
from dataclasses import dataclass
import math
import signal
import subprocess
from typing import Callable

from .history import HistoryWriter


# nicolaka/netshoot v0.16 multi-platform image index (linux/amd64 and linux/arm64), with iproute2.
NETEM_IMAGE = "ghcr.io/nicolaka/netshoot@sha256:b09d9b21381f47a79b3cbcb30da25266dc17186ea00ae65e99fdc51396f48e70"


@dataclass(frozen=True)
class Container:
    id: str


@dataclass(frozen=True)
class Process:
    name: str
    child: subprocess.Popen


class Nemesis:
    def __init__(self, history: HistoryWriter, *, run=subprocess.run, process=0):
        self.history = history
        self.run = run
        self.process = process
        self.undo: dict[tuple, Callable[[], None]] = {}
        self._sidecar_ready = False

    def command(self, *args, timeout=60):
        return self.run(list(args), check=True, timeout=timeout)

    @contextmanager
    def fault(self, action, node=None, *, partial=False, **details):
        fields = dict(process=self.process, f="nemesis", node=node)
        value = dict(action=action, **details)
        self.history.record("invoke", value=value, **fields)
        try:
            yield
        except BaseException as error:
            definite = isinstance(error, (OSError, subprocess.CalledProcessError)) and not partial
            outcome = "fail" if definite else "info"
            self.history.record(outcome, value=dict(value, error=type(error).__name__), **fields)
            raise
        else:
            self.history.record("ok", value=value, **fields)

    def _signal(self, target, sig):
        if isinstance(target, Process):
            if target.child.poll() is not None:
                if sig == signal.SIGCONT:
                    return
                raise ProcessLookupError(target.name)
            target.child.send_signal(sig)
        else:
            self.command("docker", "kill", "--signal", sig.name, target.id)

    def _send(self, action, target, sig):
        node = target.name if isinstance(target, Process) else target.id
        with self.fault(action, node, signal=sig.name):
            self._signal(target, sig)

    def kill(self, target):
        self._send("kill", target, signal.SIGKILL)
        self.undo.pop(("stop", target), None)

    def stop(self, target):
        key = ("stop", target)
        self.undo[key] = lambda: self.cont(target)
        try:
            self._send("stop", target, signal.SIGSTOP)
        except (OSError, subprocess.CalledProcessError):
            self.undo.pop(key, None)
            raise

    def cont(self, target):
        self._send("cont", target, signal.SIGCONT)
        self.undo.pop(("stop", target), None)

    def disconnect(self, target: Container, network: str, *, connect_options=()):
        key = ("disconnect", target, network)
        if key in self.undo:
            raise ValueError(f"{target.id} already has a recorded disconnection from {network}")
        self.undo[key] = lambda: self.connect(target, network, options=connect_options)
        try:
            with self.fault("disconnect", target.id, network=network):
                self.command("docker", "network", "disconnect", network, target.id)
        except (OSError, subprocess.CalledProcessError):
            self.undo.pop(key, None)
            raise

    def connect(self, target: Container, network: str, *, options=()):
        with self.fault("connect", target.id, network=network):
            self.command("docker", "network", "connect", *options, network, target.id)
        self.undo.pop(("disconnect", target, network), None)

    def _tc(self, target, *args, **kwargs):
        # The first run may pull the sidecar image, which takes longer than a tc command.
        timeout = 60 if self._sidecar_ready else 600
        result = self.run(["docker", "run", "--rm", "--network", f"container:{target.id}",
                           "--cap-add", "NET_ADMIN", "--entrypoint", "tc", NETEM_IMAGE, *args],
                          timeout=timeout, **{"check": True, **kwargs})
        self._sidecar_ready = True
        return result

    def _has_netem(self, target, interface):
        shown = self._tc(target, "qdisc", "show", "dev", interface, check=False,
                         capture_output=True, text=True)
        return shown.returncode == 0 and "netem 90:" in (shown.stdout or "")

    def netem(self, target: Container, *, delay_ms=None, loss=None, duplicate=None, interface="eth0"):
        options = []
        for name, value, unit, maximum in (
            ("delay", delay_ms, "ms", math.inf),
            ("loss", loss, "%", 100),
            ("duplicate", duplicate, "%", 100),
        ):
            if value is not None:
                if not math.isfinite(value) or not 0 <= value <= maximum:
                    raise ValueError(f"invalid netem {name}: {value}")
                options.extend((name, f"{value:g}{unit}"))
        if not options:
            raise ValueError("specify at least one netem impairment")
        key = ("netem", target, interface)
        if key in self.undo:
            raise ValueError(f"netem already applied on {target.id}/{interface}")
        self.undo[key] = lambda: self.clear_netem(target, interface=interface)
        try:
            with self.fault("netem", target.id, interface=interface, options=options):
                self._tc(target, "qdisc", "add", "dev", interface, "root", "handle", "90:", "netem", *options)
        except (OSError, subprocess.CalledProcessError):
            self.undo.pop(key, None)
            raise

    def clear_netem(self, target: Container, *, interface="eth0"):
        with self.fault("clear_netem", target.id, interface=interface):
            try:
                self._tc(target, "qdisc", "del", "dev", interface, "root", "handle", "90:")
            except subprocess.CalledProcessError:
                # A network disconnect deletes the interface and its qdisc, and a
                # reconnect creates it without one. Only a remaining qdisc is a failure.
                if self._has_netem(target, interface):
                    raise
        self.undo.pop(("netem", target, interface), None)

    def heal_all(self):
        errors = []
        for heal in reversed(list(self.undo.values())):
            try:
                heal()
            except Exception as error:
                errors.append(error)
        # Keep fault pairs sequential on the nemesis process.
        with self.fault("heal_all", failed=len(errors)):
            if errors:
                raise ExceptionGroup("nemesis cleanup failed", errors)
