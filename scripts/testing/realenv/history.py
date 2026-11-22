#!/usr/bin/env python3
"""Read and write nabedra-check JSON-lines histories."""

from dataclasses import asdict, dataclass
import json
from pathlib import Path
import re
from threading import Lock
import time
from typing import Iterable, Iterator

NEMESIS = 2**64 - 1


@dataclass(frozen=True)
class Event:
    index: int
    type: str
    process: int
    f: str
    key: str | None = None
    value: object = None
    request_id: str | None = None
    node: str | None = None
    time_ns: int | None = None

    def __post_init__(self):
        for name in ("index", "process", "time_ns"):
            value = getattr(self, name)
            if name == "time_ns" and value is None:
                continue
            if type(value) is not int or not 0 <= value < 2**64:
                raise ValueError(f"{name} must be an unsigned 64-bit integer")
        if self.type not in ("invoke", "ok", "fail", "info"):
            raise ValueError(f"invalid event type {self.type!r}")
        if not isinstance(self.f, str) or not self.f:
            raise ValueError("event function must be a nonempty string")
        for name in ("key", "request_id", "node"):
            value = getattr(self, name)
            if value is not None and not isinstance(value, str):
                raise ValueError(f"{name} must be a string or null")

    @property
    def is_nemesis(self) -> bool:
        return self.process == NEMESIS or self.f == "nemesis"


class HistoryWriter:
    def __init__(self, path: Path):
        self.path = path
        self._lock = Lock()
        self._file = None
        self._index = 0

    def __enter__(self):
        self._file = self.path.open("w", encoding="utf-8")
        self._index = 0
        return self

    def __exit__(self, *_):
        self._file.close()
        self._file = None

    def record(self, type: str, process: int, f: str, *, time_ns: int | None = None, **fields) -> Event:
        with self._lock:
            if self._file is None:
                raise RuntimeError("history writer must be open")
            if time_ns is None:
                time_ns = time.monotonic_ns()
            event = Event(self._index, type, process, f, time_ns=time_ns, **fields)
            self._file.write(json.dumps(asdict(event), allow_nan=False, separators=(",", ":")) + "\n")
            self._file.flush()
            self._index += 1
            return event


def reject_constant(value):
    raise ValueError(f"invalid JSON constant {value}")


def read_jsonl(lines: Iterable[str]) -> Iterator[Event]:
    position = 0
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            fields = json.loads(line, parse_constant=reject_constant)
            if not isinstance(fields, dict):
                raise ValueError("expected an event object")
            if fields.get("index") in (None, NEMESIS):
                fields["index"] = position
            process = fields.get("process")
            if process == "nemesis":
                fields["process"] = NEMESIS
            elif isinstance(process, str) and re.fullmatch(r"\+?[0-9]+", process):
                fields["process"] = int(process)
            if fields.get("type") == "unknown":
                fields["type"] = "info"
            if type(fields.get("key")) in (int, float):
                fields["key"] = str(fields["key"])
            event = Event(**fields)
        except (TypeError, ValueError) as error:
            raise ValueError(f"history line {line_number}: {error}") from error
        yield event
        position += 1


@dataclass(frozen=True)
class Operation:
    invoke: Event
    completion: Event | None


def pair_operations(events: Iterable[Event]) -> list[Operation]:
    """Pair events by process in invocation order, retaining pending invokes.

    Standalone nemesis markers are also allowed for external fault observers.
    A client operation that ends in info may still take effect at any later time,
    so its process is crashed and must not invoke again (Jepsen's rule). Clients
    continue on a fresh process number. The nemesis is exempt.
    """
    pending = {}
    crashed = set()
    operations = []
    for event in events:
        if event.type == "invoke":
            if event.process in pending:
                raise ValueError(f"overlapping invokes at event {event.index}")
            if event.process in crashed:
                raise ValueError(f"process {event.process} invokes after an info outcome at event {event.index}")
            pending[event.process] = len(operations)
            operations.append(Operation(event, None))
            continue
        if event.process not in pending:
            if event.is_nemesis:
                continue
            raise ValueError(f"completion without invoke at event {event.index}")
        position = pending.pop(event.process)
        invoke = operations[position].invoke
        if (invoke.f, invoke.key, invoke.request_id) != (event.f, event.key, event.request_id):
            raise ValueError(f"completion does not match invoke at event {event.index}")
        operations[position] = Operation(invoke, event)
        if event.type == "info" and not event.is_nemesis:
            crashed.add(event.process)
    return operations
