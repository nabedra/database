#!/usr/bin/env python3
"""K01 data command conformance against pinned Valkey 8.1.11."""

import argparse
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from enum import Enum
import json
from pathlib import Path
import random
import re
import socket
import subprocess
import sys
import tempfile
import time
import uuid

from reply_schema import ReplySchemas, SchemaError
from standalone import COMMANDS, available_port, native_config, wire


VALKEY_VERSION = "8.1.11"
VALKEY_REVISION = "5fb5bc4b13b22084b54c7db2a88d14ce2f6fa867"
VALKEY_IMAGE = (
    "valkey/valkey:8.1.11@sha256:8f836008e0986c36adf50da714c7060cf689dbb335f61fe077939d90fa650809"
)
DATA_COMMANDS = tuple(command.upper().encode() for command in COMMANDS)
MULTI_KEY = (b"DEL", b"EXISTS", b"MGET")
TTL_COMMANDS = (b"TTL", b"PTTL")
I64_MIN = -(2**63)
I64_MAX = 2**63 - 1
INTEGER = re.compile(rb"(?:0|-?[1-9][0-9]{0,18})")
Endpoint = tuple[str, int]


class RespError(ValueError):
    pass


class NonJsonReply(Enum):
    NULL_ARRAY = "null array"
    NESTED_ERROR = "nested error"


@dataclass(frozen=True)
class Reply:
    raw: bytes
    prefix: bytes
    value: object


def read_reply(stream):
    """Consume one bounded RESP2 reply and retain its exact wire bytes."""
    frame = bytearray()
    max_bytes = 4 << 20

    def parse(depth):
        if depth > 32:
            raise RespError("RESP array nesting exceeds 32")
        line = stream.readline(min(8192, max_bytes - len(frame)))
        if not line or not line.endswith(b"\r\n"):
            raise RespError("missing or unterminated RESP reply header")
        frame.extend(line)
        prefix, body = line[:1], line[1:-2]
        if prefix in (b"+", b"-"):
            if b"\r" in body or b"\n" in body:
                raise RespError("invalid RESP line reply")
            return prefix, body
        if prefix not in (b":", b"$", b"*") or not INTEGER.fullmatch(body):
            raise RespError(f"invalid RESP reply header {line!r}")
        number = int(body)
        if prefix == b":" and I64_MIN <= number <= I64_MAX:
            return prefix, number
        if prefix == b"$" and number == -1:
            return prefix, None
        if prefix == b"*" and number == -1:
            return prefix, NonJsonReply.NULL_ARRAY
        if prefix == b"$" and 0 <= number <= max_bytes - len(frame) - 2:
            payload = stream.read(number + 2)
            if len(payload) != number + 2 or not payload.endswith(b"\r\n"):
                raise RespError("missing bulk payload or terminator")
            frame.extend(payload)
            return prefix, payload[:-2]
        if prefix == b"*" and 0 <= number <= 16384:
            values = []
            for _ in range(number):
                child_prefix, value = parse(depth + 1)
                values.append(NonJsonReply.NESTED_ERROR if child_prefix == b"-" else value)
            return prefix, values
        raise RespError(f"out-of-range RESP reply header {line!r}")

    prefix, value = parse(0)
    return Reply(bytes(frame), prefix, value)


@contextmanager
def connect(endpoint):
    with socket.create_connection(endpoint, timeout=4) as connection:
        connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        with connection.makefile("rb") as stream:
            yield connection, stream


def ping(endpoint):
    with connect(endpoint) as (connection, stream):
        connection.sendall(wire(b"PING"))
        reply = read_reply(stream)
        if reply.raw != b"+PONG\r\n":
            raise RespError(f"expected PONG, got {reply.raw!r}")


def wait_ready(endpoint, process=None):
    deadline = time.monotonic() + 10
    while True:
        if process is not None and process.poll() is not None:
            raise RuntimeError(f"Nabedra exited during startup ({process.returncode})")
        try:
            ping(endpoint)
            return
        except (OSError, RespError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.05)


@contextmanager
def spawn_nabedra(binary):
    with tempfile.TemporaryDirectory() as temp:
        folder = Path(temp)
        endpoint = ("127.0.0.1", available_port())
        config = native_config(folder, endpoint[1], False, "noeviction", {})
        with (folder / "server.log").open("w+") as log:
            process = subprocess.Popen(
                [str(binary.resolve()), "serve", "--config", str(config)], stdout=log, stderr=log
            )
            try:
                wait_ready(endpoint, process)
                yield endpoint
            except Exception:
                log.seek(0)
                print(log.read(), file=sys.stderr, end="")
                raise
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()


def remove_container(name):
    subprocess.run(
        ["docker", "rm", "-f", name], stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, timeout=15, check=False,
    )


@contextmanager
def spawn_valkey():
    name = f"nabedra-kv-diff-{uuid.uuid4().hex}"
    try:
        subprocess.run([
            "docker", "run", "--rm", "--detach", "--name", name, "--memory=128m",
            "--publish", "127.0.0.1::6379", VALKEY_IMAGE,
            "valkey-server", "--save", "", "--appendonly", "no",
        ], stdout=subprocess.DEVNULL, check=True, timeout=180)
        published = subprocess.check_output(
            ["docker", "port", name, "6379/tcp"], text=True, timeout=10
        ).strip()
        endpoint = parse_endpoint(published)
        wait_ready(endpoint)
        yield endpoint
    finally:
        remove_container(name)


@dataclass(frozen=True)
class Operation:
    words: tuple[bytes, ...]
    delay_ms: int = 0

    def request(self, namespace):
        name, *args = self.words
        key_count = len(args) if name.upper() in MULTI_KEY else min(1, len(args))
        keys = [namespace + key for key in args[:key_count]]
        return keys, wire(name, *keys, *args[key_count:])


def op(*words, delay_ms=0):
    return Operation(tuple(word if isinstance(word, bytes) else str(word).encode() for word in words),
                     delay_ms)


def edge_cases():
    binary_key, binary_value = b"binary\x00\r\n\xff", b"\x00\r\n\xff"
    return [
        op("SET", "overflow", I64_MAX),
        op("INCRBY", "overflow", 1),
        op("GET", "overflow"),
        op("SET", "underflow", I64_MIN),
        op("INCRBY", "underflow", -1),
        op("INCRBY", "underflow", I64_MAX + 1),
        op("SET", binary_key, binary_value),
        op("INCRBY", binary_key, 1),
        op("SET", "empty", b""),
        op("MGET", binary_key, "empty", "missing", binary_key),
        op("EXISTS", binary_key, "missing", binary_key),
        op("DEL", binary_key, binary_key, "missing"),
        op("SET", "ttl", b"", "PX", 8000),
        op("TTL", "ttl"),
        op("GET", "ttl", delay_ms=200),
        op("PTTL", "ttl"),
        op("INCRBY", "ttl", 0),
        op("EXPIRE", "ttl", -1),
        op("TTL", "ttl"),
        op("GET", "ttl"),
        op("SET", "expire", "x", "PX", 20),
        op("GET", "expire", delay_ms=100),
        op("PTTL", "expire"),
        op("SET", "options", b"", "NX", "EX", 8),
        op("SET", "options", "x", "NX"),
        op("SET", "options", "x", "XX", "PX", 8000),
        op("SET", "options", "x", "PX", -1),
        op("PERSIST", "options"),
        op("PTTL", "options"),
        op("PEXPIRE", "options", 0),
        op("GET", "options"),
        *[op(command) for command in DATA_COMMANDS],
    ]


def random_operation(rng, keys):
    name = rng.choice(DATA_COMMANDS)
    key = rng.choice(keys)
    if rng.randrange(16) == 0:
        return op(name)
    if name == b"SET":
        value = rng.choice([b"", b"0", b"-1", b"01", rng.randbytes(rng.randrange(1, 32))])
        flags = rng.choice([(), (b"NX",), (b"XX",)])
        if rng.randrange(2):
            expiry = rng.choice([(b"EX", 30), (b"PX", 30_000), (b"PX", -1)])
            return op(name, key, value, *flags, *expiry)
        return op(name, key, value, *flags)
    if name == b"INCRBY":
        return op(name, key, rng.choice([1, -1, I64_MIN, I64_MAX, I64_MAX + 1, b"1.0", b""]))
    if name in (b"EXPIRE", b"PEXPIRE"):
        positive = 30 if name == b"EXPIRE" else 30_000
        return op(name, key, rng.choice([positive, -1, 0, b"1.0", I64_MAX + 1]))
    if name in MULTI_KEY:
        return op(name, key, *rng.choices(keys, k=rng.randrange(4)))
    return op(name, key)


def generate(seed, programs, ops_per_program):
    rng = random.Random(seed)
    for index in range(programs):
        keys = [b"k0", b"k1", b"k2", b"k3", rng.randbytes(8), rng.randbytes(12)]
        program = edge_cases()[:ops_per_program] if index == 0 else []
        program.extend(random_operation(rng, keys) for _ in range(ops_per_program - len(program)))
        yield program


class History:
    def __init__(self, stream, seed):
        self.stream = stream
        self.seed = seed
        self.index = 0
        self.attempt = 0

    def event(self, kind, side, program, step, operation, keys, payload, **details):
        row = {
            "index": self.index, "type": kind, "process": side, "node": ("a", "b")[side],
            "f": operation.words[0].decode().lower(), "time_ns": time.monotonic_ns(),
            "request_id": f"{self.attempt}-{step}-{side}", "key": keys[0].hex() if keys else None,
            "value": payload.hex() if payload is not None else None, "seed": self.seed,
            "program": program, "attempt": self.attempt, "step": step,
            "keys_hex": [key.hex() for key in keys], **details,
        }
        self.stream.write(json.dumps(row) + "\n")
        self.index += 1


@dataclass(frozen=True)
class Difference:
    step: int
    operation: Operation
    reason: str
    a: bytes
    b: bytes

    def signature(self):
        replies = () if self.reason == "TTL differs" else (self.a, self.b)
        return self.operation, self.reason, replies


def reply_difference(command, a, b, tolerance_ms):
    if a.raw == b.raw:
        return None
    if command in TTL_COMMANDS and a.prefix == b.prefix == b":" and min(a.value, b.value) >= 0:
        tolerance = tolerance_ms if command == b"PTTL" else (tolerance_ms + 999) // 1000
        return None if abs(a.value - b.value) <= tolerance else "TTL differs"
    return "wire replies differ"


def compare_program(program, endpoints, schemas, tolerance_ms, history, program_index):
    history.attempt += 1
    namespace = b"nb:kv-diff:" + uuid.uuid4().hex.encode() + b":"
    differences = []
    with ExitStack() as stack:
        clients = [stack.enter_context(connect(endpoint)) for endpoint in endpoints]
        for step, operation in enumerate(program):
            if operation.delay_ms:
                time.sleep(operation.delay_ms / 1000)
            command = operation.words[0].upper()
            keys, request = operation.request(namespace)
            replies = []
            schema_issues = []
            # Dispatch to both engines before reading. A slow reply on one
            # connection does not change when the peer receives the command.
            for side, (connection, _) in enumerate(clients):
                history.event("invoke", side, program_index, step, operation, keys, request,
                              delay_ms=operation.delay_ms)
                try:
                    connection.sendall(request)
                except OSError as error:
                    history.event("info", side, program_index, step, operation, keys, None,
                                  transport_error=str(error))
                    raise
            for side, (_, stream) in enumerate(clients):
                try:
                    reply = read_reply(stream)
                except (OSError, RespError) as error:
                    history.event("info", side, program_index, step, operation, keys, None,
                                  transport_error=str(error))
                    raise
                issue = None
                try:
                    schemas.validate(command, reply.prefix, reply.value)
                except SchemaError as error:
                    issue = str(error)
                    schema_issues.append(f"{('a', 'b')[side]} schema: {issue}")
                history.event("fail" if issue or reply.prefix == b"-" else "ok", side,
                              program_index, step, operation, keys, reply.raw, schema_error=issue)
                replies.append(reply)
            a, b = replies
            reasons = [*schema_issues, reply_difference(command, a, b, tolerance_ms)]
            differences.extend(Difference(step, operation, reason, a.raw, b.raw)
                               for reason in reasons if reason)
    return differences


def ddmin(program, fails):
    granularity = 2
    while len(program) >= 2:
        chunk = (len(program) + granularity - 1) // granularity
        for start in range(0, len(program), chunk):
            candidate = program[:start] + program[start + chunk:]
            if fails(candidate):
                program = candidate
                granularity = max(2, granularity - 1)
                break
        else:
            if granularity >= len(program):
                break
            granularity = min(len(program), granularity * 2)
    return program


def parse_endpoint(text):
    host, separator, port_text = text.rpartition(":")
    if separator and host and port_text.isascii() and port_text.isdecimal():
        port = int(port_text)
        if 1 <= port <= 65535:
            return host.removeprefix("[").removesuffix("]"), port
    raise argparse.ArgumentTypeError("expected host:port or [ipv6]:port")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    target_a = parser.add_mutually_exclusive_group(required=True)
    target_a.add_argument("--a", type=parse_endpoint)
    target_a.add_argument("--nabedra-bin", type=Path)
    target_b = parser.add_mutually_exclusive_group(required=True)
    target_b.add_argument("--b", type=parse_endpoint)
    target_b.add_argument("--spawn-valkey", action="store_true")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--programs", type=int, default=64)
    parser.add_argument("--ops-per-program", type=int, default=48)
    parser.add_argument("--ttl-tolerance-ms", type=int, default=50)
    parser.add_argument("--shrink", action="store_true")
    parser.add_argument("--history", type=Path, default=Path("artifacts/cache-valkey-diff.jsonl"))
    args = parser.parse_args(argv)
    if args.programs <= 0 or args.ops_per_program <= 0 or args.ttl_tolerance_ms < 0:
        parser.error("program and operation counts must be positive and TTL tolerance nonnegative")

    try:
        schemas = ReplySchemas(COMMANDS)
        args.history.parent.mkdir(parents=True, exist_ok=True)
        with ExitStack() as stack:
            a = args.a or stack.enter_context(spawn_nabedra(args.nabedra_bin))
            b = args.b or stack.enter_context(spawn_valkey())
            # Distinct data stores are needed even if both use the same image.
            if a == b:
                parser.error("A and B must be separate cache instances")
            history = History(stack.enter_context(args.history.open("w", encoding="utf-8")), args.seed)
            first_failure = None
            mismatch_count = 0

            def compare(program, index):
                return compare_program(program, (a, b), schemas, args.ttl_tolerance_ms, history, index)

            for index, program in enumerate(generate(args.seed, args.programs, args.ops_per_program)):
                differences = compare(program, index)
                mismatch_count += len(differences)
                if differences and first_failure is None:
                    first_failure = index, program, differences[0]
            print(f"K01 seed={args.seed} programs={args.programs} "
                  f"operations={args.programs * args.ops_per_program} differences={mismatch_count} "
                  f"history={args.history}", flush=True)
            if first_failure:
                index, program, failure = first_failure
                print(f"program {index}, operation {failure.step} {failure.operation.words!r}: "
                      f"{failure.reason}\n  A {failure.a!r}\n  B {failure.b!r}", file=sys.stderr)
                if args.shrink:
                    reduced = ddmin(program, lambda candidate: any(
                        difference.signature() == failure.signature()
                        for difference in compare(candidate, index)
                    ))
                    print(f"shrunk to {len(reduced)} commands:", file=sys.stderr)
                    for operation in reduced:
                        print(f"  delay_ms={operation.delay_ms} {operation.words!r}", file=sys.stderr)
                return 1
    except (OSError, RespError, SchemaError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"K01 differential failed: {error}; history={args.history}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
