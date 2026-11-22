#!/usr/bin/env python3
"""Sensitivity checks for K01. All servers are in-process and use loopback."""

from contextlib import contextmanager, redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import socketserver
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import benchmark_fuzz
import differential as diff
from reply_schema import ReplySchemas, SchemaError, check_schema, matches
from standalone import COMMANDS
import valkey_tcl


OVERFLOW = b"-ERR increment or decrement would overflow\r\n"


def bulk(value):
    return b"$-1\r\n" if value is None else b"$%d\r\n%s\r\n" % (len(value), value)


class FakeHandler(socketserver.StreamRequestHandler):
    def handle(self):
        while self.rfile.peek(1):
            request = diff.read_reply(self.rfile)
            self.wfile.write(self.server.reply(*request.value))


class FakeCache(socketserver.ThreadingTCPServer):
    daemon_threads = True

    def __init__(self, mutation=None):
        super().__init__(("127.0.0.1", 0), FakeHandler)
        self.mutation = mutation
        self.data = {}

    def reply(self, command, *args):
        if command == b"PING":
            return b"+BAD\r\n" if self.mutation == "ping" else b"+PONG\r\n"

        key, *rest = args
        value, deadline, ttl = self.data.get(key, (None, None, None))
        now = time.monotonic_ns() // 1_000_000
        if deadline is not None and now >= deadline:
            self.data.pop(key, None)
            value, deadline, ttl = None, None, None

        if command == b"SET":
            ttl = int(rest[2]) if len(rest) == 3 and rest[1] == b"PX" else None
            self.data[key] = (rest[0], now + ttl if ttl else None, ttl)
            return b"+OK\r\n"
        if command == b"GET":
            if self.mutation == "bad-get":
                return b":1\r\n"
            if ttl is not None and self.mutation == "read-refresh":
                self.data[key] = (value, now + ttl, ttl)
            return bulk(value)
        if command == b"PTTL":
            missing = -1 if self.mutation == "missing-as-persistent" else -2
            remaining = missing if value is None else -1 if deadline is None else deadline - now
            return b":%d\r\n" % remaining
        if command == b"EXPIRE" and int(rest[0]) < 0:
            if value is None:
                return b":0\r\n"
            if self.mutation != "negative-expire":
                self.data.pop(key)
            return b":1\r\n"
        if command == b"INCRBY":
            result = int(value or b"0") + int(rest[0])
            if not diff.I64_MIN <= result <= diff.I64_MAX:
                if self.mutation == "overflow-text":
                    return b"-ERR arithmetic overflow\r\n"
                return OVERFLOW
            self.data[key] = (str(result).encode(), deadline, ttl)
            return b":%d\r\n" % result
        raise AssertionError(f"fake did not expect {command!r}")


@contextmanager
def fake_cache(mutation=None):
    with FakeCache(mutation) as server:
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        thread.start()
        try:
            yield server
        finally:
            server.shutdown()
            thread.join()


class DifferentialTests(unittest.TestCase):
    def run_case(self, operations, mutation, reference_mutation=None):
        with (fake_cache(mutation) as a, fake_cache(reference_mutation) as b,
              tempfile.TemporaryDirectory() as temp,
              patch.object(diff, "generate", return_value=[operations]),
              redirect_stdout(io.StringIO()) as stdout, redirect_stderr(io.StringIO()) as stderr):
            history = Path(temp) / "history.jsonl"
            status = diff.main([
                "--a", f"127.0.0.1:{a.server_address[1]}",
                "--b", f"127.0.0.1:{b.server_address[1]}",
                "--programs", "1", "--ops-per-program", str(len(operations)),
                "--history", str(history), "--shrink",
            ])
            rows = [json.loads(line) for line in history.read_text().splitlines()]
        return status, stdout.getvalue(), stderr.getvalue(), rows

    def test_identical_servers_and_binary_history(self):
        program = [diff.op("SET", b"k\xff", b"\x00\xff\r\n"), diff.op("GET", b"k\xff")]
        status, stdout, stderr, rows = self.run_case(program, None)
        self.assertEqual(status, 0, stderr)
        self.assertIn("operations=2 differences=0", stdout)
        self.assertEqual(stderr, "")
        self.assertEqual([row["index"] for row in rows], list(range(8)))
        self.assertEqual(rows[0]["value"], rows[1]["value"])
        self.assertEqual(rows[4]["request_id"], rows[6]["request_id"])
        self.assertEqual(rows[4]["type"], "invoke")
        self.assertEqual(rows[6]["type"], "ok")
        self.assertEqual(bytes.fromhex(rows[6]["value"]), bulk(b"\x00\xff\r\n"))

    def test_overflow_error_text_shrinks_to_two_commands(self):
        program = [
            diff.op("SET", "noise", "x"),
            diff.op("GET", "noise"),
            diff.op("SET", "counter", diff.I64_MAX),
            diff.op("GET", "counter"),
            diff.op("INCRBY", "counter", 1),
            diff.op("GET", "noise"),
        ]
        status, _, stderr, _ = self.run_case(program, "overflow-text")
        self.assertEqual(status, 1)
        self.assertIn("wire replies differ", stderr)
        self.assertIn("arithmetic overflow", stderr)
        self.assertIn("shrunk to 2 commands", stderr)
        self.assertIn("(b'SET', b'counter', b'9223372036854775807')", stderr)
        self.assertIn("(b'INCRBY', b'counter', b'1')", stderr)

    def test_read_refresh_shrinks_to_three_commands(self):
        program = [
            diff.op("SET", "counter", "3"),
            diff.op("SET", "ttl", "v", "PX", 10_000),
            diff.op("GET", "ttl", delay_ms=150),
            diff.op("INCRBY", "counter", 2),
            diff.op("PTTL", "ttl"),
            diff.op("GET", "counter"),
        ]
        status, _, stderr, _ = self.run_case(program, "read-refresh")
        self.assertEqual(status, 1)
        self.assertIn("TTL differs", stderr)
        self.assertIn("shrunk to 3 commands", stderr)
        self.assertIn("delay_ms=150 (b'GET', b'ttl')", stderr)

    def test_negative_expiry_that_keeps_the_value(self):
        program = [
            diff.op("SET", "ttl", "v"), diff.op("GET", "unused"),
            diff.op("EXPIRE", "ttl", -1), diff.op("GET", "ttl"),
        ]
        status, _, stderr, _ = self.run_case(program, "negative-expire")
        self.assertEqual(status, 1)
        self.assertIn("wire replies differ", stderr)
        self.assertIn("shrunk to 3 commands", stderr)

    def test_expired_key_reported_as_persistent_is_not_normalized(self):
        # Regression for review of K01: -2 (missing) and -1 (no expiry) are
        # distinct sentinels, so the PTTL tolerance must not merge them.
        program = [
            diff.op("SET", "noise", "x"),
            diff.op("SET", "ttl", "v", "PX", 20),
            diff.op("GET", "noise", delay_ms=100),
            diff.op("PTTL", "ttl"),
            diff.op("GET", "noise"),
        ]
        status, _, stderr, _ = self.run_case(program, "missing-as-persistent")
        self.assertEqual(status, 1)
        self.assertIn("wire replies differ", stderr)
        self.assertIn("A b':-1\\r\\n'", stderr)
        self.assertIn("B b':-2\\r\\n'", stderr)
        self.assertIn("(b'PTTL', b'ttl')", stderr)

    def test_identical_schema_violations_still_fail(self):
        status, _, stderr, rows = self.run_case([diff.op("GET", "key")], "bad-get", "bad-get")
        self.assertEqual(status, 1)
        self.assertIn("GET reply does not match its schema", stderr)
        self.assertEqual([row["type"] for row in rows], ["invoke", "invoke", "fail", "fail"])
        self.assertTrue(all(row["schema_error"] for row in rows[2:]))

    def test_generator_is_seeded_and_uses_every_supported_command(self):
        programs = list(diff.generate(17, 3, 48))
        self.assertEqual(programs, list(diff.generate(17, 3, 48)))
        self.assertNotEqual(programs, list(diff.generate(18, 3, 48)))
        self.assertEqual([len(p) for p in programs], [48, 48, 48])
        self.assertEqual({step.words[0] for p in programs for step in p},
                         {command.encode().upper() for command in COMMANDS})
        self.assertTrue(all(diff.op(command) in programs[0] for command in diff.DATA_COMMANDS))

    def test_ttl_normalization_does_not_hide_sentinels_or_other_integers(self):
        def difference(command, a, b, tolerance=50):
            a, b = (diff.read_reply(io.BytesIO(b":%d\r\n" % value)) for value in (a, b))
            return diff.reply_difference(command, a, b, tolerance)

        self.assertIsNone(difference(b"PTTL", 1000, 950))
        self.assertIsNone(difference(b"TTL", 2, 1))
        self.assertEqual(difference(b"PTTL", 1000, 949), "TTL differs")
        self.assertEqual(difference(b"TTL", 2, 1, 0), "TTL differs")
        for command, a, b in [(b"TTL", -2, -1), (b"PTTL", 0, -1), (b"EXISTS", 2, 1)]:
            self.assertEqual(difference(command, a, b), "wire replies differ")


class RespSchemaTests(unittest.TestCase):
    def test_raw_resp2_parser_keeps_types_and_consumes_exactly_one_frame(self):
        raw = b"*4\r\n$4\r\n\x00\xff\r\n\r\n$-1\r\n:-2\r\n*1\r\n+OK\r\n"
        stream = io.BytesIO(raw + OVERFLOW)
        first = diff.read_reply(stream)
        self.assertEqual(first.raw, raw)
        self.assertEqual(first.value, [b"\x00\xff\r\n", None, -2, [b"OK"]])
        error = diff.read_reply(stream)
        self.assertEqual((error.prefix, error.raw), (b"-", OVERFLOW))

    def test_resp2_parser_rejects_truncated_and_invalid_frames(self):
        for invalid in (
            b"", b":1\n", b"%1\r\n", b"$-2\r\n", b"$3\r\na\r\n", b"*2\r\n$-1\r\n",
            b":9223372036854775808\r\n", b"$99999999999999\r\n", b"+A\rB\r\n", b":00\r\n",
        ):
            with self.subTest(invalid=invalid), self.assertRaises(diff.RespError):
                diff.read_reply(io.BytesIO(invalid))

    def test_vendored_schemas_enforce_type_range_and_array_items(self):
        schemas = ReplySchemas(COMMANDS)
        for command, raw in (
            (b"SET", b"+OK\r\n"), (b"GET", b"$1\r\n\xff\r\n"),
            (b"MGET", b"*2\r\n$0\r\n\r\n$-1\r\n"),
            (b"TTL", b":-2\r\n"), (b"TTL", b":-1\r\n"), (b"TTL", b":0\r\n"),
            (b"INCRBY", OVERFLOW),
        ):
            with self.subTest(command=command, raw=raw):
                reply = diff.read_reply(io.BytesIO(raw))
                schemas.validate(command, reply.prefix, reply.value)
        for command, raw in (
            (b"GET", b":1\r\n"), (b"GET", b"*-1\r\n"),
            (b"MGET", b"*0\r\n"), (b"MGET", b"*1\r\n:7\r\n"),
            (b"MGET", b"*1\r\n-ERR unexpected\r\n"),
            (b"DEL", b":-1\r\n"), (b"TTL", b":-3\r\n"),
            (b"EXPIRE", b":2\r\n"), (b"INCRBY", b"-bad-tag\r\n"),
        ):
            with self.subTest(command=command, raw=raw), self.assertRaises(SchemaError):
                reply = diff.read_reply(io.BytesIO(raw))
                schemas.validate(command, reply.prefix, reply.value)

    def test_schema_composition_and_unknown_keywords(self):
        overlapping = [{"type": "integer"}, {"const": 2}]
        self.assertFalse(matches({"oneOf": overlapping}, 2))
        self.assertTrue(matches({"anyOf": overlapping}, 2))
        self.assertFalse(matches({"const": 1}, True))
        with self.assertRaises(SchemaError):
            check_schema({"items": {"pattern": "unsupported"}})


class CanaryFuzzTests(unittest.TestCase):
    def test_tcl_counts_real_results_and_does_not_count_setup_or_summary_as_tests(self):
        self.assertTrue(valkey_tcl.skip_patterns(valkey_tcl.SKIPLIST))
        units = [f"canary unit {name} exit={int(i == 0)}"
                 for i, name in enumerate(valkey_tcl.ALLOWLIST)]
        log = "\n".join([
            "[\x1b[0;32;49mok\x1b[0m]: GET hit", "[err]: TTL assertion", "[ignore]: skipped",
            "*** [err]: TTL assertion", "[exception]: ERR unknown command 'flushall'", *units,
        ])
        with redirect_stdout(io.StringIO()) as stdout:
            valkey_tcl.summarize(log)
        self.assertIn("executed=2 [ok]=1 [err]=1 [exception]=1 failed_units=1", stdout.getvalue())
        self.assertIn("canary is not authoritative", stdout.getvalue())
        with redirect_stdout(io.StringIO()), self.assertRaises(ValueError):
            valkey_tcl.summarize(log.replace(units[-1], ""))
        startup_failure = "\n".join(["Binary not found or not executable", *units])
        with redirect_stdout(io.StringIO()), self.assertRaisesRegex(ValueError, "without test results"):
            valkey_tcl.summarize(startup_failure)

    def test_fuzz_mode_detection_and_post_run_ping(self):
        self.assertFalse(benchmark_fuzz.supports_fuzz("--fuzz-mode malformed-commands"))
        self.assertTrue(benchmark_fuzz.supports_fuzz("--fuzz\n--fuzz-mode malformed-commands"))

        with fake_cache() as server, patch.object(benchmark_fuzz, "benchmark") as run:
            def corrupt_ping(network, argv, *, check):
                self.assertEqual(network, "host")
                self.assertTrue(check)
                self.assertEqual(argv[-3:], ["--fuzz", "--fuzz-mode", "malformed-commands"])
                server.mutation = "ping"

            run.side_effect = corrupt_ping
            with self.assertRaisesRegex(diff.RespError, "expected PONG"):
                benchmark_fuzz.fuzz(server.server_address)
            run.assert_called_once()


if __name__ == "__main__":
    unittest.main()
