"""Public installed-package checks shared with the TypeScript driver."""
from importlib.metadata import version
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import time

import nabedra
from nabedra.cache import Cache


vectors = json.loads((Path(__file__).parent / "vectors.json").read_text())
config = json.loads(os.environ["NABEDRA_CACHE_TEST_CONFIG"])
assert version("nabedra") == vectors["version"]
assert nabedra.Cache is Cache
assert sorted(name for name in Cache.__dict__ if not name.startswith("_")) == [
    "close", "connect", "delete", "expire", "get", "increment", "persist", "set", "ttl_ms",
]


def expect_error(name, command, code=None, outcome=None):
    try:
        command()
    except Exception as error:
        assert type(error).__name__ == name, (name, error)
        if code:
            assert error.code == code
        if outcome:
            assert error.outcome == outcome
    else:
        raise AssertionError(f"expected {name}")


def await_fixture(observer, event):
    deadline = time.monotonic() + 2
    while observer.get(event) != b"1":
        assert time.monotonic() < deadline, event
        time.sleep(0.005)


def semantics(cache):
    for command, key, *args in vectors["operations"]:
        if command == "get":
            actual = cache.get(key)
            expected = None if args[0] is None else bytes.fromhex(args[0])
        elif command == "set":
            data, ttl_ms, when, expected = args
            actual = cache.set(key, bytes.fromhex(data), ttl_ms=ttl_ms, when=when)
        elif command == "ttl":
            actual = cache.ttl_ms(key)
            expected = args[0]
            if isinstance(expected, list):
                assert expected[0] <= actual <= expected[1], (command, key, actual, expected)
                continue
        elif command == "expire":
            ttl_ms, expected = args
            actual = cache.expire(key, ttl_ms)
        elif command == "persist":
            actual, expected = cache.persist(key), args[0]
        elif command == "delete":
            actual = cache.delete(key)
            if not args:
                continue
            expected = args[0]
        elif command == "increment":
            actual, expected = cache.increment(key, int(args[0])), int(args[1])
        elif command == "incrementError":
            expect_error("ValidationError", lambda: cache.increment(key, int(args[0])))
            continue
        elif command == "waitMiss":
            deadline = time.monotonic() + 2
            while cache.get(key) is not None:
                assert time.monotonic() < deadline, key
                time.sleep(0.005)
            assert cache.ttl_ms(key) == -2
            continue
        else:
            raise AssertionError(command)
        assert type(actual) is type(expected) and actual == expected, (command, key, actual, expected)

    large = b"\x00\xff" * (512 * 1024)
    assert cache.set("large", large) and cache.get("large") == large and cache.delete("large")


def fixture():
    namespace = vectors["namespace"]
    endpoint = config["fixture"]
    auth = {"username": "test", "password": "secret", "namespace": namespace}
    expect_error("AuthenticationError", lambda: Cache.connect(endpoint, username="test", password="bad"))
    with Cache.connect(endpoint, **auth, timeout_ms=500) as cache:
        for key, encoded in vectors["keys"]:
            assert cache.get(key) == encoded.encode("ascii")
        for key, error, code, outcome in vectors["serviceErrors"]:
            expect_error(error, lambda: cache.set(key, b"x"), code, outcome)
        invalid = [
            lambda: cache.get(""), lambda: cache.get("é" * 126), lambda: cache.get("\ud800"),
            lambda: cache.set("k", b"a", ttl_ms=0), lambda: cache.set("k", b"a", ttl_ms=1 << 53),
            lambda: cache.set("k", b"a", ttl_ms=True), lambda: cache.set("k", b"a", when="maybe"),
            lambda: cache.set("k", bytes((1 << 20) + 1)), lambda: cache.expire("k", -1),
            lambda: cache.increment("k", 1 << 63), lambda: cache.increment("k", True),
        ]
        for command in invalid:
            expect_error("ValidationError", command, "validation_error", "not_applied")
        expect_error("UnknownOutcomeError", lambda: cache.increment("drop", 1), "outcome_unknown", "unknown")
        assert cache.get("drop") == b"1"
        expect_error("UnknownOutcomeError", lambda: cache.increment("garbled", 1))
        assert cache.get("garbled") == b"1"
        expect_error("ProtocolError", lambda: cache.get("badframe"))
        expect_error("TransportError", lambda: cache.get("stall"))
        start = time.monotonic()
        expect_error("UnknownOutcomeError", lambda: cache.increment("deadline", 1))
        assert time.monotonic() - start < 1.5
        assert cache.get("deadline") == b"1"
    cache.close()
    expect_error("TransportError", lambda: cache.get("closed"), "transport_error", "not_applied")

    expect_error("ValidationError", lambda: Cache.connect("redis://192.0.2.1"))
    expect_error("ValidationError", lambda: Cache.connect(endpoint, namespace="UPPER"))
    expect_error("ValidationError", lambda: Cache.connect(endpoint, namespace="ok\n"))
    expect_error("TransportError", lambda: Cache.connect(config["badTls"], ca_file=config["fixtureCa"], **auth))
    with Cache.connect(config["tls"], ca_file=config["fixtureCa"], **auth) as secure:
        assert secure.get("sku:42") == b"nb:catalog:c2t1OjQy"

    with Cache.connect(endpoint, **auth) as observer:
        for phase, target in [("auth", config["closeAuth"]), ("tls", config["closeTls"])]:
            with Cache.connect(config["tls"], ca_file=config["fixtureCa"], cluster=True,
                               allowed_nodes=[target], timeout_ms=10_000, pool_size=1, **auth) as closing:
                with ThreadPoolExecutor(max_workers=1) as executor:
                    write = lambda: closing.increment(f"close-{phase}", 1)
                    pending = executor.submit(expect_error, "TransportError", write, "transport_error", "not_applied")
                    try:
                        await_fixture(observer, f"{phase}-entered")
                        expect_error("OverloadedError", lambda: closing.get("full"), "overloaded", "not_applied")
                    finally:
                        closing.close()
                    await_fixture(observer, f"{phase}-closed")
                    pending.result(timeout=2)
                    expect_error("TransportError", write, "transport_error", "not_applied")

    with Cache.connect(endpoint, pool_size=1, **auth) as limited, Cache.connect(endpoint, **auth) as observer:
        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(limited.get, "held")
            try:
                deadline = time.monotonic() + 1
                while observer.get("held-entered") != b"1":
                    assert time.monotonic() < deadline
                    time.sleep(0.005)
                expect_error("OverloadedError", lambda: limited.get("anything"), "overloaded", "not_applied")
            finally:
                assert observer.get("held-release") == b"1"
            assert pending.result(timeout=2) == b"released"

    seed = config["seed"]
    target = config["target"]
    with Cache.connect(seed, cluster=True, allowed_nodes=[target], **auth) as cache:
        assert cache.increment("move", 11) == 11
        assert cache.increment("move", 2) == 13
        assert cache.get("move-count") == b"2"
        assert cache.get("move-seed-count") == b"1"
        expect_error("ProtocolError", lambda: cache.increment("wrong-slot", 1))
        expect_error("ProtocolError", lambda: cache.increment("untrusted", 1))
    with Cache.connect(seed, cluster=True, **auth) as cache:
        expect_error("ProtocolError", lambda: cache.increment("move", 1))


if "fixture" in config:
    fixture()
if endpoint := config.get("endpoint"):
    with Cache.connect(endpoint, namespace=vectors["namespace"], username=config.get("username"),
                       password=config.get("password"), ca_file=config.get("caFile"),
                       cluster=config.get("cluster", False), allowed_nodes=config.get("allowedNodes", []),
                       allowed_node_suffix=config.get("allowedNodeSuffix")) as cache:
        semantics(cache)
print("Python installed Cache contract passed")
