"""Record a bounded live replay through the installed synchronous Cache API."""
import json
import os
from pathlib import Path
import time

from nabedra.cache import Cache

config = json.loads(os.environ["NABEDRA_CACHE_REPLAY"])
stop = Path(config["stop"])
options = {
    "username": config["username"], "password": config["password"],
    "ca_file": config["caFile"], "allowed_nodes": config["allowed_nodes"],
    "cluster": True, "namespace": config["namespace"], "timeout_ms": 1500,
}
value = bytes.fromhex("00ff400a")
ttl_ms = config["limitSeconds"] * 1000 + 300_000
cache = Cache.connect(config["seeds"], **options)
for index in range(64):
    if not cache.set(f"provider-py-entry-{index}", value, ttl_ms=ttl_ms):
        raise RuntimeError("provider replay preload failed")
started = time.monotonic_ns()
number = 0
while not stop.exists() and time.monotonic_ns() - started < config["limitSeconds"] * 10**9:
    scheduled = started + number * 50_000_000
    time.sleep(max(0, scheduled - time.monotonic_ns()) / 10**9)
    key = f"provider-py-entry-{number % 64}"
    action = "get" if number % 5 else "set"
    result = "ok"
    try:
        if action == "set":
            if not cache.set(key, value, ttl_ms=ttl_ms):
                result = "incorrect_reply"
        else:
            actual = cache.get(key)
            if actual is None:
                result = "origin_refill_miss"
            elif actual != value:
                result = "incorrect_reply"
    except Exception as error:
        result = getattr(error, "code", "driver_error")
    completed = time.monotonic_ns()
    print(json.dumps({"arrivalNs": scheduled - started, "completedNs": completed - started,
                      "command": action, "outcome": result}), flush=True)
    number += 1
cache.close()
