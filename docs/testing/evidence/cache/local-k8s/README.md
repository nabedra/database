# LIVE3 Cache checks in local Docker

The paired diagnostic runs the release Cache binary and Valkey 8.1.11
sequentially in one Linux container. The arguments select the stock RESP2
driver, TLS, 64 KiB GET hits and SET PX, and 10,000 requests per trial at
650 requests/s. K04a qualification still needs the full paired rate sweep
on the specified EC2 host.
[Cache benchmark contract](../../../cache-benchmark.md)

The lifecycle driver runs three controllers and zone-labelled workers on
one VM. It pauses one zone's controller and worker after verifying the
replication prefix. It measures read attempts and captures survivor resource
samples. Its growth and drain check moves a representative slot onto each
new primary, verifies cross-zone-labelled replica placement, and terminates
workers after the controller removes their assignments. Local process
faults and small slot movements cover only part of K04b and K05.

```sh
python3.11 -m pip download --no-deps --only-binary=:all: --dest "$LIVE3_WORK" redis==6.4.0
python3.11 -m pip download --no-deps --only-binary=:all: --platform manylinux2014_x86_64 \
  --implementation cp --abi cp311 --python-version 3.11 --dest "$LIVE3_WORK" hiredis==3.2.1
PYTHONDONTWRITEBYTECODE=1 python3.11 scripts/local-kind/docker_checks.py cache \
  --binary "$LIVE3_WORK/live3-cache/nabedra-db" --revision "$LIVE3_REVISION" \
  --redis-wheel "$LIVE3_WORK/redis-6.4.0-py3-none-any.whl" \
  --hiredis-wheel "$LIVE3_WORK"/hiredis-3.2.1-*.whl \
  --lane "$LIVE3_LANE" --output "$LIVE3_WORK/cache"
```

## Results at the post-merge revision

| Check | Status | Executed assertion |
| --- | --- | --- |
| K04a local comparison | Passed the paired workload | 120,000 correct operations, zero operation errors. The fixed rate and local VM leave EC2 qualification open. |
| K04b local zone promotion | Passed | The replica had the acknowledged prefix before both zone processes stopped. A survivor served the exact value in 5.654 s. |
| K04b full replica readiness on resume | Failed the local watchdog | The controller view did not return to 16,384 ready primaries and 16,384 ready replicas within 60 s. The resumed worker logged a timed-out update. |
| Cloud K04a, K04b, K05 | Not run | LIVE3 used local Docker without AWS. |

Median per-run client arrival-to-completion latency at 650 offered operations/s,
in milliseconds. Each cell contains three 10,000-operation trials per engine.

| Operation | Nabedra p50 / p99 | Valkey p50 / p99 |
| --- | ---: | ---: |
| 64 KiB TLS GET hit | 2.826 / 111.060 | 2.920 / 142.125 |
| 64 KiB TLS SET PX | 2.180 / 97.737 | 2.299 / 49.581 |

Both engines missed the 1 ms p99 budget at this rate. The raw zone-loss trace
contains 15 successful reads before the pause, three during it and 196 after
resume. The longest successful attempt during the pause took 5.608 s. The
container's sampled memory increased from 298,307,584 to 349,671,424 bytes.
The trace records the surviving controller and worker CPU ticks, RSS and
loopback counters. The small load does not establish sustainable survivor
headroom. [Zone trace](zone-loss/trace.jsonl) ·
[Restore assertion](zone-loss/assertion-output.txt) ·
[Resumed worker output](zone-loss/worker-output.txt)

The Cache release binary at `20c102659f058c6a512cb1b924087032ea07a44d` had
SHA-256 `0bf5217080559f904ee1ecb9220add10664aa27061d1cb0ac0352ebd21d42d26`.
The Valkey reference had SHA-256
`1bf3407a5194c24117ddc353620037241209acd9fba9e18f5b7520eba68c9bf3`.
The highest sampled Docker VM use across the pair and zone run was
1,334,583,296 bytes. The combined driver exited 1 because of the replica
readiness watchdog. The paired workload itself exited zero.
[Raw samples and configs](paired-raw.tar.gz) · [Paired output](pair-output.txt) ·
[Docker session](docker-trace.jsonl) ·
[Linux build](https://github.com/zPy52/nabedra-database/actions/runs/44929728236)
