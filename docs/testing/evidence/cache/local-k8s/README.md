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
PYTHONDONTWRITEBYTECODE=1 python3.11 scripts/local-kind/docker_checks.py cache \
  --binary "$LIVE3_WORK/live3-cache/nabedra-db" --revision "$LIVE3_REVISION" \
  --redis-wheel "$LIVE3_WORK/redis-6.4.0-py3-none-any.whl" \
  --lane "$LIVE3_LANE" --output "$LIVE3_WORK/cache"
```
