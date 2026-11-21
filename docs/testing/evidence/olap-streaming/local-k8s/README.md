# LIVE3 OLAP provider checks in local Docker

The local provider fixture starts real API and worker processes inside one
Linux container. Its callbacks provision and terminate processes and inject
an inventory failure. The stream oracle reopens the original handle after
worker removal, verifies the byte-identical replay, then reads the remaining
row and EOF. This fixture exercises the A01, A02 and ST05 callback and drain
assertions. Cloud provider checks stay in the
[LIVE2 preflight](../live-aws/README.md).

Run from the repository root using the verified `live3-olap` artifact.

```sh
PYTHONDONTWRITEBYTECODE=1 python3.11 scripts/local-kind/docker_checks.py olap \
  --binary "$LIVE3_WORK/live3-olap/nabedra-db" --revision "$LIVE3_REVISION" \
  --lane "$LIVE3_LANE" --output "$LIVE3_WORK/olap"
```
