# Events SDK and OLAP sink acceptance

These tests cover the Python Events client's wire protocol and the OLAP sink.
Both run inside Docker. The sink test starts a real Events broker and a
database API with its workers, all on loopback, from Linux builds of
`nabedra-events` and the OLAP `nabedra-db`. The database stores its objects in
the repository's local SQLite object store, which supports conditional writes.

From the repository root, set `EVENTS_BIN_DIR` to an absolute path to the
directory that holds `nabedra-events`, and `OLAP_BIN_DIR` to the one that holds
`nabedra-db`. Change the CPU and memory limits to fit your Docker host. The
repository is mounted read-only, so write the evidence to a separate
directory:

```sh
mkdir -p /tmp/nabedra-events-sink
docker run --rm --network none --cpus 1.5 --memory 768m --memory-swap 768m \
  -v "$PWD:/repo:ro" -v "$EVENTS_BIN_DIR:/events-bin:ro" \
  -v "$OLAP_BIN_DIR:/olap-bin:ro" -v /tmp/nabedra-events-sink:/evidence \
  -w /repo -e PYTHONPATH=/repo/orm/python/src -e PYTHONDONTWRITEBYTECODE=1 \
  python:3.11-bookworm sh -ec '
    python -m unittest discover -s orm/python/tests/events -v
    python orm/python/tests/events/acceptance_sink.py \
      --events-binary /events-bin/nabedra-events \
      --olap-binary /olap-bin/nabedra-db --evidence /evidence/ev04.json
  '
```

The Python image must have the runtime libraries the two binaries need. The
evidence file records the hash of each binary and every assertion that
passed, even when a later one fails.

| Test | Required observations |
| --- | --- |
| SDK wire | Retry a claim with the same UUID after a missing response. Classify a missing publish response, then explicitly retry the same bytes and identity. Validate decimals, large offsets, delivery ownership, typed errors and the gateway selector. |
| EV04 commit and redelivery | Hold an OLAP reply until its durable commit is observable. Deliver the other partition and read both through an independent subscription. Kill the sink before its checkpoint PUT reaches Events. Redeliver and verify every row and its exact JSON by identity. |
| EV04 worker recovery | Observe an accepted write at a worker-side barrier. Restart the API and recover the same receipt after losing the scheduler worker. Stop the last worker with another accepted write held and check its abandonment, prior durable decisions and the sink's fresh request. |
| EV04 sink limit and destination checks | Hold the global permit while another partition polls. When released, both writes complete. A replaced table UUID, incompatible schema or conflicting row identity leaves the affected checkpoint unchanged. |

The worker recovery case covers the API restart and surviving-copy behavior
of scenarios Q04 and R01, and the full-shutdown behavior of R04. The lost OLAP
reply happens at the public API, after the commit. The sink limit here is the
sink's own limit on its submissions, not the scheduler's.

These tests don't cover the following, which each need their own fixture:
scheduler saturation by queue count and bytes, identity races between several
APIs, receipt results expiring, lost object store responses for the catalog,
fencing of catalog PUTs from a stale run, parity between same-process and
distributed runs, and malformed traffic while the scheduler is saturated.
