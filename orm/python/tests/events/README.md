# Events SDK and OLAP sink acceptance

Run the SDK wire tests and the sink process fixture inside Docker. The sink
fixture takes Linux binaries for Events and the OLAP-enabled `nabedra-db`. It
runs a real Events broker plus the database API and workers on loopback. The
database uses the repository's local SQLite conditional object store.

From the repository root, set `EVENTS_BIN_DIR` and `OLAP_BIN_DIR` to absolute
paths to directories containing `nabedra-events` and `nabedra-db`, respectively.
Adjust the CPU and memory limits to fit the current Docker host. Use an output
directory separate from the read-only repository mount.

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

Use a Python image with the runtime libraries required by the two binaries.
The fixture writes binary hashes and completed assertions to the evidence
file, including when a later assertion fails.

| Test | Required observations |
| --- | --- |
| SDK wire | Retry a claim with the same UUID after a missing response. Classify a missing publish response, then explicitly retry the same bytes and identity. Validate decimals, large offsets, delivery ownership, typed errors and the gateway selector. |
| EV04 commit and redelivery | Hold an OLAP reply until its durable commit is observable. Deliver the other partition and read both through an independent subscription. Kill the sink before its checkpoint PUT reaches Events. Redeliver and verify every row and its exact JSON by identity. |
| EV04 worker recovery | Observe an accepted write at a worker-side barrier. Restart the API and recover the same receipt after losing the scheduler worker. Stop the last worker with another accepted write held and check its abandonment, prior durable decisions and the sink's fresh request. |
| EV04 sink limit and destination checks | Hold the global permit while another partition polls. When released, both writes complete. A replaced table UUID, incompatible schema or conflicting row identity leaves the affected checkpoint unchanged. |

The worker recovery case targets the API restart and surviving-copy behavior
of Q04 and R01, and the complete shutdown behavior of R04. The lost OLAP
reply is at the public API boundary after commit. The sink limits its own
submissions. The scheduler's queue count and byte saturation, multi-API
identity races, receipt-result expiration, object-store catalog response
loss, stale-run catalog PUT fencing, same-process and distributed parity,
and malformed traffic during scheduler saturation require distinct fixtures.
