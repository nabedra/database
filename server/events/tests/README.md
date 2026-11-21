# Events Docker acceptance

This suite runs Events brokers in Docker and checks them against the plan's
EV scenarios. Run it from the repository root. You need Python 3.11 or newer
and an Events image built from the checkout you are testing. The `tls` case
also uses OpenSSL to issue short-lived certificates for the fixture.

Check how much CPU and memory Docker has free before you pick the limits:

```sh
docker info --format '{{.NCPU}} {{.MemTotal}}'
docker stats --no-stream

python3.11 server/events/tests/acceptance.py \
  --image nabedra-events:local \
  --cpus 2 --memory-mib 1536 \
  --output /tmp/events-acceptance.json --logs /tmp/events-acceptance-logs
```

`--cpus` and `--memory-mib` cap all the brokers of a case together, so leave
room for any other Docker jobs that are running. The suite runs one case at a
time on loopback ports that Docker assigns. Each broker gets its own named
volume, and when a case ends the suite removes that case's containers, network
and volumes.

To run only some cases, pass `--cases`. The JSON output lists every assertion
that passed, even when a later check in the same case fails. Logs from failed
cases are written only when you pass `--logs`.

| Case | Observations |
| --- | --- |
| `rf1` | EV01–EV03 partition offsets and batch atomicity, exact body caps, independent subscriptions, lease expiry, retention reset, cold restart, low disk and admission saturation |
| `quorum` | EV07 wrong-broker forwarding, unread commit reply, leader SIGKILL, minority writes, healthy independent group and restart from the original disks |
| `growth` | EV08 RF1 to RF3 through snapshot installation, controller death during a pending move, six-broker placement, partition discovery, drain and identity conservation |
| `backup` | EV10 RF3 snapshot bundle integrity, corrupt image rejection, offline bootstrap under a fresh identity and original checkpoints |
| `tls` | EV10 peer mutual TLS, grant and delivery key overlap across rolling broker restarts |
| `load-1,load-3,load-6` | EV09 warmup and three small measured repetitions per topology, balanced and skewed publishing, fast and slow subscriber lag, event conservation and Docker resource samples |

The throughput numbers come from local Docker runs under these caps, and
Docker's network counters mix peer and client traffic. So the results say
nothing about EBS, traffic between Availability Zones or recovery times in the
cloud.

For the `growth` case, `--fault-image` adds a Python container to the fixture
network. Give it a local image with Python 3.11 or newer. The container relays
the first bytes of a snapshot to a new voter, then pauses the transfer while
the test confirms a partial file exists and kills the voter. Up to 0.15 CPU and
64 MiB of the limits you pass go to this container.
