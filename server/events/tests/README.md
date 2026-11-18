# Events Docker acceptance

Run from the repository root with Python 3.11 or newer and the Events image
built from the checkout under test. The `tls` case also uses OpenSSL to issue
short-lived certificates for the fixture.

```sh
docker info --format '{{.NCPU}} {{.MemTotal}}'
docker stats --no-stream

python3.11 server/events/tests/acceptance.py \
  --image nabedra-events:local \
  --cpus 2 --memory-mib 1536 \
  --output /tmp/events-acceptance.json --logs /tmp/events-acceptance-logs
```

The suite runs cases serially, uses daemon-assigned loopback ports, and
removes each Compose project's containers, network and volumes at case exit.
Each broker gets a distinct named mount. Choose the aggregate CPU and RAM
limits after checking other active Docker jobs. Use `--cases` to execute a
subset. The JSON records successful assertions even if a later check in the
same case fails. Failure logs are written only if requested.

| Case | Observations |
| --- | --- |
| `rf1` | EV01–EV03 partition offsets and batch atomicity, exact body caps, independent subscriptions, lease expiry, retention reset, cold restart, low disk and admission saturation |
| `quorum` | EV07 wrong-broker forwarding, unread commit reply, leader SIGKILL, minority writes, healthy independent group and restart from the original disks |
| `growth` | EV08 RF1 to RF3 through snapshot installation, controller death during a pending move, six-broker placement, partition discovery, drain and identity conservation |
| `backup` | EV10 RF3 snapshot bundle integrity, corrupt image rejection, offline bootstrap under a fresh identity and original checkpoints |
| `tls` | EV10 peer mutual TLS, grant and delivery key overlap across rolling broker restarts |
| `load-1,load-3,load-6` | EV09 warmup and three small measured repetitions per topology, balanced and skewed publishing, fast and slow subscriber lag, event conservation and Docker resource samples |

The throughput numbers describe local, capped Docker runs. Peer and client
traffic share the Docker network counters. The tests cannot establish EBS,
inter-AZ traffic, or cloud recovery targets.

The optional `--fault-image` parameter to `growth` adds a Python container
to the fixture network. Supply a local image with Python 3.11 or newer. The
container relays the first snapshot bytes to a new voter and pauses the
transfer while the observer confirms a partial file and kills the voter.
The fixture reserves up to 0.15 CPU and 64 MiB for this container inside
the supplied aggregate limits.
