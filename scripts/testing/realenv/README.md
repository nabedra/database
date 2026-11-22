# Local real-environment checks

These checks run against Docker and kind. Python 3.11 or later is required.
The host needs Docker and its Compose plugin for K03, and kind and kubectl
for EV06 and EV07. The kind node image runs Kubernetes 1.34, so use a kubectl
client within one minor version.

## Events on kind

The test uses the Events image built by the repository's Docker CI job. Run
this in the same environment as the image, or import the built image into
local Docker first.

```sh
python3.11 -m scripts.testing.kind.install --output /tmp/nabedra-bin/kind
NABEDRA_KIND=/tmp/nabedra-bin/kind python3.11 -m scripts.testing.kind.suite_events \
  --image nabedra-events:ci --history /tmp/nabedra-results/kind-events.jsonl
```

The suite creates and deletes its own kind cluster. It uses three worker nodes
by default, plus a control plane. It checks a retained PVC after forced Pod
deletion and publishes through a new leader while the previous leader's node
is paused. For the RV-REP #3 wiped-disk shape, the suite scales down the last
ordinal, deletes its PVC and waits for the old PV to disappear. It starts
that ordinal with a new PVC and broker ID, moves the affected groups to it,
and confirms it can participate in a quorum with only one original broker.
The queue check compares the final replay with every acknowledged publish,
including event IDs, contents and contiguous offsets from 1. The JSON-lines
history can be fed to nabedra-check once its queue checker has a CLI.

To manage a test cluster by hand, use
`python3.11 -m scripts.testing.kind.cluster up --nodes 3`, then
`load-image <image>` or `down` with the same module. `--nodes 4` adds a
fourth worker. The cluster uses kind's default `standard` local-path
StorageClass.

The OCI image and the kind release binary are pinned. The ORM
`KubernetesProvider` creates individual worker Pods and bootstrap Secrets.
It has no StatefulSet, Service or PVC render path. The minimal Events
renderer uses the same Kubernetes Pod fields for images, environment,
mounts and resource limits and adds the StatefulSet and Services needed for
broker identity and storage.

## Cache cluster

Run `scripts/cache-acceptance/cluster.sh` with `NABEDRA_CACHE_BINARY`
pointing at the Linux Cache executable and `NABEDRA_CACHE_ORM_ARTIFACTS`
pointing at the directory that contains the existing Redis client wheel
`redis-6.4.0-py3-none-any.whl`. The wrapper retains its default artifact
paths and accepts an output directory as its first argument. K03 writes
`K03.jsonl` next to the existing evidence and metrics.

The harness waits for committed controller placement, worker metrics and
routed RESP replies. These interfaces lack a blocking watch, so the test
retries its readiness probes with bounded backoff.

## Faults and history

Use a single `Nemesis` per fault stream and a distinct process number per
client stream. For Docker network faults, use containers owned by the fixture
and supply the original aliases or static address as `connect_options`
when the fixture depends on them. The netem sidecar shapes the container's
outgoing traffic on `eth0` by default. The Docker host kernel must support
`sch_netem`. Call `heal_all()` before disposing of a fixture to resume
stopped processes, restore network links and remove network rules.

The kind fault helper uses IPv4 node addresses and the peer Pod CIDR. PVC
wipe applies to the final ordinal of a StatefulSet with retained claims.
SIGKILL, Pod deletion and disk wipe remove state that a generic heal cannot
restore. The suite controls when to restart and re-enroll those targets.

Cache's Linux ClockBound mode reads through `ClockBoundClient::new()` with
a per-thread shared-memory mapping. K03 uses a shared Docker kernel clock.
Tests with injected ClockBound error bounds still need a shared-memory
fixture.

## Weekly mixed images

`python3.11 -m scripts.testing.realenv.upgrade_matrix` lists local image
slots for the previous two Events releases and head. Import the release
images and run with `--images oldest,previous,head` to start three brokers
on the oldest build, replace two of their broker containers in place, and
check replay with all three versions serving. This is the first weekly
mixed-version case. The rolling downgrade and version permutations remain
to be added.
