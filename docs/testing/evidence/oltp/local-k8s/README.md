# LIVE3 local kind OLTP

The driver creates one control plane and three worker nodes on Docker
Desktop. The worker nodes have zone labels `a`, `b`, and `c`. The driver
starts the API separately and gives each registered system voter a dedicated
StatefulSet and a `ReadWriteOncePod` PVC. It uses the Docker mount profile and the
[retained-worker restart procedure](../../../../oltp-recovery.md).

The pinned Kubernetes host-path CSI setup gives each zone its own instance of
the sample driver and records the actual Kubernetes `VolumeAttachment`. The
sample driver stores filesystem volumes in directories on the backing
filesystem. The optional Rawfile CSI setup uses ext4 loopback volumes for
actual filesystem growth and per-volume exhaustion. Both setups share the
Docker Desktop VM and the external SSD.
[kind v0.33.0](https://github.com/kubernetes-sigs/kind/releases/tag/v0.33.0) ·
[Host-path CSI v1.19.0](https://github.com/kubernetes-csi/csi-driver-host-path/tree/v1.19.0) ·
[Rawfile CSI v0.15.3](https://github.com/openebs/rawfile-localpv/releases/tag/v0.15.3)

## Reproduce

Download the `live3-oltp` Linux release artifact from an opt-in
`[ci live3]` run and verify `SHA256SUMS`. For the deterministic queue-copy
and locally applied voter assertions, also download and verify
`live3-oltp-test` from an opt-in `[ci live3-fixture]` run of the same server
source. The test executable has private inspection hooks. The separate API
uses the release binary in both modes.

Run the following from the repository root with Python 3.11 and PyYAML.
Set `LIVE3_WORK`, `LIVE3_LANE`, and `LIVE3_REVISION` to the local scratch directory,
the shared Docker lane directory, and the release source SHA.

```sh
scripts/local-kind/prepare-csi.sh "$LIVE3_WORK"
PYTHONDONTWRITEBYTECODE=1 python3.11 scripts/local-kind/acceptance.py \
  --binary "$LIVE3_WORK/live3-oltp/nabedra-db" \
  --test-executable "$LIVE3_WORK/live3-oltp-test/oltp-test" \
  --revision "$LIVE3_REVISION" --scenario lifecycle \
  --csi-source "$LIVE3_WORK" --lane "$LIVE3_LANE" --output "$LIVE3_WORK/lifecycle"
```

Run again with `--scenario storage` and a fresh output directory for the
host-path PVC and CSI error checks. Use `--scenario scale` for the local
Docker data worker and drain permit check. Use `--scenario rolling` or
`--scenario zone` for either lifecycle check on its own. The zone check
places Kubernetes DNS on the control plane before stopping any data node.
To try the independent ext4
volumes, render the pinned chart with Helm and replace `--csi-source` with
`--rawfile-manifest "$LIVE3_WORK/rawfile.yaml"` for the storage run. Use
`--scenario preflight` with that manifest to exercise one CSI volume before
starting Nabedra.

```sh
scripts/local-kind/prepare-rawfile.sh "$LIVE3_WORK"
```

The driver uses a dedicated kubeconfig and a unique kind cluster name,
collects timing and storage observations, and removes the cluster before
releasing the lane. The standard OLTP acceptance runner leaves O18 and O27
incomplete when their real-cloud clauses are unavailable.

## Recorded attempts

The first local control plane reached RF3 worker startup with three bound
PVCs and three attached `VolumeAttachment` records, one per zone. The
separate API failed to open because its peer and HTTP bind settings overlapped.
The fixture now assigns separate ports. This attempt preceded the hardening
merge at `bb6dc32`. [Startup output](startup-retry/trace.jsonl) ·
[Storage snapshot](startup-retry/storage.txt)

The post-merge attempt used the release and test artifacts at
`20c102659f058c6a512cb1b924087032ea07a44d`. Kubeadm timed out creating
the administrator ClusterRoleBinding before any CSI or Nabedra workload
started. The run removed all nodes and released the lane. Peak sampled VM
use was 1,372,762,112 bytes. [Bootstrap output](post-merge-bootstrap/kind-create.txt) ·
[Cleanup trace](post-merge-bootstrap/trace.jsonl)

The next post-merge run reached a separate ready API and three attached
PVC-backed StatefulSet workers. Kubernetes rejected a second writer, and
the worker probes found matching accepted request bodies and context hashes
in all three queues. After the zone A node stopped, independent reads did
not recover during the 65 s watchdog. Both CoreDNS replicas were scheduled
on that node. The corrected fixture puts CoreDNS on the control plane.
[Request and cleanup trace](zone-attempt/trace.jsonl) ·
[CoreDNS pod placement](zone-attempt/pods.txt) ·
[Survivor output](zone-attempt/survivor-output.txt)

Two early Rawfile probes stopped during infrastructure setup. A third run
created three PVC objects, which stayed pending because the plugin rejected
the fixture's `6Gi` reserve setting during startup. The chart documents
`GiB`, and the generator now uses `6GiB`. [Driver output](rawfile-attempts/invalid-capacity-driver.txt) ·
[Pod scheduling](rawfile-attempts/invalid-capacity-scheduler.txt)

The Linux process tests reran after the hardening merge. Retained restart
verified 78 acknowledged writes, two settled receipts and three applied
Data voters across two ranges. The lost-system-worker replacement test
recovered RF3 in 30.632 s and settled its queued receipt. These tests
ran on Linux without Kubernetes. [CI test transcripts](ci-test-results.txt) ·
[Recovery workflow](https://github.com/zPy52/nabedra-database/actions/runs/44934576362)
