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
With the inspection executable, the lifecycle and rolling scenarios
finish with the R02 check. It reads both a queued and a running record
from the surviving worker's RAM after sequentially stopping the other
two workers.
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

The rolling attempt with corrected DNS placement observed a new Pod UID
after deleting `w1-0`. It stopped on the fixture's full-response replay
comparison. The fixture now repeats preparation through the same
operator, compares the prepared identity, and records both replies.
[Rolling attempt](rolling-attempt/trace.jsonl)

The `f5a7d24` run, built after hardening advanced to `b694f75`, replaced
all three workers and checked each retained PVC, mount and enrollment.
The replay returned the same identity with a different Raft apply position.
With the zone A node stopped, the two remaining zones served a verified
read after 41.101 s and acknowledged writes. The held request was present
on both survivors and copied to the replacement before readiness. The
successful-read gap was 41.994 s including the stop. For requests started
while the node was down, nearest-rank p99 across all attempts was
7,017.585 ms for reads and 7,001.494 ms for direct writes.

A repeat with both DNS pods still on the control plane acknowledged 105
direct writes during the 65.703 s node outage. Its independent reads all
failed or timed out before the node was restored. The repeat stopped at
the read watchdog before the final audit.
[Repeated zone trace](repeated-zone/trace.jsonl) ·
[Repeat DNS placement](repeated-zone/coredns-placement.txt)

The same run attached a second PVC, checked its mount before publishing
the pool, and replayed the mount receipt. A host-path resize reported 2 GiB
without a filesystem size change. Nabedra kept its growth claim in
`awaiting_mount`. Both the oversized resize and the oversized new claim
returned CSI `OutOfRange` and the existing data remained readable.
The final audit verified the acknowledged direct writes, then stopped
when polling a same-ID queued retry yielded a terminal
`overloaded/not_applied` receipt for an earlier HTTP 503. The exact combined row count and applied
voter checks did not finish in this run. The 503 carried `storage_unavailable`
with outcome `unknown`. The later receipt resolved that uncertainty.
[Host-path request and cleanup trace](hostpath/trace.jsonl)

Two early Rawfile probes stopped during infrastructure setup. A third run
created three PVC objects, which stayed pending because the plugin rejected
the fixture's `6Gi` reserve setting during startup. The chart documents
`GiB`, and the generator now uses `6GiB`. The corrected provider-only
probe expanded an ext4 PVC from 256 MiB to 512 MiB without replacing the Pod.
`df` grew from 235,431 KiB to 480,868 KiB and the sentinel survived. Filling
the volume returned ENOSPC on the subsequent allocation. [Filesystem trace](rawfile-filesystem/trace.jsonl) ·
[CSI expansion calls](rawfile-filesystem/driver-resize.txt) ·
[Earlier driver output](rawfile-attempts/invalid-capacity-driver.txt)

The native OLTP Rawfile run attached a secondary PVC on a distinct ext4
device and registered it once after replay. Filling that pool reduced
reported available capacity from 882,802,688 to 61,440 bytes, and a further
allocation failed with ENOSPC. The primary PVC expanded online from 1 GiB
to 2 GiB with the same Pod UID and a 1,031,612 KiB filesystem increase.
Nabedra published 2,077,073,408 mounted bytes only after the filesystem grew.
The duplicate mount request returned the original growth claim with one
attempt. The direct-write audit passed for 259 acknowledgments. The queued
audit stopped at a request that had received HTTP 202 and later settled
with `conflict/not_applied` after its original execution authority was fenced.
The audit now continues when an already accepted mutation resolves to this
explicit abort, or when an unknown submission resolves to the recorded
handler-lane refusal. The final table comparison checks that each failed
mutation left its row absent. The historical storage traces ended before that
comparison.
[Native Rawfile trace](rawfile-storage/trace.jsonl)

The first Docker scale attempt reached native preparation but the new
worker refused its mounted volume. The fixture seeded the system-worker
key filename. The data worker requires `.nabedra-data-worker-key`, and
the fixture now writes that file with owner-only permissions.
[Data worker output](scale-attempt/worker-output.txt) ·
[Scale attempt trace](scale-attempt/trace.jsonl)

The corrected worker passed native mount enrollment and reached queue
join. The runtime reported `Unauthorized: Control refused data worker queue join`.
The log does not identify the underlying Control reply. A fourth ready
worker and drain permit were not observed. [Scale rerun trace](scale-retry/trace.jsonl) ·
[Scale rerun worker output](scale-retry/worker-output.txt)

The isolated rolling and R02 run passed. The final table matched exactly
353 rows and the count matched. The audit verified 250 acknowledged
direct writes and settled all 68 queued receipts, of which 58 needed
an identical-ID retry. Both ranges reported the same three applied Data
voters on every worker. After two data nodes stopped in sequence, the
remaining worker still held both the queued and running requests with
their original body and context hashes.
[Rolling and R02 trace](rolling-last-holder/trace.jsonl)

The Linux process tests reran after the hardening merge. Retained restart
verified 78 acknowledged writes, two settled receipts and three applied
Data voters across two ranges. The lost-system-worker replacement test
recovered RF3 in 30.632 s and settled its queued receipt. These tests
ran on Linux without Kubernetes. [CI test transcripts](ci-test-results.txt) ·
[Recovery workflow](https://github.com/nabedra/database/actions/runs/44934576362)

## AZ1 point failover

The Linux [packet-drop fixture](../../../../../server/tests/oltp_blackhole.py)
keeps the workers running. It drops packets in both directions in one
worker's network namespace and probes the Raft leaders from both survivors.
The API and the worker peers use literal IP addresses. The fixture verifies
each acknowledged row and replays the original request identity after the
outage. Its 8 s end-to-end target allows up to 3 s for the old leader lease,
3 s for the election timer, and 2 s for Control authorization, Catalog, and
Timestamp during simultaneous leader loss. Each client attempt keeps the
LIVE3 7 s HTTP timeout, and every read on a surviving Data leader must succeed.

With PR #23 already in the baseline, the survivors agreed on new Data leaders
in 4.542 and 4.725 s and a new Control leader in 5.505 s. The affected point
operations had no success during 14.1 s of packet loss. One range whose leader
survived also timed out after initially serving reads in milliseconds.
[Linux baseline and raw JSONL](https://github.com/nabedra/database/actions/runs/44944730741)

The first candidate, `7bcc902d5885`, bounded gateway attempts and separated
direct API grant renewal from queue readiness. Its kind run still took
52.251 s to complete the first verified independent read and 30.120 s to
acknowledge a direct write started during the outage. The surviving read
leader spent up to 8,388.619 ms in table descriptor lookup. Its Data read
barrier took at most 0.403 ms. The API's point sends topped out at 1,506.050 ms,
and 146 DNS lookups from the API Pod to the two surviving peers succeeded,
with a maximum of 17.472 ms. The audit verified 215 acknowledged direct
writes and both RF3 placements. One queued receipt ended with overload.
[Stage measurements and outcome](az1/initial-candidate.json) ·
[Candidate build and Linux assertions](https://github.com/nabedra/database/actions/runs/44945672868)

The next Linux run, `b4c9403065d9`, bounded direct Catalog reads and kept
the original attempt open while the gateway tried another voter. Affected
Data ranges read again in 5.894 and 6.306 s. Every read on a surviving
leader succeeded, with one request taking up to 4.266 s across a Control
election. Timestamp failover still put the first successful write beyond
6.5 s in two faults. The trace shows that the client tried the silent
Timestamp voter for two seconds again between visits to the survivors.
[Linux packet-drop results](https://github.com/nabedra/database/actions/runs/44947875503)

After shortening the Timestamp probe, `1e6edd4d9e71` served writes in
4.758 s when only the Timestamp leader was lost. With Control, Timestamp
and both Data leaders on the lost node, the two ranges served reads in
6.099 and 6.302 s and writes in 6.838 and 6.908 s. This missed the first
6.5 s assertion. The final 8 s budget accounts for the metadata calls
needed on a cold replacement leader. All point attempts on surviving Data
leaders returned HTTP 200. Dropping a follower alone returned the first
reads in 8–9 ms and writes in 19–20 ms.
[Timestamp failover run](https://github.com/nabedra/database/actions/runs/44948558360)

The final Linux candidate, `b423c2d731dd`, passed the packet-drop assertions.
When Control, Timestamp and both Data leaders shared the lost worker, the
survivors agreed on the Data leaders in 4.636 and 4.859 s, Control in
4.920 s, and Timestamp in 5.278 s. The affected reads completed in
5.913 and 5.963 s. Both write streams acknowledged their first write in
6.114 s. Losing only the Timestamp leader restored writes in 5.612–5.613 s
and surviving Data leaders kept serving reads. With a follower lost,
the first reads completed in 13 ms and first writes in 24–27 ms.
All 523 reads and 384 writes returned HTTP 200, and all acknowledged
writes passed the row and same-ID replay checks.
[Final Linux measurements](az1/linux-final.json) ·
[Final Linux packet-drop assertions](https://github.com/nabedra/database/actions/runs/44949594158)

The checksum-verified release of the same commit passed LIVE3's `zone`
scenario in kind, using the same external SSD as the earlier attempts.
The first verified read completed 5.028 s after zone A stopped. The first
read and write that started after the stop completed at 5.294 and 5.491 s.
Every attempt started during the 22.397 s outage succeeded. Nearest-rank
p99 was 407.574 ms across 61 reads and 616.721 ms across 69 writes.
Both CoreDNS replicas ran on the control-plane node. All 52 DNS samples
resolved the surviving peers, with a maximum of 23.785 ms. The driver
restored the original PVC, verified both RF3 placements, and audited
202 exact rows, 167 acknowledged direct writes and 35 settled receipts.
The session deleted its nodes and released the Docker lane.
[Final kind evidence](az1/kind-final.json) ·
[Raw CI test results](az1/ci-test-results.txt)
