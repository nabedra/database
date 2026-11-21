# SNAP1: Raft snapshot export off the apply path

PERF1 traced the soak stalls of 1.4–3.6 s to Raft snapshot export. Roughly every 5,000 Data entries, each replica ran
`export_snapshot` on OpenRaft's state-machine task. It held the group's application order, and the worker's only query
thread, while it iterated, wrote, fsynced and published a full group file of 140–480 MB.

SNAP1 makes a Raft snapshot a **cut**:
- Under the application order, the cut syncs any unsynced applies, takes one `nabedra-kv` view, and reads the applied
  position, membership and response from that view. It rewrites nothing.
- Policy builds are header-only cuts. OpenRaft keeps only their metadata, which it uses to purge the log.
- A lagging follower gets a fresh cut from `get_current_snapshot`. The replication task writes that cut's file from the
  pinned view, in 1 MiB native jobs. The file is not fsynced and lives under `snapshots/<group>/outgoing/`. The task then
  releases the view.
- The production profiles set `snapshot_policy = LogsSinceLast(1024)` and keep 4,096 entries.

## Soak A/B on runners (relative numbers only)

`[ci snap1]`, run [44915229646](https://github.com/zPy52/nabedra-database/actions/runs/44915229646):
- **Before:** SNAP1's base, 5b6939a (PERF1 merged). **After:** 158fa12.
- ubuntu-24.04, 4 vCPU, NS11 caps, API uncapped.
- NS11 soak: 80,000 rows with 2 KiB payloads, 300 s in five 60 s phases, 32 clients, 500 keys verified.
- A stall window is a run of updates that took more than 250 ms, scheduled less than 1 s apart (`analyze_snap1.py`).

| Soak | Acked / offered | Refused (client cap) | p50 / p99 / max ms | Stall windows > 250 ms | Written per replica per 5,000 entries |
| --- | --- | --- | --- | --- | --- |
| 100/s before | 28,990 / 30,000 | 1,010 (3.37%) | 2.25 / 20.7 / 3,278 | 5, every ~52 s, max 1.4–3.3 s | 907–917 MB |
| 100/s after | 30,000 / 30,000 | 0 | 2.30 / 10.2 / 73 | 0 | 530–536 MB |
| 50/s before | 14,637 / 15,000 | 363 (2.42%) | 2.98 / 140 / 3,518 | 3 of 2.9–3.5 s, plus 2 of ~0.35 s | 525–529 MB |
| 50/s after | 15,000 / 15,000 | 0 | 3.10 / 31.3 / 351 | 2 of 0.30–0.35 s | 325–341 MB |

- Verification found 0 mismatches in all four runs.
- Per-phase p99, 50/s:
  - before: 113 / 2,595 / 5.6 / 2,448 / 2,898 ms
  - after: 140 / 6.1 / 5.9 / 136 / 5.9 ms
- The two short windows at 50/s, at 34.8 s and about 191 s, appear in both binaries. They are not snapshot export.
- What each replica still writes (about 107 KB per update at 100/s) is LSM flush and compaction of 2 KiB-payload updates
  with their MVCC history, plus the journal. 5 s write bursts of 325–525 MB remain. That is the next thing to look at for
  the sustained-write limit.

## Apply stall during an export (regression, local Mac, debug build)

`snap1_o11_applies_proceed_while_a_snapshot_file_is_written`: a 48 MiB group, one query thread, and one synced apply
after another while `export_snapshot` runs.

| | Export | Applies during it | Apply p50 / max |
| --- | --- | --- | --- |
| Base (5b6939a product code) | 3.95 s | 2 | 3.75 s / 3.75 s: **FAILED** ("only 2 applies completed during a 3.95 s export") |
| SNAP1 | 3.35 s | 52 | 57 ms / 154 ms: ok |

The apply p50 is dominated by its own `F_FULLFSYNC` on macOS.

## Files

- `analyze_snap1.py`: stall windows, refusals, p50/p99 and cgroup `io.stat` writes from soak run directories.
- `ci-soak-summary.json`, `ci-soak-runs.txt`: compact outputs of run 44915229646.
