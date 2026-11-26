# OLAP commit protocol

This plan describes the OLAP mutation commit protocol as it ships on `main` (e6b4040b), and carries its design review.
No other plan owns the protocol by name. Its rules come from `plans/resource-aware-scheduling.md` (task S4,
"Run/term fencing and transactional publication/reconciliation", acceptance T01 to T06) and from the catalog-fence
paragraph of `plans/cli-and-serverless.md` (SL03, SL05). Those plans stay the source of the requirements. This file
states how the shipped code meets them, so the spec and code review have one description to check. Guarantees:
OLAP-G01 to OLAP-G05 in `docs/testing/guarantees/olap.md`. Module `olap`, level L2.

## Objects in the store

One namespace per database holds three kinds of object.

- **Table files.** Immutable Parquet partitions at `ObjectKey::partition(table_id, commit_id, ordinal)`
  (`server/src/storage.rs:64`). The commit ID is a fresh UUID per attempt, so two attempts never share a key. Every
  file is written with `WriteCondition::Create`, which is `If-None-Match: *` on S3 (`server/src/storage/s3.rs:202`).
- **Decision records.** Immutable `decisions/<commit_id>.json` (`server/src/storage/decision.rs`). A decision holds the
  request ID, the exact request body, the response status and body, its revision and its parent decision. Every
  mutation publishes one, including one that ends in a 4xx result, which publishes an unchanged table set.
- **The catalog root** `catalog.json` (`server/src/storage/decision.rs`, `ClusterCatalog`). It holds the format
  version, the owner cluster (serverless, format 5), the revision, the table entries, the head of the decision chain
  and the fence `{run_id, scheduler_term}`. It is the only mutable object, written with `If-Match` on its ETag.

The visible state of a database is a function of the current root only. Files and decisions that no root reaches are
invisible: nothing lists the bucket to find data.

## Commit of one mutation

The queue owner dispatches a mutation attempt with the fence captured from admitted work (`peers.fence`). The
coordinator (`server/src/node/coordinator.rs` `execute_inner`, up to three attempts under one 60 s deadline):

1. Loads the root and checks its owner cluster and that its fence equals the captured fence (`:461-485`).
2. Looks up the request ID in the root's decision chain (`:486-500`). Found with the same body returns the original
   result; found with another body is a conflict; absence is proved only at the chain's null terminus.
3. Computes the result against the loaded tables, resolving the table by UUID (`resolve`, `:631`).
4. Prepares the candidate root and decision (`PreparedPublication::prepare_fenced` or `prepare_owned`): the base must
   carry the captured fence, the decision's parent and base revision must be the base's head and revision, and at
   most one table changes.
5. Uploads the new table files (`Command::UploadStaged`), then publishes (`server/src/storage/publication.rs`):
   looks up the chain again, creates the decision object, then PUTs the root `If-Match` the base ETag.
6. Classifies the PUT:
   - success: the decision is the result;
   - `412`: reloads the root and searches its whole chain. Found is the result; absent is `NeedsRecompute`, and the
     coordinator returns to step 1 with a fresh root;
   - any other error or the deadline (ambiguous): reloads and searches. Found is the result; anything else is
     `Unknown`, returned as `outcome_unknown`. An ambiguous PUT is never reported as `not_applied`.

## Fences

`Storage::fence_cluster_catalog` (`server/src/storage/history.rs:375-415`) is one conditional root PUT that changes
only the fence and keeps tables and history. A takeover raises the scheduler term within a run; a cold start sets a
new run ID. The owner fences a database before it dispatches to it (`owned_catalog`,
`server/src/node/queue_runtime.rs:642-671`). Because the fence lives in the same root as the tables, a PUT based on an
older ETag fails after the fence, and an attempt captured under an older fence fails step 1 or step 4 even when it
re-reads the root.

## Receipts

A receipt for the current run comes from the replicated queue record, then from the decision chain while the record is
reconciling. A receipt for another run (`Command::Lookup`, `queue_runtime.rs:1473-1617`) answers from the decision chain
of the database's catalog, and `abandoned` when the decision is absent.

## Limits

The chain lookup reads one decision object per step, sequentially, within 4096 records, 64 MiB and the attempt's
deadline (`coordinator.rs:450`; 30 s for receipts). Nothing truncates the chain. Nothing deletes table files,
decisions or roots: superseded and orphaned objects stay in the bucket.

## Design review

Reviewed as a shipped design on 2026-11-26 (wave rv, task rv-olap). One TLA+ spec, `specs/olap/OlapCommit.tla`. The
findings, with fixes and code locations, are in the review findings file `olap.md` of the wave (OLAP-1 to OLAP-n);
the bug records are in `docs/testing/bugs/`.

### Guarantees

| Guarantee | Properties | Notes |
| --- | --- | --- |
| OLAP-G01 | `G01_NoTornCommit`, `G01_ReaderSnapshot`, `GC_NeverDeletesReferenced` | A reader pins one root. Collection is modelled only as a candidate rule (`Gc = "safe"`), since nothing is deleted today. |
| OLAP-G02 | `G02_NoLostUpdate`, `G02_LinearHistory` | The rows of the current root are exactly the requests in its chain. |
| OLAP-G03 | `G03_NoFalseNotApplied`, `G03_TerminalIsCommitted` | Ghost `claims` records every `not_applied` or `abandoned` answer. |
| OLAP-G04 | `G04_FenceMonotonic`, `G04_NoStaleCommit`, `G04_CommitStaysVisible` | Action properties. |
| OLAP-G05 | `G05_OncePerRequest`, `G05_SameResult`, `G05_Progress` (liveness) | `Expired` is the one defined result past a retention window (candidate fix only). |

Not modelled: query semantics (OLAP-G06), result encoding and size limits (pure computation), table DDL as a separate
action (a mutation that changes the table set is a commit like any other; table identity is bound by UUID in
`resolve`), multi-table mutations (the code refuses them in `prepare_inner`), and the old-format writer of a
migration (R-olap-15: `prepare` refuses a fenced root, so a current binary cannot drop the fence; an older binary is
out of scope).

### Topology

```text
 client --HTTP--> API --queue replication--> workers (RAM queue)      scheduler owner (term) = one worker
                                               | coordinator attempt
                                               v
          object store: table files (Create), decisions (Create), catalog.json (If-Match, fence inside)
```

| Channel or store | Modelled faults | Out of scope |
| --- | --- | --- |
| Coordinator to store | A root PUT applied with its reply lost; a PUT delayed past the writer's deadline and applied later; concurrent PUTs from the same base | Store inconsistency (S3 is strongly consistent) |
| Writers | Crash at any step, losing the attempt; three writers race | Byzantine writers |
| Queue (RAM) | Survives a writer crash; lost on a cold start (every worker stops), which abandons queued work | Partial replication (the queue spec owns it) |
| Scheduler owner | Takeover (new term) with old attempts still running; cold start (new run) with PUTs still pending | Clock drift: no step reads time except as an event |
| Client | One replay of a settled request, in the same run or after a cold start; receipt polls | |

### Spec

| | Value |
| --- | --- |
| Spec | `specs/olap/OlapCommit.tla`, model `MCOlapCommit.tla` |
| T1 safety, `OlapCommit.cfg` | 3 writers, 2 requests, 2 partitions, 1 crash, 1 lost reply, 1 delayed PUT, 1 takeover, 1 cold start, 1 replay, `HistoryBudget` 2, symmetry on writers and requests |
| T1 safety, `OlapCommit.gc.cfg` | the same plus 1 reader and the candidate collection rule |
| T1 safety, `OlapCommit.window.cfg` | the candidate bounded decision window (`Variant = "window"`, `Window` 1) |
| T1 liveness, `OlapCommit.live.cfg` | the same bounds, no symmetry, candidate window with `HistoryBudget` 1 |
| Fairness | weak fairness on `ScheduleAttempt` (the queue pump re-evaluates every 100 ms and on each settlement), on each attempt's steps (the coordinator task runs to its 60 s deadline, then `PutTimeout`), on `StoreApply` (the store evaluates or drops every accepted PUT) and on `FenceClusterCatalog` (startup and takeover retry the fence before dispatch). None on faults, clients, receipts, readers or collection. |
| State constraints | none; every fault has a budget and commit IDs are reused only when nothing refers to them |
| T check | `olap-olap-commit` |

Abstractions: the decision object and the root PUT are one step (a decision no root reaches is invisible); a
reply is folded into the attempt's end; an unawaited PUT whose base is no longer the root is pruned, since roots never
repeat. Known gap: the liveness baseline assumes an unknown outcome is retried until it resolves. The shipped queue
stops after three attempts (finding F2 of the earlier review, mutant `prefix_retry_limit`), which the queue spec owns.

### Results

TLC v1.8.0, pinned image, Actions run 45800777105 at 0f075ab3 (check `olap-olap-commit`, T1, 900 s per run).

| Run | Result | Distinct states | Depth |
| --- | --- | --- | --- |
| `OlapCommit.cfg` (L2 safety) | pass, exhaustive | 2,100,196 | 61 |
| `OlapCommit.window.cfg` | pass, exhaustive | 2,765,474 | 64 |
| `OlapCommit.gc.cfg` | not-run: stopped at 900 s, 1,611,449 states left on the queue | | |
| `OlapCommit.live.cfg` | not-run: stopped at 900 s, 776,145 states left on the queue | | |
| reader control (`unpinned_read` control: one reader, no collection) | pass, exhaustive | 18,335,430 | |

The T1 check fails on coverage because `GcDelete` and the reader steps run only in the unfinished collection baseline,
and a stale-receipt step that only the mutant enabled (fixed in 333795de). The T2 check `olap-olap-commit-long` runs
the same baselines with a 5-hour budget: pending. The collection and liveness baselines need an abstraction to fit
T1; their bounds are not cut.

### Mutants

Counterexample lengths in states, from the same run.

| Property | Mutant | Counterexample (states) |
| --- | --- | --- |
| `G01_NoTornCommit` | `root_before_uploads` | 7 |
| `G01_ReaderSnapshot` | `unpinned_read` | 11 |
| `G02_NoLostUpdate` | `loser_keeps_files` | 17 |
| `G02_LinearHistory` | `no_cas` | 11 |
| `G02_LinearHistory` | `loser_reput_stale` | 14 |
| `G03_NoFalseNotApplied` | `timeout_not_applied` | 8 |
| `G03_NoFalseNotApplied` | `prefix_unfenced_receipt` (finding OLAP-2) | 9 |
| `G03_NoFalseNotApplied` | `prefix_stale_receipt` (SCHED-11) | 12 |
| `G03_TerminalIsCommitted` | `ok_on_conflict` | 12 |
| `G04_FenceMonotonic` | `fence_put_unconditional` | 7 |
| `G04_NoStaleCommit` | `fence_outside_root` | 10 |
| `G04_NoStaleCommit` | `rebase_adopts_term` | 10 |
| `G04_CommitStaysVisible` | `cold_start_blind_overwrite` | 11 |
| `G04_CommitStaysVisible` | `no_cas_rollback` | 13 |
| `G05_OncePerRequest` | `skip_lookup` | 15 |
| `G05_SameResult` | `skip_lookup_result` | 17 |
| `G05_SameResult` | `prefix_local_unknown` (SCHED-12) | 12 |
| `G05_Progress` | `prefix_retry_limit` (SCHED-3) | 14 (lasso) |
| `G05_Progress` | `prefix_history_budget` (OLAP-1) | not counted: its control is the unfinished liveness baseline |
| `GC_NeverDeletesReferenced` | `gc_ignores_pins`, `gc_ignores_inflight` | not counted: their control is the unfinished collection baseline |

### Counterexamples

Three design findings on the shipped protocol, each a mutant that restores the shipped behavior while the baseline
holds the candidate fix:

- **Unbounded decision chain, bounded lookup** (`prefix_history_budget`, `G05_Progress`). Every new request ID proves
  absence by walking the whole chain, which nothing truncates; past 4096 decisions or 64 MiB of decision bytes, every
  mutation fails before it executes. The fix changes the root format and adds an `expired` outcome, so it waits for
  the user; this plan does not change.
- **Receipt `abandoned` from an unfenced catalog** (`prefix_unfenced_receipt`, `G03_NoFalseNotApplied`). The
  receipt path must fence the database for the current run, or see the current run's fence, before it reports
  `abandoned`. Plan rule, added here: *an `abandoned` receipt is valid only against a root that carries a newer fence
  than the request's run.*
- **A stale process answers for another run** (`prefix_stale_receipt`, `G03_NoFalseNotApplied`): a process compares
  the fence of the root it loads with its own before it answers for another run. Plan rule, added here.

### Cost

Per mutation on one table with P new partition files, a chain of k decisions, and no contention. Counted from the code
paths the spec's actions name.

| Operation | GET | PUT | Bytes written | Notes |
| --- | --- | --- | --- | --- |
| Mutation, success | 1 root + 2k decisions | P files + 1 decision + 1 root | files + decision (request and response body) + whole root | the chain is walked twice on the same base: `coordinator.rs:489` and `publication.rs:151` |
| CAS loser, per extra round | 1 root + up to 2k' + 1 | P' + 2 | as above | at most three rounds in one 60 s deadline |
| Ambiguous PUT | 1 root + k | 0 | 0 | then `outcome_unknown` |
| Fence | 1 root | 1 root | whole root | |
| Receipt, old run | 1 root + k | 0 | 0 | |

The plan sets no per-commit target. The cost of a mutation grows linearly with the database's whole write history.

### Independent read

The read is `wave-dr/reads/olap.md` (17 scenarios, 2026-11-24). The plans it read have not changed since.

| Read ID | Encoding |
| --- | --- |
| R-01 | `PutDelayTimeout`, `Ambiguous`, `RecoverPublication` (absent after an ambiguous PUT is unknown); mutant `timeout_not_applied` |
| R-02 | `RecoverPublication` searches the fresh chain before recomputing; mutants `skip_lookup`, `skip_lookup_result` |
| R-03 | `LoadAndLookup` checks the root fence against the attempt's fence; mutant `rebase_adopts_term` |
| R-04 | `ScheduleAttempt` requires `Fenced`; a takeover leaves old attempts running |
| R-05 | `ColdStart` keeps pending PUTs; `FenceClusterCatalog` re-reads after a failed fence; mutant `cold_start_blind_overwrite` |
| R-06 | `ClientReplay` after a cold start; the durable lookup returns the original decision (`G05_OncePerRequest`) |
| R-07 | Not modelled as a step: keys are unique per attempt and written with `Create` (`storage.rs:64`, `s3.rs:202`) |
| R-08 | `GcDelete` under the candidate rule; mutants `gc_ignores_inflight`, `gc_ignores_pins` |
| R-09 | `RecoverPublication` recomputes from the fresh root; mutants `loser_reput_stale`, `loser_keeps_files` |
| R-10 | Not modelled: `resolve` binds the table by UUID (`coordinator.rs:631`) |
| R-11 | `HistoryBudget` and `Variant`; mutant `prefix_history_budget` (finding OLAP-1) |
| R-12 | `Expired` result in the window variant; the oversized-result fallback publishes no table change (`coordinator.rs:570-575`) |
| R-13 | Three writers race one request after a takeover; `G05_OncePerRequest` |
| R-14 | `LookupReceipt`; mutants `prefix_unfenced_receipt`, `prefix_stale_receipt` |
| R-15 | Not modelled (see above) |
| R-16 | Not modelled: `prepare_owned` binds the owner cluster to the same root and ETag |
| R-17 | `UploadStaged` crashes between files; mutant `root_before_uploads` |
