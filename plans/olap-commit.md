# OLAP commit protocol

This plan describes the OLAP mutation commit protocol as it ships on `main` (e6b4040b), and carries its design review.
No other plan owns the protocol by name. Its rules come from `plans/resource-aware-scheduling.md` (task S4,
"Run/term fencing and transactional publication/reconciliation", acceptance T01 to T06) and from the catalog-fence
paragraph of `plans/cli-and-serverless.md` (SL03, SL05). Those plans stay the source of the requirements. The shipped-path description comes first, followed by the approved decision-index design. Guarantees:
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

## Persistent decision index

The OLAP-1 fix keeps the exact mutation identity and its terminal response in an immutable UUID index. The request ID
is a client-supplied UUID. An index over all committed decisions can prove the absence of an unused UUID at any
catalog revision. The earlier bounded-window experiment assumed a trusted admission revision. Its model and its
results are historical evidence of the chain budget failure.

### Catalog and index

Catalog format 6 carries either a migration descriptor or an active immutable index root. It retains
the database owner, run and term fence, tables and decision head. Use a copy-on-write radix tree with a maximum of
one bounded page per UUID byte. Each slot names a child page or an entry containing the full UUID and its immutable
decision ID. Bound encoding size and depth. Validate a fetched page against its content address and route prefix.
Use full UUID equality at the terminal entry, then read the decision object and compare the exact request bytes
where the caller supplied a body. Return the original status and response bytes for an exact hit and the existing
identity conflict for a different body. Prove absence from a valid root and path with no matching UUID. If a
referenced page or decision is missing, invalid or unreadable, the lookup returns an error or an unknown outcome
within the existing deadline.

The coordinator prepares from one captured root and names that base in its absence proof. It stages the decision,
the new table objects and the copied index path as immutable objects. It publishes tables and the new index root in
one catalog PUT with the captured ETag. After a definite conflict, re-read the winning root and consult its index
before recomputing. After a lost or ambiguous response, use a fresh root to find the exact decision. If that
decision is absent and the submission can still apply, return `outcome_unknown`. A receipt can report
`abandoned` only after the existing run-fence proof and an authoritative absence lookup. Queue and identified
local mutations use the same index semantics.

| Operation | Object-store cost per attempt |
| --- | --- |
| Look up or prove absence | One catalog GET, up to 16 bounded page GETs, and one decision GET for a hit |
| Publish a fresh mutation | The base lookup, up to 16 private page PUTs, one decision PUT, the new table objects and one catalog CAS |
| Ambiguous response or CAS conflict | One fresh catalog GET and a bounded lookup before settlement or recomputation |
| Migrate | One pass through the frozen decision chain to build the index, resumable beyond the normal lookup budget |

Use one absence proof per root revision in the mutation path. The maximum lookup path depends on the 128-bit UUID
width. The number of live entries and decisions grows with committed identities. Collection of unreachable copied
pages is a separate retention design under OLAP-4.

### Migration and compatibility

1. Deploy readers and writers that support the current catalog formats and the indexed format. Enable migration
   explicitly per database. Check the root has space for the new metadata before freezing it. If preflight sees an
   oversized candidate, leave the legacy root active for a planned catalog size migration.
2. Pause mutation admission and use a catalog CAS to publish the building state, tied to the current owner, run and
   term. The winning CAS freezes the source revision and decision head and invalidates every old-base mutation PUT.
   Current old binaries reject the new format. Compatible participants use the migration state to pause writers.
   The pause may return the existing retryable availability outcome.
3. Validate the immutable source chain through its terminus, including decision IDs and contiguous revision links,
   and build private pages under a migration ID. Checkpoint the source identity, next unprocessed decision and
   candidate index root so maintenance can resume. The checkpoint is outside the serving root. Bind resumption to
   the exact source and owner fence. If a contender or a run change fences the migrator, reload the current root
   and resolve ownership before proceeding. The scan has its own bounded work units and can process a source
   history larger than 4,096 entries or 64 MiB.
4. Publish the complete validated index with a CAS on the building root. Confirm an ambiguous activation by
   reading the current root. Mutation admission resumes once the active index is visible under the current fence.
   A crash or `412` can leave private pages or a resumable checkpoint. No private index supplies absence proofs.
   Use an indexed-format compatible binary for rollback after activation.

Acceptance exercises two UUIDs with a shared radix prefix, same-body and different-body replay across conversion,
an unused UUID beyond both former limits, competing writers, timeout before a late CAS, a lost successful reply,
and migration interruption before activation. Cover format rejection by a legacy writer and its delayed pre-freeze
CAS. Check that receipt reconciliation either returns the original decision or an existing failure outcome that
the fence and lookup justify.

### Index design review

The refinement `specs/olap/index/OlapDecisionIndex.tla` abstracts bounded immutable pages as a map and covers
the publication and migration boundary. `PublishedIndexIsExact` links the serving index to the committed
decisions for OLAP-G01 and OLAP-G02. `LegacyWritersFenced` covers the migration gate for OLAP-G04.
`OneDecisionPerUuid`, `ExactLookup`, and `ReplayIsExact` cover OLAP-G05. `TimeoutRemainsUnknown` covers
OLAP-G03. `MigrationCompletes` relies on the resumable maintenance loop and conditional write resolution.

The safety and liveness baselines start with one committed request, add two fresh request IDs, and use one legacy
and two indexed writers. Each writer gets one attempt, which can stay pending across the format fence, a timeout
or a CAS conflict. The model includes a lost reply and an interrupted private build. One migrator acts at a time.
The freeze and activation actions abstract each root write as a CAS against the current captured state. Migration
checkpoint encoding, page fanout and byte layout, multistage retry loops and the row payload belong to separate
checks. The T1 check is `olap-decision-index`.

Local pinned TLC 1.8.0 checked 67,485 distinct states at depth 19 in each baseline. All 12 actions were reached
and all ten mutants were caught. Final-head Actions evidence is pending.

| Mutant | Property | Counterexample states |
| --- | --- | --- |
| `incomplete_migration`, `index_update_omitted` | `PublishedIndexIsExact` | 3, 8 |
| `old_accepts_new_root`, `stale_legacy_cas` | `LegacyWritersFenced` | 7, 5 |
| `no_catalog_match` | `OneDecisionPerUuid` | 10 |
| `only_match_prefix`, `missing_page_absent` | `ExactLookup` | 8, 5 |
| `timeout_final` | `TimeoutRemainsUnknown` | 4 |
| `lost_reply_old_root` | `ReplayIsExact` | 8 |
| `no_migration` | `MigrationCompletes` | 9, lasso |

## Design review

Reviewed as a shipped design on 2026-11-26 (wave rv, task rv-olap). The shipped-path spec is `specs/olap/OlapCommit.tla`. The
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
| T1 safety, `OlapCommit.window.cfg` | the historical bounded decision window experiment (`Variant = "window"`, `Window` 1) |
| T1 liveness, `OlapCommit.live.cfg` | the same bounds, no symmetry, historical window experiment with `HistoryBudget` 1 |
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

The T1 check fails on coverage because `GcDelete` and the reader steps run only in the unfinished collection
baseline (a stale-receipt step that only the mutant enabled was fixed in 333795de). Re-run at 41f8b577 (Actions run
45811524492): the same baseline results (`OlapCommit.gc.cfg` 1,274,651 and `OlapCommit.live.cfg` 565,818 states left
at 900 s); the history and collection mutants now run at smaller fault budgets with their own controls and are
caught (`prefix_history_budget` 17 states, lasso, control 10,100 states; `gc_ignores_pins` and `gc_ignores_inflight`,
control 122,140 states); the `unpinned_read` control (one reader at the L2 bounds) stopped at 902 s after passing in
608 s on the earlier run, so the mutant was not counted there. The T2 check `olap-olap-commit-long` runs the same
baselines with a 5-hour budget: pending. The collection and liveness baselines need an abstraction to fit T1; their
bounds are not cut.

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
| `G05_Progress` | `prefix_retry_limit` (SCHED-3) | 14 (lasso); 17 in run 45811524492 |
| `G05_Progress` | `prefix_history_budget` (OLAP-1) | 17 (lasso), run 45811524492, at reduced fault budgets with its own control |
| `GC_NeverDeletesReferenced` | `gc_ignores_pins` | 15, run 45811524492, reduced fault budgets |
| `GC_NeverDeletesReferenced` | `gc_ignores_inflight` | 7, run 45811524492, reduced fault budgets |

### Counterexamples

Three design findings on the shipped protocol, each a mutant that restores the shipped behavior while the baseline
holds the candidate fix:

- **Unbounded decision chain, bounded lookup** (`prefix_history_budget`, `G05_Progress`). Every new request ID proves
  absence by walking the whole chain, which nothing truncates; past 4096 decisions or 64 MiB of decision bytes, every
  mutation fails before it executes. The approved fix is the persistent decision index above.
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
