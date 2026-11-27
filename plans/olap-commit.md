# OLAP commit protocol

This plan describes the OLAP mutation commit protocol as it ships on `main` (e6b4040b), and carries its design review.
No other plan owns the protocol by name. Its rules come from `plans/resource-aware-scheduling.md` (task S4,
"Run/term fencing and transactional publication/reconciliation", acceptance T01 to T06) and from the catalog-fence
paragraph of `plans/cli-and-serverless.md` (SL03, SL05). Those plans stay the source of the requirements. It describes the protocol as fixed in wave rv phase 2 (task fx-olap): the decision history is an immutable UUID index and catalog format 6 is the only root format. Guarantees:
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
  version (6), the owner cluster (serverless only), the revision, the table entries, the head of the decision
  history, the root page of the decision index and the fence `{run_id, scheduler_term}`. It is the only mutable
  object, written with `If-Match` on its ETag.
- **Decision index pages.** Immutable `decision-index/<sha256>.bin` (`server/src/storage/decision_index.rs`), see
  "Persistent decision index".

The visible state of a database is a function of the current root only. Files and decisions that no root reaches are
invisible: nothing lists the bucket to find data.

## Commit of one mutation

The queue owner dispatches a mutation attempt with the fence captured from admitted work (`peers.fence`). The
coordinator (`server/src/node/coordinator.rs` `execute_inner`, up to three attempts under one 60 s deadline):

1. Loads the root and checks its owner cluster and that its fence equals the captured fence (`:461-485`).
2. Looks up the request ID in the root's decision index (`:486-500`). Found with the same body returns the original
   result; found with another body is a conflict; absence is proved only by a validated index path.
3. Computes the result against the loaded tables, resolving the table by UUID (`resolve`, `:631`).
4. Prepares the candidate root and decision (`PreparedPublication::prepare_fenced` or `prepare_owned`): the base must
   carry the captured fence, the decision's parent and base revision must be the base's head and revision, and at
   most one table changes.
5. Uploads the new table files (`Command::UploadStaged`), then publishes (`server/src/storage/publication.rs`):
   looks up the index again (from the page cache), uploads the copied index path, creates the decision object,
   then PUTs the root `If-Match` the base ETag.
6. Classifies the PUT:
   - success: the decision is the result;
   - `412`: reloads the root and searches its index. Found is the result; absent is `NeedsRecompute`, and the
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
reconciling. A receipt for another run (`Command::Lookup`, `queue_runtime.rs`) answers from the decision history
of the database's catalog. It answers `abandoned` only when the decision is absent from a root that carries the
current run's fence (OLAP-2): a root still fenced by an older run is fenced first, as dispatch does
(`owned_catalog`), and looked up again; without that proof the receipt answers `storage_unavailable`, which is
retryable. The fence makes every pending PUT of the older run fail its `If-Match`.

## Limits

Each lookup runs within the caller's budget (4096 records, 64 MiB and the attempt's deadline, `coordinator.rs:450`;
30 s for receipts). Through the decision index below it reads at most 16 pages and one decision; the shipped chain
walk, which read one decision per step until the null terminus, is gone. Nothing deletes table files,
decisions, index pages or roots: superseded and orphaned objects stay in the bucket (OLAP-4).

## Persistent decision index

The OLAP-1 fix keeps the exact mutation identity and its terminal response in an immutable UUID index. The request ID
is a client-supplied UUID. An index over all committed decisions can prove the absence of an unused UUID at any
catalog revision, so replay stays indefinite and no new outcome is needed. The earlier bounded-window experiment
assumed a trusted admission revision. Its model and its results are historical evidence of the chain budget failure.

### Catalog and index

Catalog format 6 holds the owner cluster (optional), the run and term fence (required), the tables, the revision,
the decision head and `decision_index`: the SHA-256, in lowercase hex, of the index root page, present exactly when
the decision head is.

The index is the canonical radix tree over the 16 bytes of the request UUID (`server/src/storage/decision_index.rs`).
A page at depth `d` has up to 256 slots keyed by byte `d`. A slot holds an entry, the full request UUID and its
decision UUID, when exactly one indexed UUID has that prefix, and otherwise a child page at depth `d + 1`. The tree is
a function of its entry set, so every writer that indexes the same decisions writes the same bytes. A page is a
fixed-width binary record of at most 8,712 bytes, stored once with `Create` at `decision-index/<sha256>.bin`. A
reader checks the hash of the bytes, the depth, the order of the slots, the route prefix of every entry and the
canonical shape before it uses a page. A referenced page that is missing or invalid is an error, never absence.

A lookup descends from the root page to the slot of the request's next byte: an empty slot or an entry with another
UUID proves absence; an entry with the same UUID names the decision, which is read and checked (its ID, its request
ID and a revision not above the root's), and its exact request bytes are compared when the caller supplied a body.
The original status and response bytes are returned for an exact hit and the identity conflict for a different
body. Each `Storage` keeps validated pages in a cache of 1 MiB keyed by content hash; a hit is checked again against
its route prefix, and the request budget counts only uncached reads.

A commit prepares its candidate from one captured root. The lookup before publication returns the path it read, the
commit copies that path with its own entry (adding a child page where two UUIDs share a prefix), and uploads the new
pages, then the decision, then the root with one PUT `If-Match` on the captured ETag. The new index is the base's
exact index plus one pair, so the conditional write keeps it exact. After a definite conflict the commit re-reads
the winning root and consults its index before it recomputes; after a lost or ambiguous response a fresh root must
show the exact decision, else the outcome stays `outcome_unknown`. A receipt can report `abandoned` only after the
run-fence proof and an authoritative absence lookup. Queue and identified local mutations use the same code.

| Operation | Object-store cost per attempt |
| --- | --- |
| Look up or prove absence | One catalog GET, at most 16 page GETs (none for cached pages), one decision GET for a hit |
| Publish a fresh mutation | The lookup, at most 17 page PUTs (the copied path, usually 2 or 3), one decision PUT, the table objects, one catalog CAS |
| Ambiguous response or CAS conflict | One fresh catalog GET and an indexed lookup before settlement or recomputation |

Measured on the in-memory store after a 5,000-decision history (`olap_1_mutations_after_a_long_history_cost_bounded_requests`
in `server/tests/history.rs`): see the OLAP-1 row of the fix report. Collection of unreachable pages, chunks and
copied paths is the separate retention design under OLAP-4.

### One format

Catalog format 6 is the only root format (user decision 2026-11-27: no system uses the database yet, so breaking
changes are allowed and no compatibility layer is kept). A root of any other format, including the fenced formats 4
and 5 and the unfenced format 3, is refused with `UnsupportedCatalog` before its fields are read, and nothing is
written; there is no migration. A brand new namespace gets an empty format 6 root, fenced in the same `Create`. The
`--migrate-catalog` flag and the `database migrate` command now refuse with that reason; removing them from the CLI
is left to the owners of those files.

### Index design review

The refinement `specs/olap/index/OlapDecisionIndex.tla` abstracts bounded immutable pages as a map and covers the
publication boundary. `PublishedIndexIsExact` links the serving index to the committed decisions for OLAP-G01 and
OLAP-G02. `OneDecisionPerUuid`, `ExactLookup`, and `ReplayIsExact` cover OLAP-G05. `TimeoutRemainsUnknown` covers
OLAP-G03.

The first draft (commit `8863bf0f`) also modelled a migration from the legacy chain with a separate building root,
a legacy writer and format refusal by old binaries; fx-olap first replaced the building state with a migration in
the commit's own conditional write. With a single format, both are gone: the model has no legacy writer, no
migration actions, and no `LegacyWritersFenced` or `MigrationCompletes`.

The safety baseline starts with one committed request, adds two fresh request IDs, and uses three writers. Each
writer gets one attempt, which can stay pending across a timeout or a CAS conflict. The model includes a lost
reply. Page fanout and byte layout, retry loops and the row payload belong to the unit and integration tests. The
T1 check is `olap-decision-index`.

Local pinned TLC 1.8.0 (2026-11-27, fx-olap): `OlapDecisionIndex.cfg` 60,714 distinct states at depth 13, all
actions covered (minimum 9,506), all six mutants caught. Actions evidence is in the fx-olap report.

| Mutant | Property | Counterexample states |
| --- | --- | --- |
| `index_update_omitted` | `PublishedIndexIsExact` | 4 |
| `no_catalog_match` | `OneDecisionPerUuid` | 7 |
| `only_match_prefix`, `missing_page_absent` | `ExactLookup` | 5, 3 |
| `timeout_final` | `TimeoutRemainsUnknown` | 4 |
| `lost_reply_old_root` | `ReplayIsExact` | 5 |

## Capacity plan

The review found four storage limits whose full fix is a redesign (findings OLAP-3, OLAP-4, OLAP-6 and OLAP-8 of
the wave rv review). Breaking changes are allowed (user, 2026-11-27), so none of them waits for a contract decision
any more; they wait on scope. Their local parts are fixed; the designs below are the next steps, each with the
measurement that justifies it. None of them is implemented yet.

### Per-table manifests (OLAP-3)

The root is one JSON document, capped at 1 MiB and rewritten whole on every commit. Measured with
`ClusterCatalog::encode`: an empty root with one 10-column table is 634 bytes, each partition file entry about 198
bytes, so about 1,900 tables of 10 columns, 370 tables of 128 columns or 5,000 partition files fill it. Every commit
also uploads the whole root: a 500 KB root costs 500 KB of PUT per one-row insert.

Fixed now: a commit whose candidate root would exceed the cap publishes a definite `limit_exceeded` decision with
unchanged tables (coordinator and local engine), so the client gets a replayable 413 instead of
`outcome_unknown`.

Design for later: format 7 keeps in the root only the table directory (name, UUID, schema digest) and, per table, the
content hash of an immutable table manifest object that lists its partition files. A commit changes one table, so it
writes one manifest and a root that grows with the number of tables only. Readers pin the root and the manifest
hashes, which also makes queue pins small (OLAP-6). Format 7 replaces format 6 outright; older roots are refused.

Why it is not done in fx-olap: serverless grants are per request and keep no cache between credential snapshots
(`server/src/storage/registry.rs`), so a reader that loads every manifest eagerly pays one GET per table on every
request, where it pays one root GET today. The design therefore needs lazy, per-table catalog access, which changes
the in-memory catalog that the coordinator, the stream runtime, queue pins and the local engine all read (files
owned by other wave tasks). Measure before committing to it: root bytes, commit latency and GETs per request for
100, 1,000 and 5,000 files and 10, 100 and 1,000 tables, before and after.

### Collection of unreachable objects (OLAP-4)

Nothing deletes table files, decisions, index pages or roots. A one-row insert into a one-partition
10 MiB table leaves a 10 MiB orphan per commit (1M commits: about 10 TB while the live data stays 10 MiB).

Design for later, a garbage-collection contract:

- An object may be deleted only when no current root reaches it, no pinned root reaches it (queue pins, stream pins
  up to 30 minutes) and it is older than a grace period longer than the longest attempt (the 60 s mutation deadline,
  plus the 30-minute stream pin, plus clock margin). The spec's candidate rule `Gc = "safe"` with the mutants
  `gc_ignores_pins` and `gc_ignores_inflight` states this.
- Candidates come from listing `tables/<id>/` and `decision-index/`; the listing only proposes, reachability from
  the roots decides. Decisions are never collected while a replay can name them, which with indefinite replay means
  a decision object is kept for as long as the index names it; superseded index pages are collectable.
- Breaking changes are allowed, so the documented promise that nothing is deleted can change with this contract.

Why it is not done in fx-olap: the reclaimer needs a listing call on the `ObjectStore` trait and every backend (only
test helpers list today), a process that runs it (the scheduler runtime, owned by fx-sched) and the set of live pins
across the cluster, and the commit model's `gc` baseline does not finish within the T1 budget yet.

Measure first: bucket bytes over reachable bytes for a steady insert workload, and the cost of one reclaimer pass per
10,000 objects.

### Pins by reference (OLAP-6)

Each request start copies the encoded root (up to 1 MiB) into its queue record and replicates it to every member.
With format 7 a pin is the root's ETag plus the table manifest hash it reads, about 100 bytes. Until then the
replicated bytes per read are the root size times the number of other members. The lock and the serial copy at
request start belong to the scheduler (SCHED-7); measure admission throughput with a 500 KB root and 8 workers
before and after.

### Ranged reads and a footer cache (OLAP-8)

Every get, collect and count downloads and decodes whole partition objects: a point get on a one-partition 64 MiB
table reads 64 MiB. The `ObjectStore` trait has only a whole-object GET. Design for later: a ranged GET on the trait
(S3 `Range`, the HTTP store's `Range` header), a footer and row-group statistics read through a cache keyed by the
immutable partition key, and point gets that read one row group. A smaller default partition target cuts the cost
first and needs no format change. Measure point-get latency and bytes read against partition size (scenarios B01,
B02) before and after.

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
