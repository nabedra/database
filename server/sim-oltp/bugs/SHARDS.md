# Searching 10,000 seeds in shards

`sim_bugs.py shard`, `merge` and `count`. A target's seeds are split over attested jobs, and the counting run reads their
records. The rule for what counts is in [README.md](README.md) and does not change. Lead decision 13.2 governs the merge.

## Checks

| Check | Runs |
| --- | --- |
| `oltp-sim-bugs-<id>-<n>` | One shard each: 2,500 seeds of a `direct` target (4 shards), 1,250 of `az2` (8). 20 checks for the four phase 10 targets that can be found. |
| `oltp-sim-bugs-<id>` | The counting run of one target. |
| `oltp-sim-bugs-direct` | The counting run of all five targets: the acceptance record. It cannot pass while ACC3 is not shown. |

A pass of a shard says that its range ran to the end. It is no detection and claims no guarantee. Phase 11 targets get their shard checks with their
workloads.

**Dispatch a shard from the commit it tests**: `--ref` a branch at that commit, and `sha=` the same commit. The attestation names the commit of the
workflow that ran. The tool accepts only the tested commit there, so a branch with another workflow cannot write a record.

## A shard

`sim_bugs.py shard <id> --shard 2/4`:

1. `check`, then build the unpatched sweep.
2. The **base run first**: the unpatched build on every seed of the range, in chunks, with kinds, trace hash and determinism per seed. A
   passing seed runs once and a red one twice.
3. Build the patched sweep and reset the tree.
4. The patched build on every seed of the range, the same way. Every seed, also one whose base run is red: the patched rate is reported.
5. A seed that reports an expected kind and is red in the base run is masked as `base-red`, with no replay. The base run is the
   twin's own run. A base seed that is not deterministic is red.
6. Any other seed that reports an expected kind is replayed alone on the patched build, and then on the unpatched one unless the first
   replay masks it. The masks are the README's. The twin has to have the base run's kinds and trace hash, or the shard is an `error`.
7. The shard goes on after a hit. It never decides whether a hit counts. `[[masked]]` of the manifest is the merge's.
8. Only at the end it prints its rows and then one line `sim-bugs-shard: complete`, with the key below, the range, the number of
   rows and the SHA-256 over them. The log is attested, so it covers them.

**A run that dies is an `error`, not clean seeds.** A shard without its `complete` line (a signal, out of memory, a lost
runner, a time limit, a missing or unreadable seed line) contributes nothing, not even the chunks it finished. Its range has
to run again. A seed on which the harness panicked is a row with the kind `harness`: tried, not clean, listed in the result.

Steps 2 and 4 are ordinary sweeps. `NABEDRA_SIM_EXPECT` would stop at each hit and lose the rest of its parallel batch, and it
reports no trace hash. Both modes report the kinds of the same run of a seed.

## What a row is for

The `complete` line carries the **key** of the rows: the target, the plan, the review digest of the patch, the digest of its changed lines, the
harness defect (none) and the build. The build is the Git tree of the commit without the targets' own files, so it is the same on two
commits that differ only in manifests. Rows of two keys never mix. The plan, the patch or anything else in the build makes another key,
and earlier merges stay in `searches.jsonl`.

A shard that is complete for a key does not run again when `[[masked]]` or `[detection]` are committed. If it does run again, both
sets of rows have to be equal, seed by seed. A difference makes the target an `error`, wherever it is: it is
nondeterminism, and neither run wins.

## The merge

`sim_bugs.py merge <id> --records <dir>` reads downloaded evidence: for every shard the record, its log and its attestation as
`sha256-<digest of the record>.jsonl` (`gh attestation download` writes it). It verifies each signature itself. A record is a shard only if

- `gh attestation verify` accepts it for this repository, the build workflow, a GitHub-hosted runner and the record's own commit, and the
  certificate names a dispatch, and the run and the attempt that the record names;
- its result is `pass`, its tree was clean, and its check is the one of that shard;
- the one log it names is there, and names the same commit, run and attempt;
- the log is whole: one `complete` line, after it nothing, and before it exactly the rows it names, one per seed and in order;
- every row says by itself why its seed can or cannot count, and its replays fit;
- it has this checkout's key, and its commit is in the clone and has that build.

Everything else is no shard, and the merge prints why. The answer is the lowest seed that nothing masks, with `[[masked]]` of the manifest
applied. **A hit counts only when every lower range is complete.** `not-detected` needs all ranges. A range that is missing,
died, is unattested or is of another commit makes the answer `incomplete`. Ranges above the hit are not needed.

| Answer | Meaning |
| --- | --- |
| `candidate` | The lowest unmasked hit. Rates and masks are over seeds 1 to the hit. Unmasked seeds above it in its range are listed and nothing more. |
| `not-detected` | Every range is complete and every hit is masked. |
| `incomplete` | Says nothing. |
| `error` | Two runs of one range differ. Says nothing. |

`--record` appends an answer (`candidate` or `not-detected`) to `searches.jsonl` as an entry of kind `merge`. `[detection]` names its hash.

## The counting run

`sim_bugs.py count <id>`, at a commit that has main's build. It does not trust the merge entry. It

1. reads the records from `<id>/` of the branch `evidence/sim-bugs` (or `--records <dir>`), verifies and merges them again;
2. refuses a recorded merge that differs (`error`). Without one the hit is a candidate;
3. builds both sweeps, which have to be the shards' binaries;
4. replays the hit on both builds, which has to give its shard's row, and runs the base run of the hit and its neighbours up to the chunk
   size again, which has to give their rows (`error` otherwise);
5. applies everything of "What has to be there", and runs the regression without and with the patch.

It does not search seeds 1 to the hit again. A missing or unattested shard makes the target `incomplete`.

The job needs no token and no workflow change. The `checks` job fetches every branch, so the records, logs and attestations are in its clone,
and `gh attestation verify --bundle` needs neither login nor the API. Nothing on that branch is trusted: what is not signed is not there.

A sweep holds the path of the tree it was built in. So `shard` and `count` build in one path per target directory, and refuse when something is
there. One commit then gives one binary on every runner that was compared, and the shards of one merge and the counting run have to have the
same two. (With a path per process, two jobs of one commit had different ones. Their 300 rows were equal.)

## What this design had left open

Each took the refusing answer.

| Open | Taken |
| --- | --- |
| How the counting run gets and verifies the records | The branch and the offline verification above. |
| Shards of two commits with one key | `incomplete`. The shards of one merge are of one commit. The counting run may be of a later one with the same build. |
| The commit of the workflow | It has to be the tested commit. |
| The same seeds in two splits, or two binaries | `incomplete`. |
| A patched replay with the sweep's kinds and another trace hash | Masked as `search-replay-disagree`. |
| A disagreement in a range above the hit | `error` for the target. |
| A merge entry with no answer | Not recorded. The attested logs keep `incomplete` runs. |
| A target that cannot count | It has no shard. `shard` and `count` say `not-shown` or `no-detector`. |

**Shared base runs are not implemented.** The unpatched results of two targets with the same build, workload and profile are equal. No two
targets that can be searched have the same ones: the four of phase 10 differ in profile, and in phase 11 only `acc2` and `topo1-child-schema`
share theirs, and the second is held.

## Cost

From the attested sweeps of run 45668293011 at `137c2a2e` (240 seeds per shard of `oltp-sim`, every seed twice, 4 jobs): 3.9 CPU s per run of `direct`
and 5.6 of `move`, 3.75 effective cores, 5.1 GB peak RSS. Nearly every base seed is red (220 of 240 `direct`, 240 of 240 `move`), and a red
seed runs twice on both builds, so a seed is at most four runs. A rehearsal job (150 seeds, `direct`, profile `network`) had 2.4 CPU s per run,
3.55 cores, 4.9 GB and 18 minutes of builds.

| Per shard | Runs | CPU | Wall with builds |
| --- | --- | --- | --- |
| `direct`, 2,500 seeds | at most 10,000 | 6.7 to 10.9 h | 2.2 to 3.3 h |
| `move`, 1,250 seeds | at most 5,000 | 7.7 h | 2.4 h |

Three `direct` targets and one `move` target: 141 to 193 core-hours, 45 to 59 runner-hours. A shard's limit is 17,400 s. A hit that is judged
adds four runs on one thread.

## What it cannot prove

- That nobody left out a run. A shard that ran twice with other rows is an `error` only if both records are on the branch. The Actions runs of the
  checks are visible, and a reviewer lists them.
- That a row is what the harness said. The attested job's own tool wrote it, from the commit the key names.
- What the README lists for every search: an invented confirmer, a mask for a finding that is not there, a hunk that acts only in the simulator.
