# Searching 10,000 seeds in shards (design, not implemented)

Nothing here is in `sim_bugs.py`. Until it is, a search runs in one job from seed 1, and one that does not finish is
`incomplete`. The merge step is counting logic, so it gets its own change and its own review.

## Cost

Figures of the second review of #113. The model is arithmetic, not a run: a patched seed runs once, a base seed once (twice
if red), and each hit that is judged four times on one thread. The inputs were measured on Actions: 3.17 CPU s per `direct` run,
2.94 per `cold`, 3.69 per `move`, at about 2.6 effective cores with 3 jobs, and a peak RSS of 2.5 to 3.3 GB for 30 seeds.

| Per target, 10,000 seeds | Core-hours | Wall on one runner |
| --- | --- | --- |
| 0.5 to 1 CPU s per seed, quiet main | 2.8 to 5.6 | 1.1 to 2.1 h |
| Measured cost, quiet main | 16 to 20 | 6.3 to 7.9 h |
| Measured cost, red rates of 2026-11-24 | 28 (`cold`), 29 (`direct`), 62 (`move`) | 12.7, 13.3 and 43 h |
| The same, judging only seeds that the base run has green | 24 to 30 | 9.4 to 11.7 h |

All five targets cost about 90 core-hours on a quiet main at the measured cost, and 177 at those red rates. One check of 18,000 s holds
about 13. The red rates come from unpatched seeds at `712c6459`: 27 of 30 `direct`, 29 of 30 `move` and 30 of 30 `cold` are
red, `ResourceBaseline` in 82 of the 86. Until that is explained, about one `direct` seed in ten can count.

acc3-cold-fence is not shown and is not searched. That leaves four targets: about 72 core-hours, scaled from the 90 for five.

## Checks

| Check | Runs |
| --- | --- |
| `oltp-sim-bugs-<id>-1` to `-4` | One shard each: seeds 1 to 2,500, 2,501 to 5,000, 5,001 to 7,500, 7,501 to 10,000 of one target. 16 checks for four targets. |
| `oltp-sim-bugs-<id>` | The counting run of one target, on main. |
| `oltp-sim-bugs-direct` | All five targets' results, as the acceptance record. It cannot pass while ACC3 is not shown. |

16 shard jobs are one pass. The reviewer costed two waves of eight jobs under the nightly's cap of 8. The lead allows two waves
of 16, and the account runs 20 jobs at once. Either way the first range of every target starts first, because a hit there makes the
other ranges unnecessary. On a quiet main a shard takes 1.6 to 2 h plus its builds, and 2.4 to 2.9 h at the red rates above. The limit is 18,000 s.

## A shard

1. `check`, then build the unpatched sweep.
2. The **base run first**: the unpatched build on every seed of the range, in chunks, with kinds, trace hash and
   determinism per seed.
3. Build the patched sweep and reset the tree.
4. The patched build on every seed of the range, in chunks.
5. A seed that reports an expected kind and is red in the base run is masked as `base-red`, with no replay. The base
   run is the twin's own run, so this is decision 8's red twin at the cost of nothing. A base seed that is not deterministic is red.
6. Any other seed that reports an expected kind is judged as today: patched replay, clean twin, and the same masks. The
   twin has to have the base run's trace hash, or the shard is an `error`.
7. The shard goes on after a hit. It never decides whether a hit counts.
8. Only at the end it prints its rows and then one line `sim-bugs-shard: complete`, with the range, the number of rows and the
   SHA-256 over them. The log is attested, so it covers them.

**A run that dies is an `error`, not clean seeds.** A shard without its `complete` line (a signal, out of memory, a lost
runner, a timeout, a missing or unreadable seed line) contributes nothing, not even the chunks it finished. Its range has
to run again. A seed on which the harness panicked is a row with the kind `harness`: tried, not clean, listed in the result. The
harness prints that row only with `sweep.rs` changed (a patch for P10-WORKLOADS). Without it, such a seed makes its
shard an `error` on every run.

## What a row is for

Each row carries its **key**: the target, the plan, the review digest of the patch, the range, and the build. The build is the Git tree
of HEAD without the targets' own files, so it is the same on two commits that differ only in manifests. Rows of two keys never
mix. The key does not name a machine, and recorded hashes agreed between machines in the 90 seeds measured so far.

**Resume** is by whole shard. A shard that is complete for a key does not run again when `[[masked]]` or `[detection]` are
committed, because the key does not change. If it does run again, both sets of rows have to be equal, seed by seed, in
kinds, trace hash and masks. A difference makes the target an `error`: it is nondeterminism, and neither run wins. The plan,
the patch or anything else in the build makes another key, and the earlier rows stay in `searches.jsonl`.

## The merge step

`sim_bugs.py merge <id> --records <dir>` reads downloaded evidence artifacts. A reviewer who wrote neither the patch nor the
workload runs it, after `gh attestation verify` on every record. It refuses unless

- every shard's log has the SHA-256 that its record names, the record is of this repository's `checks` job, the tree was clean and the attempt is
  named;
- every shard has the same key and its `complete` line fits its rows;
- the ranges up to the answer have no gap and no overlap, and every seed has one row.

Its answer is the lowest seed that nothing masks, with `[[masked]]` of the manifest applied. **A hit in a higher range counts only when
every lower range is complete.** `not-detected` needs all four ranges and the base rate over all 10,000 seeds. A range that
is missing, died or disagrees makes the answer `error` or `incomplete`, never `not-detected`.

It appends one entry of kind `merge` to `searches.jsonl`: the key, the answer, masks, base and patched rates, and per range the
run URL, attempt, record and row digests. That entry goes through review like `[detection]`. `evidence` compares it with the logs.

## The counting run

On main, for one target. It takes the hit from the committed merge entry, and derives again everything that says the hit is real: both
builds, the patched replay, the clean twin, the base run of the hit and its neighbours up to the chunk size, the
confirmation, and the regression without and with the patch. It does not search seeds 1 to the hit again.

So the counting run trusts the merge entry for two things it does not derive: that no lower seed was an unmasked hit, and
the base rate. A forged entry could hide an earlier hit or a base rate. It could not create a hit. The reviewed merge step and
`evidence` are what stands behind those two, and that is a weaker guarantee than the one-job search gives today.

## Tests the change needs

Each first as a failing test, with the fake harness: a shard without its line, with a line for other rows, with a missing,
doubled or foreign seed; two shards of different keys, SHAs of different builds, overlapping ranges, a gap below the hit;
a higher hit with a lower range missing, dead or disagreeing; rerun rows that differ; a base-red seed counted as a hit; a twin with
another trace hash than the base run; an edited merge entry; a record whose log digest is another's; a dead shard
counted as clean seeds; and `not-detected` from three ranges.
