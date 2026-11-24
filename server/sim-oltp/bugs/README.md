# Reverted fixes

Each directory holds one reverted fix: a patch that brings a known bug back, and a manifest. Phase 10's targets are
here, later phases in `phase<N>/`. `scripts/testing/sim_bugs.py` applies a patch only in a scratch worktree. A target
is found when the simulator detects the reverted fix by the rule below. Results are fault-model evidence.

## Files

| File | Content |
| --- | --- |
| `bug.toml` | The manifest. |
| `revert.patch` | The patch. It changes product sources only, in the form `git diff` writes. Nothing before, between or after its `diff --git` sections. |
| `searches.jsonl` | Every search of the target, also those that a later one replaced. Append-only. Written by `sim_bugs.py record`. |

A committed target or record cannot leave the tree, and a target keeps its bug record, phase and set. A target that a
spare replaces stays listed as not detected.

## The search rule

1. The seeds are tried in order from 1 to 10,000. `workload`, `profile`, `expect` and `seeds` are the plan, written in
   the manifest before the first result. No search resumes another: each run starts at seed 1.
2. A seed that reports an expected kind is replayed alone, twice, on the patched build and on the unpatched build.
   It is masked, recorded, counted towards the 10,000, and the search goes on, when
   - the unpatched twin reports anything (`twin-red`);
   - the patched replay is not deterministic (`nondeterministic`) or reports other kinds than the search
     (`search-replay-disagree`);
   - a kind is a fault of the harness (`harness-fault`);
   - both builds have the same trace (`same-trace`);
   - the manifest says that this exact hit shows a known finding (`known-finding`).
3. The first seed that nothing masks is the hit. The search stops there. An earlier hit has to be settled before a
   later one can count.
4. The unpatched build runs every seed tried. The result gives its base rate of the expected kinds next to the patched
   rate. A base rate does not veto a hit.
5. The hit counts as found only if nothing under "What has to be there" is missing. Otherwise it is a candidate.

| Result | Meaning |
| --- | --- |
| `found` | A confirmed hit with everything below. |
| `candidate` | A hit that does not count yet. The output says why. |
| `not-detected` | 10,000 seeds tried, with the base rate, and no unmasked hit. |
| `incomplete` | The search stopped earlier (`--seeds`, a time limit). Says nothing. |
| `error` | A run left something unsaid: a missing or unreadable seed line, a report for another seed, workload or defect, an unexpected exit, no trace, no history, the same binary. Says nothing. |
| `no-detector` | `expect` is empty. Not detected. |
| `not-run` | The harness lacks the workload. |

## What has to be there for a hit to count

- The regression evidence is for this patch: its changed lines, its context, and a commit that holds it. The regression
  also passes without the patch and fails with it in the run that counts.
- `[review]` says `sensitive`. There is no `hold`.
- `[detection]` confirms the mechanism for that exact seed, trace hash and history SHA-256. Its `search` is the hash of
  the recorded search that produced the hit, with the same plan and patch.
- Every plan that a committed manifest ever had is in `searches.jsonl`, as a search or as `never_searched` with a
  reason.
- The base run covers every seed tried.
- Nothing is in the working tree that is not in HEAD, and the build is main's: HEAD may differ from its merge base with
  `origin/main` only outside `server/` and in this directory. `sim_bugs.py` itself is main's.
- From phase 11, the detector has the text of its rule in `[[rules]]`.
- No other target shares the bug record, the patch, the hit or the prose.

Anything missing, inconsistent or unreadable is not a detection.

## Manifest fields beyond the obvious

```toml
reverts = "<sha>"                     # or a list, for a hand port. A clean port reverts one commit.
expect = []                           # phase 11 or later only, and then with
expect_reason = "<why no rule may detect it>"
hold = "<why this target may not count yet>"

[regression]
assertion = "state"                   # or counter, timeout: what the in-tree regression asserts. Optional.

[regression.evidence]                 # of a `verify` run, or of two commits with and without the patch
digest = "<sim_bugs.py digest>"
clean = { run = "<Actions run URL>", sha = "<commit>", result = "passed" }
patched = { run = "<Actions run URL>", sha = "<commit>", result = "failed", output = """<lines the run printed>""" }

[review]                              # the independent review of the patch
verdict = "sensitive"                 # or partial, not-sensitive. Those never count.
assertion = "state"                   # what the verdict rests on. A counter or a timeout alone is not sensitive.
reviewer = "<who>"
source = "<where the review is>"

[[rules]]                             # the text an expected kind enforces (lead decision 9)
kind = "Liveness"
source = "docs/testing/oltp-contract.md"
text = "<literal quote>"

[[masked]]                            # a hit that shows a known finding instead
seed = 0
trace_hash = "<16 hex digits>"
history_sha256 = "<64 hex digits>"
finding = "DR-1"
reason = "<what the history shows>"
confirmed_by = "<who>"
confirmation = "<where they say so>"

[detection]
seed = 0
trace_hash = "<16 hex digits>"
history_sha256 = "<64 hex digits>"
detector = "<one of expect, reported by the hit>"
search = "<hash printed by record>"
lines = ["<lines of this hit's history or trace that show the reverted path ran>"]
mechanism = "<names the seed, the trace hash and a file the patch changes, and no other hit's>"
confirmed_by = "<who wrote neither the patch nor the workload>"
confirmation = "<where they say so>"
patch_author = "<who>"
workload_author = "<who>"
```

`lines` must be exact lines of the hit's `history.jsonl`, `trace.txt` or `trace-tail.txt`, and the twin must lack at
least one.

## From a hit to a detection

1. Dispatch the search. A new hit is a candidate, and the job fails.
2. Download the artifact and `sim_bugs.py record <id> --from sim-bugs-run.json`. Commit `searches.jsonl`.
3. Someone who wrote neither the patch nor the workload reads the history and the trace against the known findings, and
   writes `[detection]` or `[[masked]]`.
4. Merge, and dispatch the search again on main.
5. Compare the records with the attested logs. Download the `evidence-*` artifacts of every cited run into one
   directory, verify each record with `gh attestation verify`, and run
   `sim_bugs.py evidence --records <dir>`.

A manifest whose plan changed and was never run is accounted for with
`sim_bugs.py record <id> --never-searched <commit> --reason "<text>"`.

## What the tool cannot prove

- **Independence.** It records who confirmed a hit and who wrote the patch and the workload, and refuses equal names.
  The names are free text. It cannot show that two names are two people, or that the confirmer read anything.
- **Prose.** It refuses prose that does not name this hit, or that names another. It cannot tell whether the mechanism
  is right, or whether the quoted lines show the reverted path.
- **Recorded text.** `check` ties the evidence to a commit that holds the patch. Only step 5 shows that the named run
  printed those lines. The same holds for `searches.jsonl`.
- **Unrecorded searches.** It sees a plan that was committed. It does not see a search whose result nobody recorded, or
  history that was rewritten before it reached main.
- **Intent in a hand port.** The path rules, the cfg rules and the text rules refuse the known ways to act only in the
  simulator. A reviewer still has to read every hunk.
- **The harness and the machine.** It believes what main's harness reports, and the compiler, Git and the Actions runner
  that ran it.
