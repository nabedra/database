# Reverted fixes

Each directory holds one reverted fix: a patch that brings a known bug back, and a manifest. Phase 10's targets are
here, later phases in `phase<N>/`. `scripts/testing/sim_bugs.py` applies a patch only in a scratch worktree. A target
is found when the simulator detects the reverted fix by the rule below. Results are fault-model evidence.

[SHARDS.md](SHARDS.md) is the design for searching 10,000 seeds in shards. It is not implemented.

## Files

| File | Content |
| --- | --- |
| `bug.toml` | The manifest. |
| `revert.patch` | The patch. It changes product sources only, in the form `git diff` writes. Nothing before, between or after its `diff --git` sections. Plain ASCII, in lines of at most 200 characters. |
| `probe.patch` | Only with `regression.probe = true`: the test that the regression runs, for a fix whose own test is not a steady proof. Same form, only added lines. See "What a probe may add". `verify` applies it to both builds. No sweep ever contains it. |
| `searches.jsonl` | Every search of the target, also those that a later one replaced. Append-only. Written by `sim_bugs.py record`. |

No other file may be in a target's directory. A committed target or record cannot leave the tree, and a target keeps its bug record, phase and set. A target that a
spare replaces stays listed as not detected.

## What a probe may add

A probe may add anything to test files, except `include`, `include_str`, `include_bytes` and block comments. What it adds
to a product source has to be one block at a time, in whole lines:

- the first line is exactly `#[cfg(test)]` or `#[cfg(any(test, feature = "test-hooks"))]`, and `#[doc(hidden)]` may follow;
- then one `let` on one line, or a plain `fn` or `if` head that opens the block, lines that are indented further and hold no
  brace, comment, escape, character, attribute, byte string or raw string, and the line that closes it;
- no line before it in the file holds a raw string or a block comment, and each closes the quotes it opens;
- the line before it ends in `{`, `}` or `;`, and the line after it starts with a word other than `else`, with `#` or with `}`.

The tool does not read Rust for this. Whatever it cannot tell, it refuses. A macro that drops the attributes of its input
would still compile such a block, so a reviewer looks at where the block stands.

## The search rule

1. The seeds are tried in order from 1 to 10,000. `workload`, `profile`, `expect` and `seeds` are the plan, written in
   the manifest before the first result. No search resumes another: each run starts at seed 1.
2. A seed that reports an expected kind is replayed alone, twice, on the patched build and on the unpatched build.
   It is masked, recorded, counted towards the 10,000, and the search goes on, when
   - the unpatched twin reports anything (`twin-red`);
   - the patched replay is not deterministic (`nondeterministic`) or reports other kinds than the search
     (`search-replay-disagree`);
   - a kind is a fault of the harness (`harness-fault`);
   - both builds have the same trace (`same-trace`). Every history event and every violation is a trace line, so equal
     traces have equal verdicts;
   - the manifest says that this exact hit shows a known finding (`known-finding`).
3. The first seed that nothing masks is the hit. The search stops there. An earlier hit has to be settled before a
   later one can count.
4. The unpatched build runs every seed tried. The result gives its base rate of the expected kinds next to the patched
   rate, and lists the seeds on which the harness itself panicked. A base rate does not veto a hit.
5. The hit counts as found only if nothing under "What has to be there" is missing. Otherwise it is a candidate.

| Result | Meaning |
| --- | --- |
| `found` | A confirmed hit with everything below. |
| `candidate` | A hit that does not count yet. The output says why. |
| `not-detected` | 10,000 seeds tried, with the base rate, and no unmasked hit. |
| `not-shown` | No hit could count: no test fails because of the patch, the review does not call it sensitive, or it is held. It is not searched, so it takes no time from the others. Not detected. |
| `no-detector` | `expect` is empty. Not searched, not detected. |
| `incomplete` | The search stopped earlier (`--seeds`, a time limit). Says nothing. |
| `error` | A run died or left something unsaid: a signal, a missing or unreadable seed line, a report for another seed, workload or defect, an unexpected exit, no trace, no history, the same binary. Says nothing, and none of its seeds are clean. |
| `not-run` | The harness lacks the workload. |

A check passes only if every target of its set is found. While a target is `not-run`, the record is not-run.

## What has to be there for a hit to count

- The regression evidence is for this patch: its changed lines, its context, and a commit that holds it. The regression
  also passes without the patch and fails with it in the run that counts.
- Evidence of a `verify` run is of a commit that holds the same patch, the same probe and the same regression. Evidence of
  scratch commits: the commit with the patch is, in the files of the probe and the patch, its parent plus those two, byte for
  byte, and the parent is in this history. The clean commit holds the probe's lines. It may hold other probes as well, so a
  reviewer compares it.
- `[review]` says `sensitive`, names the digest of the whole patch as it is now, and does not contradict
  `regression.assertion`. There is no `hold`. A patch that is refreshed, even in its context, needs another review. So
  does another probe, and any change of `[regression]` but its evidence and `timeout_s`: the same `[review]` (its
  `notes` aside) may not stand beside two regressions in the history.
- `[detection]` confirms the mechanism for that exact seed, trace hash and history SHA-256. Its `search` is the hash of
  the recorded search that produced the hit, with the same plan and patch.
- Every plan that a committed manifest ever had is in `searches.jsonl`, as a search or as `never_searched` with a
  reason.
- The base run covers every seed tried.
- Nothing is in the working tree that is not in HEAD, and the build is main's: HEAD may differ from its merge base with
  `origin/main` only in the targets' own files here. A toolchain file, a Cargo configuration, a rule's source or
  `sim_bugs.py` is part of the build. The result says how far that merge base is behind `origin/main`.
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
kind = "lib-test"                     # with test, and optionally probe or needs_server
                                      # or "acceptance" with scenario, or "recovery-suite" with suite
test = "<full library test path>"     # in the tree, or added by probe.patch. Not both.
probe = true                          # or absent. Required exactly when there is a probe.patch.
needs_server = true                   # or absent: the test starts server processes. Only these tests get one.
suite = "replacement-wipe"            # of server/tests/oltp_recovery_operations.py, for a recovery-suite
failure_contains = ["<text>"]         # what the failure says. Required for the two process kinds.
assertion = "state"                   # or counter, timeout: what the regression asserts. Required to count.
                                      # A field of another kind, or one that is off, is refused.

[regression.evidence]                 # of a `verify` run, or of two commits with and without the patch
digest = "<sim_bugs.py digest>"
clean = { run = "<Actions run URL>", sha = "<commit>", result = "passed" }
patched = { run = "<Actions run URL>", sha = "<commit>", result = "failed", output = """<lines the run printed>""" }

[review]                              # the independent review of the patch
digest = "<the review digest that sim_bugs.py digest prints: SHA-256 of the whole patch>"
verdict = "sensitive"                 # or partial, not-sensitive. Those never count.
assertion = "state"                   # what the verdict rests on. A counter or a timeout alone is not sensitive.
reviewer = "<who>"
source = "<where the review is>"

[[rules]]                             # the text an expected kind enforces (lead decision 9)
kind = "Liveness"
source = "docs/testing/oltp-contract.md"   # a Markdown file in docs/ or plans/ of HEAD, nothing else
text = "<literal quote of at least five words>"

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

`lines` must be exact lines of the hit's `history.jsonl`, `trace.txt` or `trace-tail.txt`. At least one must be an event
of the trace that is neither the violation nor the client history, and that the twin does not have at any time.

## From a hit to a detection

1. Dispatch the search. A new hit is a candidate, and the job fails.
2. Download the artifact and `sim_bugs.py record <id> --from sim-bugs-run.json`. Commit `searches.jsonl`.
3. Someone who wrote neither the patch nor the workload reads the history and the trace against the known findings, and
   writes `[detection]` or `[[masked]]`.
4. A reviewer of that change does the "reread" column below and runs `evidence` (step 6). Merge with a merge commit: the
   evidence commits have to stay ancestors.
5. Dispatch the search again on main.
6. Compare the records with the attested logs. Download the `evidence-*` artifacts of every cited run into one
   directory, verify each record with `gh attestation verify`, and run `sim_bugs.py evidence --records <dir>`. This is a
   required step of the merge, and no tool enforces it.

A manifest whose plan changed and was never run is accounted for with
`sim_bugs.py record <id> --never-searched <commit> --reason "<text>"`.

## What the tool cannot prove

Four gaps stay open however the manifest looks. Each needs someone to reread something.

| Gap | What can happen | What to reread |
| --- | --- | --- |
| **An invented confirmer, or wrong prose** | A false detection counts. The names are free text: one model under two labels, or a name nobody has, passes. A hit whose schedule exposed DR-1 or DR-2 can carry prose about the reverted fix. The quoted lines have to exist and have to be an event the twin lacks. They need not show the reverted path. | The hit's trace and history, from the artifact of the attested run, against the patch and the known findings. Nothing in the manifest replaces it. |
| **A hand-written search record** | No false hit, because the counting run derives the seed, trace, history, twin, base rate and regression again. It hides an earlier search. | `sim_bugs.py evidence` against the attested logs, and the traces of the searches it lists. |
| **An unrecorded search under the same plan** | No false hit. It hides extra attempts: another main SHA is another 10,000 schedules. Actions runs are visible, local ones are not. | The Actions runs of the check, and what each one's trace hashes were. |
| **A hunk that acts only in the simulator** | A false detection counts if the reviewer misses it. The path, text and cfg rules refuse the known ways. A hand port can still ask for a node ID, a thread name, a time or anything else the rules do not know. | Every hunk of the patch, at the digest that `[review]` names, and in the trace whether the event comes from the reverted hunk. |

Also open:

- An invented `[[masked]]` removes an earlier hit, so that a later one counts. Reread the masked trace as well.
- A `[review]` written for a patch by nobody. The digest shows which patch it is about, not that anyone read it.
- `expect` chosen broadly before any result, or a rule whose text was added to the documents for the target.
- A quoted line that differs between the builds and shows nothing.
- A probe that its reviewer did not read. It may add anything to a test file, and the row rests on it.
- A `[review]` edited together with the regression it is about. It is then another review on paper.
- A quote from `plans/testing/`. `check` finds the text in the copy, and does not compare the copy with the design or know
  which rows lead decision 13.1 lets a design rule count for.
- Recorded regression output, as far as the named run is concerned. `evidence` compares it.
- Branch history that was rewritten before it reached main.
- The text of the recorded runs against the manifest beyond what is named above: the `mode` of scratch evidence, for one.
- A recovery suite's failure is any assertion or timeout it prints at exit 1 beside the expected text, also one of a process
  it started. A patch may hold code behind many tabs. A probe's test may read the source at run time.
- The harness of main, the compiler, Git, the Actions runner, and these of the machine: `PATH`, `HOME` with what Cargo and
  rustup keep there, the interpreter that runs this script, `--cargo` (the output names it), and Git attributes of the
  clone, whose filters the comparison of the scratch tree refuses.

Builds, tests and Git get a fixed environment. From the machine it has `PATH`, `HOME`, `USER`, `LOGNAME`, `LANG`, `LC_ALL`,
`LC_CTYPE` and `TZ`, and nothing else. Scripts of a process regression also get the interpreter's own library directory if
`LD_LIBRARY_PATH` named it. The scratch worktree and `TMPDIR` are in `/var/tmp`, and a Cargo configuration in any directory
above the worktree is refused. Git uses no global or system configuration, no hook, and this script's own settings for
applying patches. It still reads the rest of the clone's own configuration. After a patch is applied, the tree has to hold exactly the old lines, less the removed and plus the
added ones.
