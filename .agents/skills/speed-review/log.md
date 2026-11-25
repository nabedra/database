# Speed review log

One entry per run, newest first. [The skill](SKILL.md#log) gives the format.

## 2026-11-25
Head: 7ebfe814987afd21318d5fb919dbf96812515d67
Constraint: Simulator workloads are stacked on sweeps in which almost every seed is red. In the last 720-seed sweep (run 45657936802) 700 are red, and `ResourceBaseline` is the only reason named for 357 of them. The topology (0 of 48), membership (0 of 80) and queued (0 of 16) workloads have no clean seed, fault-free seeds included. On 2026-11-25 simulator checks used 139 of 167 runner hours, and 183 of 380 of those jobs failed.
Numbers: 16.0 landed per day (125 in 7.8 days). Opened to merged median 0.8 h, p90 10.5 h. Queue per job median 0.2 min, p90 15.8 min (sampled week), 13.5 min on 11-25, when 20 jobs was the most that ran at once. Repair commits 318 of 1,305 by subject. 131 agent logs: 244 of 467 wall-clock hours in tool calls over a minute, 163 of them in commands that look like waits (matched by text). Skill text 21,624 to 131,595 bytes over the range; this run adds no net skill text outside this log and the advice.
Not read: PR diffs; inline comments outside the slowest PRs; most of 173 wave reports (grepped); job data for successful dispatches before 11-25; peak build memory. Runner hours count completed dispatched runs only. Several causes of idle time are inferred.

| ID | Change | Driver | Predicts | Must not hurt | Verdict |
| --- | --- | --- | --- | --- | --- |
| S1 | Order for bringing up a simulation workload, in `simulation.md` | 3 Truth | Failed share of `oltp-sim*` jobs falls from 48% to under 25%, and each open workload has a clean fault-free seed, by the next run | Reliability: a real finding dismissed as a wrong rule. Notice: a rule fix without a cited text or with a planted bug no longer caught | open |
| S2 | A hunk that others wait for lands alone, waiting PRs are finished first, stacked PRs are retargeted before the branch goes (`subagent-driven-development.md`, `nabedra-change`) | 5 Focus | No PR closed by a deleted base (4 this time). p90 opened to merged falls from 10.5 h to under 6 h. No hunk waits over 4 h (20 h for the probe hook) | none | open |
| S3 | Checks no longer hard-code `cargo -j 2`; Actions builds with 4 jobs and local runs keep 2 (#155) | 9 Limits | Measured: the three builds timed take 25% less (one run per side). Median of simulator jobs over 15 min falls from 26.6 to 22 min | Reliability: an out-of-memory build read as a red check. Notice: lost runners or killed builds in `checks` jobs | open |

Seen and left for a later run, by rough cost: one shared build for all simulator checks of a dispatch (the workflow file is owned by a running wave); the catalog stayed stale for 23 h because regeneration waits for the lead; batches cut by two agents' `pkill -f` patterns; a quarantine row whose selector matches no case; about six review rounds on checker code (#124) that a generated corpus of legal and illegal inputs could shorten.
