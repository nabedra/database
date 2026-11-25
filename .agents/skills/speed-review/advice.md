# Speed advice

What only the user can do. [The skill](SKILL.md#advice) gives the format. This file is public.

# Open

## A1. Decide what "finished" means for the simulator workloads
Status: open
Opened: 2026-11-25
Kind: decision
Drag: In the last sweep of the main workload 20 of 720 seeds are clean. The topology (0 of 48), membership (0 of 80) and queued (0 of 16) workloads have none. Product findings are recorded and not fixed in this wave, so some reasons stay red on every head and hide the rest. Four PRs (#132, #136, #138, #143) have been open behind them for 7 to 24 hours. On 2026-11-25 simulator checks used 139 of 167 runner hours, and 183 of 380 of those jobs failed. Estimate: 20 or more agent hours per week.
Cost: One answer. Fixes to engine code still need the design review gate.
Gives: A wave that can end. Recommended: a workload is finished when its fault-free seeds are clean or red only for open records, and every record that leaves a workload without one clean seed is fixed next, through the gate. Meanwhile agents settle the harness and rule reasons first (S1).

## A2. Check which GitHub plan covers the account that owns this repository
Status: open
Opened: 2026-11-25
Kind: purchase
Drag: The repository never ran more than 20 jobs at once on 2026-11-25 and sat at exactly 20 for 114 minutes. Jobs queued 67 hours in total that day (p90 13.5 minutes). 20 is the limit of the Free plan. A personal account owns the repository, so a plan held by an organization may not reach it (inferred).
Cost: Pro gives 40 jobs, Team 60 and Enterprise 500 (docs.github.com/en/actions/reference/limits). Team is $4 per user per month (github.com/pricing); the price of Pro was not confirmed. All checked 2026-11-25.
Gives: Two to three times the jobs. S3 (#155) is the free part. Buy only if the p90 queue stays above 5 minutes with it.

# Closed

None yet.
