# Long-term speed

Long-term speed is how much finished work reaches `main` per week, for the user's time and money
spent. Work is finished when it is on `main`, does what was asked and never has to be touched again
for that reason. The clock starts when the user asks for a change. It stops when the last rework
that change causes is done, so a change that shipped in an hour and was repaired three times is slow.

```text
time of a change = decide + build + check + wait + rework
```

Quality is part of the sum. Too little of it returns as rework: a bug hunt, a revert, a redesign,
lost data, a user who leaves. Too much of it shows up as check and wait. Lower the total. A cut in
one term that grows another by more is a loss.

## The test for a step

A step is a gate, test, review, document, rule, report or approval. It earns its place when:

```text
share of runs that stop a real defect × time that defect would cost later
    > time the step adds to every run + time it makes other work wait
```

Fill it in from this repository's history: what the step caught recently, and what it cost. A
defect that loses an acknowledged write, corrupts data or breaks an upgrade has no acceptable rate,
because it costs the users. For those, ask which step catches it earliest, and never drop the last one.

Two claims need evidence, or they lose:

- "This step protects us in the long term." Name the defect it stopped or would have stopped.
- "Cutting this step makes us faster." Name the time it costs and what else catches the same defect.

| Practice | Call | Reason |
| --- | --- | --- |
| Design review before a protocol change | Keep | A wrong design is cheapest to fix before any code exists |
| Independent review of engine code | Keep | Skipping it ships faster today and is repaid as bug hunts |
| Review and CI for a docs-only change | Drop | Words changed. Nothing can break at run time |
| Every CI job on every PR, with dozens of PRs a day | Run once on the merged `main` | The account runs 20 jobs at a time, so unneeded jobs hold back the needed ones |
| Integration, simulation and differential tests | Prefer | They exercise behavior that users depend on |
| A test that a constant set to 42 is still 42 | Delete | It fails only when someone changes the constant |
| More runner slots, a faster laptop, cloud test machines | Advise the user | They cost money and remove a real limit |

## Decide fast

Ask whether a choice can be undone. A choice that can be undone in a day is tried today and judged
on the result, because waiting a week to decide costs more than being wrong for a day. Time spent on
a choice belongs to the few that cannot be undone: on-disk and wire formats, public interfaces, data
deletion, releases. Plans, models, memos and status reports are not the work. Keep the ones
somebody used to make a decision.

## Ten drivers

Each is something speed forces when it is the only goal. Use them as the checklist for finding what
slows us down.

| # | Driver | In this codebase | Signs that it is slow | Typical changes |
| --- | --- | --- | --- | --- |
| 1 | One rule | One question settles every trade-off: does this make us faster in the long term? | Two skills give different answers. Agents reopen settled decisions. A rule whose reason nobody can state | Merge or delete rules. Write the decision and its reason once |
| 2 | Capability | The right model, effort and tools for each task | Work redone by a second agent. A strong model doing lookups. Sessions stopped by a filter or a limit. Steps that exist only because a weaker agent needed them | Change the default model per role. Give the agent a tool. Delete steps the current models do not need |
| 3 | Truth | Feedback is honest and arrives early | Flaky checks. A green job that executed nothing. A loosened assertion. A test that passes on a mutant. The first failing signal comes hours after the mistake | Fix or quarantine the flake. Make the cheap check run first. Report `not-run` as `not-run` |
| 4 | Throughput | Work moves at all hours on all the machines we have | Idle agents, runner slots or cores. Sequential tasks with no dependency between them. Work stopped overnight by one question | Split into independent tasks. Start long runs earlier. Queue ready work |
| 5 | Focus | Few things in flight, each finished before the next starts | Old open PRs and drafts. Branches that conflict in pairs. Reviews of work that was then dropped. Scope that grew beyond the request | Finish or close. Order by dependency. Smaller PRs. One owner per hot file |
| 6 | Ownership | Whoever does the work decides, within stated limits | Waits for the user on choices that can be undone. The same question asked twice. Layers of relayed reports. Re-checks of already attested results | State who decides what. Give a default. Bring the user only choices that cannot be undone |
| 7 | Learning | Nothing is paid for twice | The same failure in two threads. Environment setup rediscovered. A bug class seen again. The same command sequence typed by every agent | A script, a check or one line in a skill. A regression and the missing invariant for each bug |
| 8 | Quality | Right the first time, measured at the finished outcome | Fix, revert and follow-up PRs. Many commits after the PR opened. Bugs found late that an earlier stage could have caught | Move the check to the earliest stage that can catch the defect. Add the gate that is missing |
| 9 | Limits | The bottleneck is ours, and it has a name | Queue time on Actions. A full laptop. Rate limits. A single reviewer or a single lane | Use the limit better first. Then ask the user to raise it |
| 10 | Simplicity | Humans and agents can hold it in their heads | Long skills, plans and ledgers. An interface that needs a page of explanation. Many places to change for one idea. Slow builds | Delete. Merge. Shorten. Split crates and files that everyone edits |

## What speed must not cost

Four properties of the database make speed worth having. A faster process that lowers one of them
is slower in the long term.

- **Performance.** Latency and throughput within the CPU and memory limits.
- **Scalability.** Clusters, nodes and workers are easy to add, and adding them helps.
- **Reliability.** An acknowledged read or write is true, through crashes, retries and failover.
- **Ease of use.** Low cognitive load for the people and agents who call the SDK, CLI and API. A hard
  implementation is an acceptable price.

Pair every harness change with the property it could hurt, and say how we would notice. A change
with no plausible harm says `none`.

## Source

The method adapts Alex Hormozi's argument that speed, measured to the finished outcome, is the one
value the others derive from
([video](https://youtube.com/watch?v=rRI0iH-cpjg)). His drivers, in order, are a single decision
rule, talent, truth, work ethic, focus, ownership, innovation, quality and the removal of limits,
with fewer processes as a theme throughout. He warns that "long-term" becomes an excuse to
deliberate, which is why both claims above need evidence.
