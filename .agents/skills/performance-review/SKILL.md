---
name: performance-review
description: Benchmark Nabedra when asked to measure database latency, throughput, capacity, or performance regressions, or to compare an engine with competing systems.
---

# Performance review protocol

Use [design review](../design-review/SKILL.md#build-the-design-rubric) for
performance targets in the plan and [code review](../code-review/SKILL.md#verification-rules)
for correctness and revision checks. Use [harness review](../harness-review/SKILL.md)
for development and CI workflow speed.

## Build the benchmark rubric

Start with the requested engine, feature, or diff. Find the relevant scenarios
and guarantees in [the testing registers](../../../docs/testing/guarantees/).
For a competitor review, choose the closest reference system for each requested
workload using the [competitor matrix](references/competitors.md). Include a
second reference when it answers a different deployment or workload question.

Before measuring, write one row per comparison with the workload, dataset and
seed, client path, required guarantees, resource budget, load points, primary
metric, and target. Use published targets from the plan or guarantee register
where applicable. Otherwise report the measured difference and uncertainty.
Use the Nabedra base and head SHAs for a regression review. Add a pinned
competitor build when the review makes a comparative claim.

## Match the work and the guarantees

- Use the same logical data, operations, key distribution, selectivity, result
  size, client concurrency, offered schedule, and deadlines. Check results
  and final state with the same oracle. Use each engine's documented client
  and idiomatic schema or indexes for the requested operation. Measure SDK,
  wire, and in-process paths in distinct workloads.
- Record what a successful operation guarantees. Match durability at ACK,
  isolation, visibility, replication, retry and delivery semantics, TLS,
  batching, and compression where relevant. Give materially different
  semantics or deployment models their own labeled results. Check pinned
  upstream documentation for any setting that affects equivalence.
- Match machine architecture, CPU and memory limits, disk, network placement,
  and aggregate resources across the compared topologies. Account for client,
  coordinator, and worker resources. Use dedicated benchmark infrastructure
  for competitor claims and absolute targets. Follow [Nabedra builds](../nabedra-builds/SKILL.md)
  for builds and environment boundaries. If the required environment is
  unavailable, report the comparison as not run and state the blocker.

## Run and measure

- Start with [the paired performance specs](../../../docs/testing/perf/README.md)
  for Nabedra base/head. Use `scripts/testing/ab.py` with the matching spec
  and preserve its configured regression threshold. For cross-engine work,
  reuse the runners linked from the [competitor matrix](references/competitors.md)
  where they exercise the same workload. Verify the commands with a smoke run.
- Run release builds, warm them consistently, reset equivalent data and cache
  state between trials, and alternate or randomize engine order within matched
  pairs. Label cold-start and warm steady-state runs separately. Run long
  enough to include background work that affects the result, such as
  compaction, checkpointing, eviction, or garbage collection.
- For request workloads, sweep common offered loads from low traffic through
  saturation. Use scheduled arrivals for tail-latency and capacity claims.
  Include queueing from scheduled arrival in latency. Retain timeouts,
  rejections, retries, incomplete operations, and errors. Check whether the
  load generator or network saturated first. For batch workloads, record
  wall time through last-result materialization and first-result time when
  the operation streams.
- Record p50, p95, and p99 with sample counts, correct completions per second
  (goodput), failure rates, and peak CPU, memory, disk, or network use where
  they explain the result. For capacity, report the highest measured load that
  meets the declared latency and error budgets and bracket saturation.
  Use multiple independent repetitions and report spread or a confidence
  interval over trials or pairs. Treat noisy, under-sampled, and unbracketed
  results as inconclusive for the affected claim.

## Record the review

Use a `Performance review` section in the requested report or PR. Give each
workload its own row. Report observed values with units and make the ratio
direction explicit, for example Nabedra p99 divided by competitor p99.

| Part | Content |
| --- | --- |
| Scope | Workload, scenario IDs, relevant guarantees, target, and timing boundary |
| Reproduction | Nabedra SHAs, competitor versions or image digests, build flags, client versions, commands, configs, dataset hash or seed, topology, and hardware |
| Results | Offered load or input size, per-engine latency and goodput, errors, sample and repeat counts, paired delta or ratio, uncertainty, and whether the target held |
| Evidence | Raw samples, resource counters, aggregate results, and artifact or run links. Include a latency-versus-load or runtime-versus-size chart for a sweep |
| Limits | Unmatched semantics, inconclusive findings, and workloads skipped or blocked with reasons |

Tie every conclusion to the measured workload and environment. Use
[code review evidence rules](../code-review/references/evidence.md) when a
benchmark also serves as a registered performance check.