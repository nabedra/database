# Paired performance checks

Build both revisions for the same target and run them on a single benchmark
runner. Pass their binary paths to the A/B tool. The Events spec takes two local
Docker image references instead.

```sh
python3.11 scripts/testing/ab.py docs/testing/perf/oltp/point-update.toml \
  /path/to/base/nabedra-db /path/to/head/nabedra-db --output /path/to/evidence
```

| Spec | Input and runner setup | Metric |
| --- | --- | --- |
| Cache GET hit | Cache binaries on Linux. Set `NABEDRA_PERF_VALKEY` and `NABEDRA_PERF_VALKEY_CLI` to the reference tool paths and `NABEDRA_PERF_CLIENT_CPU` to a client CPU ID. Follow the prerequisites in [the Cache benchmark](../cache-benchmark.md). | GET-hit p99 at 24 offered requests/s with the stock client. |
| Events publish | Two locally built Events images and Docker. | Median of the publish p99 values from the measured load stages. |
| OLAP streaming | Matching OLAP binaries and the Docker streaming fixture. Set `NABEDRA_PERF_BASE_SHA`, `NABEDRA_PERF_HEAD_SHA` and `NABEDRA_PERF_FIXTURE_SHA` to the source revisions. | Median steady-state next-batch p99 across prefetch depths and worker configurations. |
| OLTP point update | Linux OLTP binaries and Docker. Set `NABEDRA_PERF_OLTP_IMAGE` to a locally available image with the benchmark's runtime dependencies. | Median of phase p99 latencies for direct updates at 20 offered requests/s. |

A spec provides `name`, `module`, `command`, `warmup` pairs, `rounds` of
measured pairs, and the `metric`. A command can be an argument array or a
shell-quoted string. The runner substitutes the binary path for `{bin}`.
With `--commands`, each side is parsed into an argument list and `{bin}` must
be a separate argument. Commands run from the repository root.

Each invocation can refer to its empty scratch directory as `{work}` and its
side as `{side}`. The runner removes scratch directories after measurement.
Use `metric = "wall"` for command elapsed time in seconds. A regex metric
must have one capture group for a positive number. It reads combined command
output by default, or the scratch-relative `metric_file` when specified. The
runner takes the median if a command reports several matching measurements.
Use costs where higher values are worse, such as latency, allocations or RPCs
per operation.

The runner alternates who goes first, including during warmup. It computes the
median head/base ratio across measured pairs and resamples those pairs with a
fixed seed to estimate a 95% confidence interval. A regression requires a ratio
at or above the configured threshold and an interval with a lower bound above
1. The defaults are `threshold_pct = 5` and `bootstrap_samples = 2000`.

`results.json` includes the CPU model, run order and paired measurements.
`summary.md` contains the decision. The runner exits 0 for a pass, 1 for a
regression and 2 for a command or input error. Use a distinct output directory
for each comparison.

## Phase 7 instruction counts

Use Callgrind on Linux or WSL to compare the phase 7 runtime wrappers in a
release build. Run from a checkout containing the harness and pass two refs
available in that clone. The registered `rt-zero-cost` check compares the
tested revision with the phase 7 base at `e3d51a6`. Install Valgrind on the
runner before dispatching the check.

```sh
python3 scripts/testing/insn_count.py --base e3d51a6 --head HEAD --output counts.json
```

Pass `--workload cache-set` to focus on a single workload during diagnosis.

The runner builds both commits in detached worktrees with private target
directories. It uses `cargo-slot` when available, or Cargo, with six build
jobs. It inserts the same driver into both checkouts, including visibility
hooks to the standalone Cache command handler and OLAP queue clock. Builds
use one engine feature at a time and the normal release profile.

| Workload | Operation count | Product path |
| --- | ---: | --- |
| OLTP storage | 512 | Apply a batch with one row overwrite in the Data group store and verify the reply and final row. |
| KV write and flush | 1,024 | Submit and await synced point writes on the native journal, flush to a run, and read back the last key. The flush cost is amortized over the writes. |
| Cache SET | 8,192 | Parse and execute one standalone SET PX command per operation, including the wall-clock expiry validation. |
| OLAP queue | 512 | Propose, acknowledge, start, finish and look up a queued request in process, using the production wall-clock helper. |

Each workload runs three times per binary in alternating order. The result
uses the median process-wide instruction count divided by the completed
operation count. The counter includes setup, teardown and background threads.
The noise floor is the full spread of the three base counts divided by their
median. The threshold is the larger of 0.5% and three times that spread.
The fixed allowance leaves room for small code generation differences and
the spread multiplier accommodates repeat variation in threaded paths. An
increase strictly above the threshold fails the comparison. JSON output
contains all six counts per workload. The exit statuses are 0 for a pass, 1
for a regression and 2 for a tool or input error.

These counts cover the measured code paths and exclude kernel instructions.
Absolute latency and throughput targets still use their dedicated benchmarks.
O09 tracks the associated clock-seam change. Clock jumps and real-time
ordering (OLTP-G02) require their own functional assertions.
