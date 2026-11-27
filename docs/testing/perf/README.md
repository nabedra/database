# Paired performance checks

Build both revisions for the same target and run them on a single benchmark
runner. Pass their binary paths to the A/B tool. The Events spec takes two local
Docker image references instead.

```sh
python3.11 scripts/testing/ab.py docs/testing/perf/oltp/point-update.toml \
  /path/to/base/nabedra-db /path/to/head/nabedra-db --output /path/to/evidence
```

| Spec | Input and runner setup | Metrics |
| --- | --- | --- |
| `cache/get-hit` | Cache binaries on Linux. Set `NABEDRA_PERF_VALKEY` and `NABEDRA_PERF_VALKEY_CLI` to the reference tool paths and `NABEDRA_PERF_CLIENT_CPU` to a client CPU ID. Follow the prerequisites in [the Cache benchmark](../cache-benchmark.md). | GET-hit and SET PX p50 at 24 offered requests/s with the stock client; their p99s are reported. |
| `events/publish` | Two locally built Events images and Docker. | Median of the publish p99 values from the measured load stages; poll p99 is reported. |
| `kv/store` | The release `store_bench` test executable of `nabedra-kv`. | NS18 16-writer synced put, get, scan and install costs, NS5 flush and compaction throughput; the 1-writer put and append costs are reported. |
| `olap/streaming` | Matching OLAP binaries and the streaming fixture. Set `NABEDRA_PERF_BASE_SHA`, `NABEDRA_PERF_HEAD_SHA` and `NABEDRA_PERF_FIXTURE_SHA` to the source revisions. | Medians of the steady-state next-batch p99 and the first-batch p99 across prefetch depths, dense profile. |
| `olap/streaming-sparse` | As `olap/streaming`. | The same metrics for the sparse/wide profile, with ordinary requests during the stream. |
| `oltp/point-update` | Linux OLTP binaries and Docker. Set `NABEDRA_PERF_OLTP_IMAGE` to a locally available image with the benchmark's runtime dependencies. | Medians of the phase p99 and p50 latencies for direct updates at 20 offered requests/s. |
| `oltp/queued-update` | As `oltp/point-update`. | The same workload with direct OLTP off, so updates take the queued path. |
| `sim/oltp-seeds` | The release `nabedra-sim` binary, built with `RUSTFLAGS='--cfg tokio_unstable'`. | Wall time of a smoke sweep of seeds 1 to 8, each run twice for replay. |

A spec provides `name`, `module`, `command`, `warmup` pairs, `rounds` of
measured pairs, and either one `metric` or a `[metrics]` table. A command can
be an argument array or a shell-quoted string. The runner substitutes the
binary path for `{bin}`. With `--commands`, each side is parsed into an
argument list and `{bin}` must be a separate argument. Commands run from the
repository root. An optional `timeout_s` fails a command that runs longer.

Each invocation can refer to its empty scratch directory as `{work}` and its
side as `{side}`. The runner removes scratch directories after measurement.
Use `metric = "wall"` for command elapsed time in seconds. A regex metric
must have one capture group for a positive number. It reads combined command
output by default, or the scratch-relative `metric_file` when specified. The
runner takes the median if a command reports several matching measurements.
Use costs where higher values are worse, such as latency, allocations or RPCs
per operation.

A `[metrics]` table names several metrics read from the same invocation. Each
value is a regex, `"wall"`, or a table with `pattern`, an optional `file` that
overrides `metric_file`, `higher_is_better = true` for a rate such as MiB/s,
which the runner compares as its reciprocal, and `gate = false` for a metric
that is reported but cannot decide the comparison. The first metric is the
primary one, whose ratio and interval also appear at the top of
`results.json`. The comparison fails if any gating metric regresses, so every
gating metric adds a chance of a false alarm. Measure its A/A noise first, and
report a metric whose noise hides a 5% shift at the spec's rounds instead of
gating on it.

The runner alternates who goes first, including during warmup. It computes the
median head/base ratio across measured pairs and resamples those pairs with a
fixed seed to estimate a 95% confidence interval. A regression requires a ratio
at or above the configured threshold and an interval with a lower bound above
1. The defaults are `threshold_pct = 5` and `bootstrap_samples = 2000`.
`--rounds` and `--warmup` override the spec for a noise study.

`results.json` includes the CPU model, run order, runtime, and per metric the
paired measurements, the decision and a `noise` block: the range and median
absolute deviation of the pair ratios, the interval's half-width and each
side's spread. `summary.md` contains the decision. The runner exits 0 for a
pass, 1 for a regression and 2 for a command or input error. Use a distinct
output directory for each comparison.

## On GitHub Actions

`jobs=ab` builds release binaries of `ab_base` and `sha` and runs every spec
above, both sides on one runner per spec. `jobs=insn` runs the instruction
counts below for the same pair. With `ab_base` empty both sides are `sha`,
which measures the runner's noise (an A/A run). The specs, `ab.py` and the
benchmark drivers come from the workflow ref, so both sides use the same
driver. Results are in the `ab-<spec>` and `insn-counts` artifacts and the job
summaries.

```sh
gh workflow run nabedra-build.yml --repo nabedra/database --ref main \
  -f sha=<head> -f ab_base=<base> -f request_id=ab-<head7>-$(date +%H%M%S) \
  -f base=false -f jobs=ab,insn
```

## Phase 7 instruction counts

Use Callgrind on Linux or WSL to compare the phase 7 runtime wrappers in a
release build. Run from a checkout containing the harness and pass two refs
available in that clone. The registered `rt-zero-cost` check compares the
tested revision with the phase 7 base at `e3d51a6`. The check installs
Valgrind when needed on GitHub Actions. Install it before running locally.

```sh
python3 scripts/testing/insn_count.py --base e3d51a6 --head HEAD --output counts.json
```

Pass `--workload cache-set` to focus on a single workload during diagnosis.

The runner builds both commits in detached worktrees with private target
directories. These live under a sibling `<checkout>-work` directory by default.
Use `--work-dir` to put them on another disk. The runner uses `cargo-slot`
when available, or Cargo, with six build jobs. It inserts the same driver
into both checkouts, including visibility
hooks to the standalone Cache command handler and OLAP queue clock. Builds
use one engine feature at a time and the normal release profile.

| Workload | Operation count | Product path |
| --- | ---: | --- |
| OLTP storage | 512 | Apply a batch with one row overwrite in the Data group store and verify the reply and final row. |
| OLTP transport | 512 | Send one request and await its reply between two enrolled workers over the mutual-TLS worker transport, on loopback. |
| KV write and flush | 1,024 | Submit and await synced point writes on the native journal, flush to a run, and read back the last key. The flush cost is amortized over the writes. |
| Cache SET | 8,192 | Parse and execute one standalone SET PX command per operation, including the wall-clock expiry validation. |
| OLAP queue | 512 | Propose, acknowledge, start, finish and look up a request on a fresh in-process queue, using the production wall-clock helper. A fresh queue per operation keeps retained receipts from growing the cost of later operations. |

Each workload runs three times per binary in alternating order with
Callgrind fair scheduling to reduce variation in threaded paths. The result
uses the median process-wide instruction count divided by the completed
operation count. The counter includes setup, teardown and background threads.
The noise floor is the full spread of the three base counts divided by their
median. The threshold is the larger of 0.5% and three times that spread.
The fixed allowance leaves room for small code generation differences and
the spread multiplier accommodates repeat variation in threaded paths. An
increase strictly above the threshold fails the comparison. If three times
a base spread exceeds 2%, that workload is reported as `NOISY` and the run
cannot pass, because a wider threshold could hide a regression. JSON output
contains all six counts per workload. The exit statuses are 0 for a pass, 1
for a regression and 2 for a noisy base or a tool or input error.

The threshold is relative, so the smallest detectable increase depends on
the cost of one operation. At 0.5% it is about 20 instructions per
operation for Cache SET, 66 for KV, 170 for OLTP storage and 670 for the
OLAP queue, whose cost is mostly request digests and record validation.
Cache SET reads the wall clock once per operation, so it is the most
sensitive check of a wrapper's own cost.

These counts cover the measured code paths and exclude kernel instructions.
Absolute latency and throughput targets still use their dedicated benchmarks.
O09 tracks the associated clock-seam change. Clock jumps and real-time
ordering (OLTP-G02) require their own functional assertions.
