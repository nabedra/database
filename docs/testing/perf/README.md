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
