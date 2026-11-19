# Paired single-host Cache benchmark

K04a compares native Nabedra Cache with Valkey 8.1.11, built from release commit `5fb5bc4b13b22084b54c7db2a88d14ce2f6fa867`. The reference image verifies the upstream tarball checksum and builds Valkey with TLS and its default allocator. The runner starts exactly one server at a time in the same Linux network namespace and uses the same CPU affinity, Linux `RLIMIT_AS`, cache data budget, loopback address and TLS certificate. Both configurations use sampled LRU with persistence and replication disabled. Make no inference about EC2 qualification from the Docker Desktop VM. [Valkey release](https://github.com/valkey-io/valkey/releases/tag/8.1.11) · [Upstream checksums](https://github.com/valkey-io/valkey-hashes)

## Build and run

Use a release binary built by the cache Docker target. The benchmark image installs the two ORM packages and the pinned stock RESP2 driver, redis-py 6.4.0 with hiredis 3.2.1. Build the release for the same architecture as the benchmark process.

```sh
docker build --build-arg NABEDRA_ENGINE=cache --target cache -t nabedra-cache-binary .
release_container=$(docker create nabedra-cache-binary)
mkdir -p artifacts/cache-binaries
docker cp "$release_container:/usr/local/bin/nabedra-db" artifacts/cache-binaries/nabedra-db
docker rm "$release_container"

docker build -f scripts/cache-benchmark/Dockerfile -t nabedra-cache-benchmark .
mkdir -p artifacts/cache-smoke
docker run --rm --cpuset-cpus 0-4 --memory 2g --memory-swap 2g \
  --mount "type=bind,source=$PWD/artifacts/cache-binaries,target=/binaries,readonly" \
  --mount "type=bind,source=$PWD/artifacts/cache-smoke,target=/results" \
  nabedra-cache-benchmark --nabedra /binaries/nabedra-db --output /results \
  --source-revision "$(git rev-parse HEAD)" --drivers stock \
  --transports tls --sizes 64 --workloads get-hit,set-px \
  --offered 4000,12000 --samples 1000 --repetitions 1
```

Use an empty output directory per run. Adjust `--server-cpus` and `--client-cpus` to distinct idle cores in the Docker affinity mask, with the server and load generator on separate physical cores where possible. Keep the host free of other benchmarks. As an initial diagnostic, the command above verifies the TLS comparison paths and raw evidence with two loads.

For the complete matrix omit `--drivers`, `--sizes`, `--workloads`, `--transports`, `--offered`, `--samples` and `--repetitions`. Default runs take substantial time and produce compressed raw samples for every run. Use `--p99-ms` to select a declared p99 budget. The runner includes the 1 ms lane when Valkey qualifies at 1 ms. Extend `--calibration-rates` until the sweep brackets the sustainable capacity, and include a low enough starting rate to observe an eligible Valkey cell. For a later EC2 qualification, use the same command in Docker on a dedicated EC2 instance with the EC2 environment label, a mounted host evidence JSON and `--ec2-host-evidence <path>`. The benchmark command does not provision or contact AWS. It reports numeric threshold results for review of the pinned builds, instance identity, CPU placement and raw samples.

## Workload and scoring

- The runner preloads uniform independent keys and verifies exact binary responses. It runs the stock RESP2 client, then the installed synchronous Python `nabedra.cache.Cache` and asynchronous TypeScript `nabedra/cache` clients. Both engines receive the same normalized key representation and offered arrival schedule. Every warm connection establishes and authenticates before the warm-up barrier, with one command per connection in flight. GET hits and SET PX with 64 B, 1 KiB and 64 KiB values are the required TLS cells. A successful SET PX is checked for visible bytes and a live TTL after draining.
- Each independent pair uses a reproducible seed and randomized engine order. Separate Valkey calibration determines the common offered rates at 5%, 50% and 80% of the largest rate meeting the p99 budget. Additional rates sweep through saturation on both engines. Goodput is correct operations divided by scheduled duration including any drain past that duration. Only a zero-error run with at least 99% of the offered goodput and within the p99 budget counts towards sustainable capacity. Use a sufficient client core count and inspect scheduling delay and server CPU for load-generator limits.
- Raw rows record operation ID, scheduled arrival, start, completion and outcome. A dropped or duplicate identity fails the runner. Backlog before dispatch counts toward latency. An already late arrival that misses the deadline is recorded and skipped before transmission. Client errors and unknown outcomes are retained and disqualify the load. Successful-operation percentiles include p50, p95 and p99 by repetition.
- K04a latency requires the upper bound of the paired 95% Student t interval on log ratios to be at most 1.20 for both p50 and p99. The reported parity band is 0.80–1.20 and faster results qualify. A bound crossing 1.20 is inconclusive. Goodput qualification uses the ratio of sustainable goodput in each pair and requires its lower 95% bound to reach 0.80. A sweep with no failure beyond the observed sustainable rate leaves capacity unbracketed.
- Plaintext, misses, mixed traffic, cold TLS (select `--transports cold`) and controlled stock pipelining (select `--drivers stock --pipeline 4`) report separate cells. Pipeline sizes and cold connections never qualify the required warm TLS cells. A fixed-rate diagnostic never qualifies capacity.

The output includes the release binary hashes, client versions, server configs with sample credentials removed, process RSS high-water marks, affinity, cgroup limits, per-run summaries and raw `.csv.gz` files. Preserve the raw files with the summary. CPU time includes warm-up and excludes the test oracle. The run does not execute cluster gates K04b or K05.

## Distributed benchmark

Use `scripts/cache-benchmark/cluster.py` inside the benchmark image with reachable private TLS endpoints for two matched cluster sets. It uses the same arrival scheduler, raw samples, reply oracles and paired confidence intervals as the standalone command. It checks full slot coverage and one replica per primary before each profile. Give it every advertised endpoint, including replica endpoints. A Nabedra worker can host both a primary and a replica group at the same endpoint.

The manifest has an optional `environment` label and a `clusters` object keyed by `"3"`, `"6"` and `"12"`. Under each size, give both `nabedra` and `valkey` an `endpoints` array of `rediss://host:port` URIs, a `certificate` path to the caller's CA bundle, a `password_env` naming the environment variable that holds the credential, and an optional `username`. Mount the certificates in the load generator's filesystem. The runner writes no auth credentials to raw evidence.

```sh
python scripts/cache-benchmark/cluster.py --manifest private-cluster-profile.json \
  --output artifacts/cache-cluster --client-cpus 2,3,4,5 \
  --rates 2500,5000,10000,20000,40000,80000
```

The offered rates are for three primary groups. The runner scales this sweep by group count, runs a separate Valkey calibration per size and case, and measures common rates on both engines. Increase the rate range until the budget crossing is bracketed. The run reports scale-out efficiency at six and twelve groups relative to the three-group goodput under the same p99 budget. Run `--distribution skew` separately and use `--distribution hot --workloads set-px` for the single-writer-key diagnostic.

Each lane warms one connection per primary. With the default two lanes per group, the twelve-group run needs 24 warm connections on every primary. For a worker configured with a 24-connection cap, use `--lanes-per-group 1` to leave room for control and verification clients.

Attach the matched configuration and CPU, memory, and network profiles of the deployed groups. Capture each worker's metrics during load and the controller resource profiles separately. Cluster qualification also requires provider-led migration and AZ-loss histories, verification of one primary hop on healthy traffic, and observed replication fan-out held at one. The runner reports numeric evidence for review.
