#!/usr/bin/env bash
# Run the ScyllaDB comparison in four capped containers.
set -euo pipefail

if (( $# == 0 )); then
    echo "Usage: $0 OUTPUT_DIRECTORY [workload arguments]" >&2
    exit 2
fi

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
mkdir -p "$1"
SCYLLA_BENCHMARK_OUTPUT=$(cd "$1" && pwd)
export SCYLLA_BENCHMARK_OUTPUT
shift

project="scylla-oltp-$(date +%s)-$$"
driver="${project}-measurement"
compose=(docker compose --project-name "$project" --file "$script_dir/benchmark_scylla/compose.yaml")

cleanup() {
    local status=$?
    trap - EXIT
    if (( status != 0 )); then
        "${compose[@]}" logs --no-color > "$SCYLLA_BENCHMARK_OUTPUT/cluster.log" 2>&1 || true
    fi
    docker rm "$driver" >/dev/null 2>&1 || true
    if ! "${compose[@]}" down --volumes --remove-orphans --timeout 10; then
        exit 1
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"${compose[@]}" build --builder "$(docker context show)" driver
nodes=()
for node in scylla1 scylla2 scylla3; do
    nodes+=("$node")
    "${compose[@]}" up --detach "$node"
    "${compose[@]}" run --no-deps --rm -T driver --wait-only --hosts "${nodes[@]}"
done

status=0
"${compose[@]}" run --no-deps -T --name "$driver" driver "$@" || status=$?
ids=()
for node in "${nodes[@]}"; do
    ids+=("$("${compose[@]}" ps --all --quiet "$node")")
done
inspect_format='{"name":{{json .Name}},"image":{{json .Config.Image}},"image_id":{{json .Image}},"nano_cpus":{{.HostConfig.NanoCpus}},"memory_bytes":{{.HostConfig.Memory}},"swap_limit_bytes":{{.HostConfig.MemorySwap}},"oom_killed":{{.State.OOMKilled}},"status":{{json .State.Status}}}'
docker inspect --format "$inspect_format" "${ids[@]}" "$driver" > "$SCYLLA_BENCHMARK_OUTPUT/containers.jsonl"
python3 - "$SCYLLA_BENCHMARK_OUTPUT/containers.jsonl" <<'PY'
import json
import sys

with open(sys.argv[1]) as records:
    containers = [json.loads(record) for record in records]
nodes, driver = containers[:3], containers[3]
for container, cpus, memory in [*((node, 1_000_000_000, 2_150_000_000) for node in nodes),
                                (driver, 650_000_000, 900_000_000)]:
    if (container["nano_cpus"], container["memory_bytes"], container["swap_limit_bytes"]) != (cpus, memory, memory):
        raise SystemExit(f"unexpected resource limits: {container}")
    if container["oom_killed"]:
        raise SystemExit(f"out-of-memory termination: {container}")
PY
for node in "${nodes[@]}"; do
    "${compose[@]}" exec -T "$node" sh -c '
        for name in cpu.stat io.stat memory.current memory.peak memory.events; do
            if [ -r "/sys/fs/cgroup/$name" ]; then
                echo "$name"
                cat "/sys/fs/cgroup/$name"
            fi
        done
    ' > "$SCYLLA_BENCHMARK_OUTPUT/${node}.cgroup.txt"
done
exit "$status"
