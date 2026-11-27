#!/usr/bin/env bash
# Prints one JSON object describing the machine and the code under test (contract 6.4: git SHA,
# lscpu -e, kernel, governor; machine settings are recorded, never changed).
#   meta.sh [repo dir]  > meta.json
set -euo pipefail
REPO=${1:-$(cd "$(dirname "$0")/../../.." && pwd)}
PY=${BENCH_PY:-/home/dev/bench-venv/bin/python}

governor=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo "unavailable (WSL exposes no cpufreq)")
powercfg=/mnt/c/Windows/System32/powercfg.exe
plan=$([[ -x $powercfg ]] && "$powercfg" /getactivescheme 2>/dev/null | tr -d '\r' || echo unavailable)

GIT_SHA=$(git -C "$REPO" rev-parse HEAD) \
GIT_DIRTY=$(git -C "$REPO" status --porcelain --untracked-files=no | wc -l) \
GIT_BRANCH=$(git -C "$REPO" rev-parse --abbrev-ref HEAD) \
LSCPU_E=$(lscpu -e) \
CPU_MODEL=$(lscpu | sed -n 's/^Model name: *//p') \
KERNEL=$(uname -r) \
GOVERNOR=$governor \
POWER_PLAN=$plan \
MEM_TOTAL=$(awk '/MemTotal/ {print $2 * 1024}' /proc/meminfo) \
LOADAVG=$(cut -d' ' -f1-3 /proc/loadavg) \
CARGO_PROCS=$(pgrep -c cargo || true) \
DOCKER=$(docker version --format '{{.Server.Version}}' 2>/dev/null || echo none) \
"$PY" -I -c '
import json, os, datetime
keys = ["GIT_SHA", "GIT_DIRTY", "GIT_BRANCH", "LSCPU_E", "CPU_MODEL", "KERNEL", "GOVERNOR", "POWER_PLAN",
        "MEM_TOTAL", "LOADAVG", "CARGO_PROCS", "DOCKER"]
meta = {k.lower(): os.environ[k] for k in keys}
meta["contended"] = int(meta["cargo_procs"] or 0) > 0 and not os.path.exists(
    "/home/dev/logs/oltp-vector/bench/WINDOW-GRANTED")
meta["time"] = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
print(json.dumps(meta, separators=(",", ":")))
'
