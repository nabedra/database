#!/usr/bin/env bash
# PERF1 (hardening): factor runs on the controlled host, one at a time, NS11 caps unless stated.
# usage: run.sh <workload> <binary> <api-cpus> [--hold-compaction]
#   workloads: fac (100/200/400 updates/s, fresh 5,000 rows), update (BENCH2 sweep),
#              soak50, soak100 (80,000 x 2 KiB rows, 5 x 60 s)
#   binary: before (5eb80fb, BENCH2's candidate = 7e110cb's product code) or after (this branch)
#   api-cpus: 0.35 (NS11), 0.7, 1.0 or none
# Before every run: wait until the 1-minute load average is below 3 (cap WAIT_CAP s), and log it.
set -u
REPO=${REPO:-$HOME/.t3/worktrees/nabedra-database/35d3dd65-a5b4f767b704f547}
L=${L:-$HOME/.t3/worktrees/nabedra-database/nabedra-hardening-4cde81a5fa19a0c7/.nabedra/workflows/hardening/perf1}
BEFORE_BIN=$HOME/.t3/worktrees/nabedra-database/remove-rocksdb/.nabedra/workflows/remove-rocksdb/bin/nabedra-db-5eb80fb
AFTER_BIN=${AFTER_BIN:?set AFTER_BIN to the [ci bench] binary of this branch}
# nabedra-oltp-perf-builder:1.94.1 by ID, as in BENCH2
IMAGE=sha256:1547483c44543b51e35b72fce17d9bf49d6b6f56c027dd5b634392006f83f7dc
WAIT_CAP=${WAIT_CAP:-1800}
COMMON="--seed 20261118 --timeout 10 --direct-oltp on"
declare -A W=(
  [fac]="--rates 100,200,400 --seconds 5 --warmup-seconds 3 --repetitions 3 --clients 32 --rows 5000"
  [update]="--rates 1,2,5,20,50,100,200,500,1000 --seconds 5 --warmup-seconds 3 --repetitions 3 --clients 32 --rows 5000 --stop-p99-ms 100 --stop-on-rejection"
  [soak50]="--soak --soak-rows 80000 --soak-payload-bytes 2048 --soak-seconds 300 --soak-phase-seconds 60 --soak-rate 50 --clients 32 --soak-verify-keys 500"
  [soak100]="--soak --soak-rows 80000 --soak-payload-bytes 2048 --soak-seconds 300 --soak-phase-seconds 60 --soak-rate 100 --clients 32 --soak-verify-keys 500"
)
w=$1 b=$2 api=$3; shift 3
case $b in before) bin=$BEFORE_BIN;; after) bin=$AFTER_BIN;; *) echo "unknown binary $b"; exit 2;; esac
name="$w-$b-api$api"; [ "${1:-}" = --hold-compaction ] && name="$name-hold"
out=$L/runs/$name
if [ -f "$out.rc" ]; then echo "skip $name (done)"; exit 0; fi
rm -rf "$out"
load1() { sysctl -n vm.loadavg | awk '{print $2}'; }
waited=0
while awk -v l="$(load1)" 'BEGIN{exit !(l >= 3)}'; do
  if (( waited >= WAIT_CAP )); then echo "=== $name load wait cap reached after ${waited}s" | tee -a $L/load.log; break; fi
  sleep 15; waited=$((waited + 15))
done
echo "=== $name start $(date -u +%FT%TZ) waited=${waited}s load: $(sysctl -n vm.loadavg)" | tee -a $L/load.log
start=$(date +%s)
cd $REPO/scripts
for i in $(seq 1 30); do docker image inspect $IMAGE >/dev/null 2>&1 && break; sleep 5; done
python3.11 benchmark_oltp_nabedra.py --image $IMAGE --binary "$bin" --api-cpus "$api" "$@" --output "$out" ${W[$w]} $COMMON \
  > $L/runs/$name.console.log 2>&1
rc=$?; echo $rc > "$out.rc"
echo "=== $name end $(date -u +%FT%TZ) rc=$rc seconds=$(( $(date +%s) - start )) load: $(sysctl -n vm.loadavg)" | tee -a $L/load.log
