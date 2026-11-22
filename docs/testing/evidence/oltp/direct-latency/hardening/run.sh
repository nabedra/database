#!/usr/bin/env bash
# BENCH3: final comparative runs of the hardening PR on the controlled host (NS11 caps), one system at a time.
# usage: run.sh <workload> <system...>
#   workloads: serial serial-get update-cr update get mix soak50 soak100   (suffix -<n> on a system label repeats it)
#   systems:   cand (nabedra/hardening c38a4a7), main (6141321), scylla (ScyllaDB 6.2.3)
# Before every run: wait until the 1-minute load average is below 3 (cap WAIT_CAP s), and log it.
# Settings are BENCH2's (docs/testing/evidence/oltp/direct-latency/final/run.sh); the sweep rates and the
# 5-minute soaks are BENCH3's brief.
set -u
REPO=${REPO:-/Users/antoniopenapena/.t3/worktrees/nabedra-database/9fbb3789-9817c99d5e1ba610}
L=${L:-/Users/antoniopenapena/.t3/worktrees/nabedra-database/nabedra-hardening-4cde81a5fa19a0c7/.nabedra/workflows/hardening/bench3}
CAND_BIN=$L/bin/nabedra-db-c38a4a7
MAIN_BIN=$L/bin/nabedra-db-6141321
# nabedra-oltp-perf-builder:1.94.1 by ID (Docker Desktop sometimes fails to resolve the tag)
IMAGE=sha256:1547483c44543b51e35b72fce17d9bf49d6b6f56c027dd5b634392006f83f7dc
WAIT_CAP=${WAIT_CAP:-600}
COMMON="--seed 20261118 --timeout 10"
RATES=1,5,20,50,100,200,300,400,600
SWEEP="--rates $RATES --seconds 5 --warmup-seconds 3 --repetitions 3 --clients 32 --rows 5000 --stop-p99-ms 100 --stop-on-rejection"
SERIAL="--rates 0.2 --seconds 5 --warmup-seconds 5 --repetitions 10 --clients 1 --rows 2000"
SOAK="--soak --soak-rows 80000 --soak-payload-bytes 2048 --soak-seconds 300 --soak-phase-seconds 60 --clients 32 --soak-verify-keys 500 --cold-restart"
declare -A W=(
  [serial]="$SERIAL"
  [serial-get]="--operation get $SERIAL"
  [update]="$SWEEP"
  [update-cr]="$SWEEP --cold-restart"
  [get]="--operation get $SWEEP"
  [mix]="--operation mix --read-fraction 0.95 $SWEEP"
  [soak50]="$SOAK --soak-rate 50"
  [soak100]="$SOAK --soak-rate 100"
)
declare -A S=(
  [cand]="--binary $CAND_BIN --direct-oltp on"
  [main]="--binary $MAIN_BIN --direct-oltp on"
)
load1() { sysctl -n vm.loadavg | awk '{print $2}'; }
w=$1; shift
cd $REPO/scripts
for label in "$@"; do
  s=${label%%-*}
  out=$L/runs/$w-$label
  if [ -f "$out.rc" ]; then echo "skip $w-$label (done)"; continue; fi
  rm -rf "$out"
  waited=0
  while awk -v l="$(load1)" 'BEGIN{exit !(l >= 3)}'; do
    if (( waited >= WAIT_CAP )); then echo "=== $w-$label load wait cap reached after ${waited}s" | tee -a $L/load.log; break; fi
    sleep 15; waited=$((waited + 15))
  done
  echo "=== $w-$label start $(date -u +%FT%TZ) waited=${waited}s load: $(sysctl -n vm.loadavg)" | tee -a $L/load.log
  start=$(date +%s)
  if [ "$s" = scylla ]; then
    ./benchmark_scylla_oltp.sh "$out" ${W[$w]} $COMMON > $L/runs/$w-$label.console.log 2>&1
  else
    for i in $(seq 1 30); do docker image inspect $IMAGE >/dev/null 2>&1 && break; sleep 5; done
    python3.11 benchmark_oltp_nabedra.py --image $IMAGE ${S[$s]} --output "$out" ${W[$w]} $COMMON > $L/runs/$w-$label.console.log 2>&1
  fi
  rc=$?; echo $rc > "$out.rc"
  echo "=== $w-$label end $(date -u +%FT%TZ) rc=$rc seconds=$(( $(date +%s) - start )) load: $(sysctl -n vm.loadavg)" | tee -a $L/load.log
done
