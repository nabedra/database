#!/usr/bin/env bash
# BENCH2: final comparative runs on the controlled host (NS11 caps), one system at a time.
# usage: run.sh <workload> <system...>
#   workloads: serial serial-snap update update-cr get mix fsync soak soak100
#   systems:   cand-on cand-off main base scylla
# Before every run: wait until the 1-minute load average is below 3 (cap WAIT_CAP s), and log it.
set -u
REPO=${REPO:-/Users/antoniopenapena/.t3/worktrees/nabedra-database/761efc3a-8869f85d6de4c79e}
LR=/Users/antoniopenapena/.t3/worktrees/nabedra-database/remove-rocksdb/.nabedra/workflows/remove-rocksdb
L=$LR/bench2
LB=$LR/bin
CAND_BIN=${CAND_BIN:-$LB/nabedra-db-5eb80fb}
MAIN_BIN=$LB/nabedra-db-c74c2d4
BASE_BIN=/Users/antoniopenapena/.t3/worktrees/nabedra-database/nabedra-oltp-direct-path/.nabedra/workflows/oltp-direct-path/bin/nabedra-db-b29a178
# nabedra-oltp-perf-builder:1.94.1 by ID (Docker Desktop sometimes fails to resolve the tag)
IMAGE=sha256:1547483c44543b51e35b72fce17d9bf49d6b6f56c027dd5b634392006f83f7dc
STRACE_IMAGE=nabedra-bench2-strace:1.94.1
WAIT_CAP=${WAIT_CAP:-1800}
COMMON="--seed 20261118 --timeout 10"
SWEEP="--rates 1,2,5,20,50,100,200,500,1000 --seconds 5 --warmup-seconds 3 --repetitions 3 --clients 32 --rows 5000 --stop-p99-ms 100 --stop-on-rejection"
SERIAL="--rates 0.2 --seconds 5 --warmup-seconds 5 --repetitions 10 --clients 1 --rows 2000"
declare -A W=(
  [serial]="$SERIAL"
  [serial-snap]="$SERIAL --snapshot-between-phases"
  [update]="$SWEEP"
  [update-cr]="$SWEEP --cold-restart"
  [get]="--operation get $SWEEP"
  [mix]="--operation mix --read-fraction 0.95 $SWEEP"
  [fsync]="--rates 1,5,20 --seconds 5 --warmup-seconds 3 --repetitions 3 --clients 32 --rows 2000 --strace"
  [soak100]="--soak --soak-rows 80000 --soak-payload-bytes 2048 --soak-seconds 600 --soak-phase-seconds 60 --soak-rate 100 --clients 32 --soak-verify-keys 500 --cold-restart"
  [soak]="--soak --soak-rows 80000 --soak-payload-bytes 2048 --soak-seconds 600 --soak-phase-seconds 60 --soak-rate 50 --clients 32 --soak-verify-keys 500 --cold-restart"
)
declare -A S=(
  [cand-on]="--binary $CAND_BIN --direct-oltp on"
  [cand-off]="--binary $CAND_BIN --direct-oltp off"
  [main]="--binary $MAIN_BIN --direct-oltp on --orm-source $L/orm-c74c2d4-src"
  [base]="--binary $BASE_BIN --orm-source $LR/ns11/orm-b29a178-src --pool-format-version 1"
)
load1() { sysctl -n vm.loadavg | awk '{print $2}'; }
w=$1; shift
cd $REPO/scripts
for s in "$@"; do
  out=$L/runs/$w-$s
  if [ -f "$out.rc" ]; then echo "skip $w-$s (done)"; continue; fi
  rm -rf "$out"
  waited=0
  while awk -v l="$(load1)" 'BEGIN{exit !(l >= 3)}'; do
    if (( waited >= WAIT_CAP )); then echo "=== $w-$s load wait cap reached after ${waited}s" | tee -a $L/load.log; break; fi
    sleep 15; waited=$((waited + 15))
  done
  image=$IMAGE; timeout=""
  [ "$w" = fsync ] && { image=$STRACE_IMAGE; timeout="--timeout 60"; }
  echo "=== $w-$s start $(date -u +%FT%TZ) waited=${waited}s load: $(sysctl -n vm.loadavg)" | tee -a $L/load.log
  start=$(date +%s)
  if [ "$s" = scylla ]; then
    ./benchmark_scylla_oltp.sh "$out" ${W[$w]} $COMMON > $L/runs/$w-$s.console.log 2>&1
  else
    for i in $(seq 1 30); do docker image inspect $image >/dev/null 2>&1 && break; sleep 5; done
    python3.11 benchmark_oltp_nabedra.py --image $image ${S[$s]} --output "$out" ${W[$w]} $COMMON $timeout > $L/runs/$w-$s.console.log 2>&1
  fi
  rc=$?; echo $rc > "$out.rc"
  echo "=== $w-$s end $(date -u +%FT%TZ) rc=$rc seconds=$(( $(date +%s) - start )) load: $(sysctl -n vm.loadavg)" | tee -a $L/load.log
done
