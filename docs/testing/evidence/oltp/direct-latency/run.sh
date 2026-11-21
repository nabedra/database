#!/usr/bin/env bash
# PERF1 comparative runs on the controlled host (NS11 caps), one system at a time.
# usage: run.sh <label> <binary> <workload...>   workloads: serial update get
set -u
REPO=${REPO:-$(cd "$(dirname "$0")/../../../../.." && pwd)}
P=${OUT:-$PWD}
# nabedra-oltp-perf-builder:1.94.1 by ID: Docker Desktop intermittently fails to resolve the tag
IMAGE=${IMAGE:-sha256:1547483c44543b51e35b72fce17d9bf49d6b6f56c027dd5b634392006f83f7dc}
COMMON="--seed 20261118 --timeout 10"
SWEEP="--rates 1,2,5,20,50,100,200,500,1000 --seconds 5 --warmup-seconds 3 --repetitions 3 --clients 32 --rows 5000 --stop-p99-ms 100 --stop-on-rejection"
declare -A W=(
  [serial]="--rates 0.2 --seconds 5 --warmup-seconds 5 --repetitions 10 --clients 1 --rows 2000"
  [update]="$SWEEP"
  [get]="--operation get $SWEEP"
)
label=$1; bin=$2; shift 2
cd $REPO/scripts
for w in "$@"; do
  out=$P/runs/$w-$label
  if [ -f "$out.rc" ]; then echo "skip $w-$label (done)"; continue; fi
  rm -rf "$out"
  # Docker Desktop sometimes reports the image missing for a while (NS11 note 10).
  for i in $(seq 1 30); do docker image inspect $IMAGE >/dev/null 2>&1 && break; sleep 5; done
  echo "=== $w-$label start $(date -u +%FT%TZ) load: $(sysctl -n vm.loadavg)" | tee -a $P/load.log
  start=$(date +%s)
  python3.11 benchmark_oltp_nabedra.py --image $IMAGE --binary $bin --direct-oltp on --output "$out" ${W[$w]} $COMMON > $P/runs/$w-$label.console.log 2>&1
  rc=$?; echo $rc > "$out.rc"
  echo "=== $w-$label end $(date -u +%FT%TZ) rc=$rc seconds=$(( $(date +%s) - start )) load: $(sysctl -n vm.loadavg)" | tee -a $P/load.log
done
