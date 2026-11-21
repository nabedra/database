#!/usr/bin/env bash
# RD2 controlled-host runs (NS11 caps, BENCH2/GC2 settings), one system at a time.
# usage: run.sh <label> <workload> [extra driver args...]
#   workloads: serial (0.2/s x10), update (GC2 sweep with stop rules), fixed (200,300,400/s, no stop rules)
#   labels pick the binary: cand* = RD2 bench binary, gc2* = GC2's 238b911
set -u
REPO=/Users/antoniopenapena/.t3/worktrees/nabedra-database/9583ea47-3a61458ee1324a02
L=/Users/antoniopenapena/.t3/worktrees/nabedra-database/nabedra-hardening-4cde81a5fa19a0c7/.nabedra/workflows/hardening/rd2
IMAGE=sha256:1547483c44543b51e35b72fce17d9bf49d6b6f56c027dd5b634392006f83f7dc
COMMON="--seed 20261118 --timeout 10"
declare -A W=(
  [serial]="--rates 0.2 --seconds 5 --warmup-seconds 5 --repetitions 10 --clients 1 --rows 2000"
  [update]="--rates 1,2,5,20,50,100,200,500,1000 --seconds 5 --warmup-seconds 3 --repetitions 3 --clients 32 --rows 5000 --stop-p99-ms 100 --stop-on-rejection"
  [fixed]="--rates 200,300,400 --seconds 10 --warmup-seconds 3 --repetitions 2 --clients 64 --rows 5000"
)
label=$1; w=$2; shift 2
case $label in
  cand*) BIN=$(ls $L/bin/nabedra-db-* | grep -v build | head -1) ;;
  gc2*) BIN=$L/../gc2/bin/nabedra-db-238b911 ;;
  *) echo "unknown label $label"; exit 2 ;;
esac
load1() { sysctl -n vm.loadavg | awk '{print $2}'; }
out=$L/runs/$w-$label
[ -f "$out.rc" ] && { echo "skip $w-$label"; exit 0; }
rm -rf "$out"; waited=0
while awk -v l="$(load1)" 'BEGIN{exit !(l >= 4)}'; do
  (( waited >= 900 )) && { echo "=== $w-$label load wait cap after ${waited}s" | tee -a $L/load.log; break; }
  sleep 15; waited=$((waited + 15))
done
for i in $(seq 1 30); do docker image inspect $IMAGE >/dev/null 2>&1 && break; sleep 5; done
echo "=== $w-$label start $(date -u +%FT%TZ) waited=${waited}s bin=$(basename $BIN) args=$* load: $(sysctl -n vm.loadavg)" | tee -a $L/load.log
start=$(date +%s)
cd $REPO/scripts
python3.11 benchmark_oltp_nabedra.py --image $IMAGE --binary $BIN --direct-oltp on --output "$out" ${W[$w]} $COMMON "$@" > $L/runs/$w-$label.console.log 2>&1
rc=$?; echo $rc > "$out.rc"
echo "=== $w-$label end $(date -u +%FT%TZ) rc=$rc seconds=$(( $(date +%s) - start )) load: $(sysctl -n vm.loadavg)" | tee -a $L/load.log
