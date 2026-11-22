#!/usr/bin/env bash
# RD3 controlled-host runs (NS11 caps, RD2/GC2 settings), one system at a time.
# usage: run.sh <label> <workload> [extra driver args...]
#   workloads: serial (0.2/s x10), fixed (200,300,400,600/s, 64 clients, no stop rules)
#   labels pick the binary: cand* = RD3 bench binary, base* = RD2's 22a1934 (bb6dc32 + RD2)
set -u
REPO=$HOME/.t3/worktrees/nabedra-database/8f99b242-1667543e3074471c
L=$HOME/.t3/worktrees/nabedra-database/nabedra-hardening-4cde81a5fa19a0c7/.nabedra/workflows/hardening/rd3
IMAGE=sha256:1547483c44543b51e35b72fce17d9bf49d6b6f56c027dd5b634392006f83f7dc
COMMON="--seed 20261118 --timeout 10"
declare -A W=(
  [serial]="--rates 0.2 --seconds 5 --warmup-seconds 5 --repetitions 10 --clients 1 --rows 2000"
  [fixed]="--rates 200,300,400,600 --seconds 8 --warmup-seconds 3 --repetitions 2 --clients 64 --rows 5000"
)
label=$1; w=$2; shift 2
case $label in
  cand*) BIN=$(ls $L/bin/nabedra-db-* | grep -v build | head -1) ;;
  base*) BIN=$L/../rd2/bin/nabedra-db-22a1934 ;;
  fin*) BIN=$(ls $L/bin2/nabedra-db-* | grep -v build | head -1) ;;
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
echo "=== $w-$label start $(date -u +%FT%TZ) waited=${waited}s bin=$(basename $BIN) args=$* load: $(sysctl -n vm.loadavg)" | tee -a $L/load.log
start=$(date +%s)
cd $REPO/scripts
python3.11 benchmark_oltp_nabedra.py --image $IMAGE --binary $BIN --direct-oltp on --output "$out" ${W[$w]} $COMMON "$@" > $L/runs/$w-$label.console.log 2>&1
rc=$?; echo $rc > "$out.rc"
echo "=== $w-$label end $(date -u +%FT%TZ) rc=$rc seconds=$(( $(date +%s) - start )) load: $(sysctl -n vm.loadavg)" | tee -a $L/load.log
