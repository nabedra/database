#!/usr/bin/env bash
# RD3 controlled-host session: holds the Docker lane (BENCH) for all runs.
# steps: serial-<cand|nobc|base><n>, fixed-<cand|noadm|base><n>
set -u
L=$HOME/.t3/worktrees/nabedra-database/nabedra-hardening-4cde81a5fa19a0c7/.nabedra/workflows/hardening/rd3
LANE=$HOME/.t3/worktrees/nabedra-database/remove-rocksdb/.nabedra/workflows/remove-rocksdb/docker-lane.lock
until mkdir "$LANE" 2>/dev/null; do sleep 30; done
echo "RD3 BENCH $(date)" > "$LANE/owner"
trap 'rm -rf "$LANE"' EXIT
NOBC="--worker-env NABEDRA_BENCH_NO_SUBMIT_BROADCAST=1"
NOADM="--worker-env NABEDRA_BENCH_NO_DIRECT_ADMISSION=1"
for step in "$@"; do
  w=${step%%-*}; v=${step#*-}
  case $v in
    cand*) $L/run.sh cand-${v#cand} $w ;;
    nobc*) $L/run.sh cand-nobc${v#nobc} $w $NOBC ;;
    noadm*) $L/run.sh cand-noadm${v#noadm} $w $NOADM ;;
    base*) $L/run.sh base-${v#base} $w ;;
    fin*) $L/run.sh fin-${v#fin} $w ;;
    fnoadm*) $L/run.sh fin-noadm${v#fnoadm} $w $NOADM ;;
    *) echo "unknown step $step" ;;
  esac
done
echo "session done $(date -u +%FT%TZ)" | tee -a $L/load.log
