#!/usr/bin/env bash
# RD2 controlled-host session: holds the Docker lane (BENCH) for all runs.
set -u
L=$HOME/.t3/worktrees/nabedra-database/nabedra-hardening-4cde81a5fa19a0c7/.nabedra/workflows/hardening/rd2
LANE=$HOME/.t3/worktrees/nabedra-database/remove-rocksdb/.nabedra/workflows/remove-rocksdb/docker-lane.lock
until mkdir "$LANE" 2>/dev/null; do sleep 30; done
echo "RD2 BENCH $(date)" > "$LANE/owner"
trap 'rm -rf "$LANE"' EXIT
NOWARM="--worker-env NABEDRA_BENCH_NO_GRANT_WARM=1"
for step in "$@"; do
  case $step in
    serial-on*) $L/run.sh cand-${step#serial-} serial ;;
    serial-off*) $L/run.sh cand-${step#serial-} serial $NOWARM ;;
    update-cand*) $L/run.sh ${step#update-} update ;;
    update-gc2*) $L/run.sh ${step#update-} update ;;
    fixed-cand*) $L/run.sh ${step#fixed-} fixed ;;
    fixed-gc2*) $L/run.sh ${step#fixed-} fixed ;;
    *) echo "unknown step $step" ;;
  esac
done
echo "session done $(date -u +%FT%TZ)" | tee -a $L/load.log
