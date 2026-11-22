#!/usr/bin/env bash
# BENCH3: one controlled-host window. Takes the shared Docker lane with BENCH in the owner file, runs the
# steps in order, and releases the lane on exit (also on failure or interrupt).
# usage: session.sh <window name> <workload>:<system>[,<system>...] ...
#   e.g. session.sh w1 serial:cand-1,main-1,scylla-1 update-cr:cand,main update:scylla
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
L=${L:-$HOME/.t3/worktrees/nabedra-database/nabedra-hardening-4cde81a5fa19a0c7/.nabedra/workflows/hardening/bench3}
LANE=$HOME/.t3/worktrees/nabedra-database/remove-rocksdb/.nabedra/workflows/remove-rocksdb/docker-lane.lock
name=$1; shift
until mkdir "$LANE" 2>/dev/null; do sleep 30; done
echo "BENCH3 BENCH window $name $(date)" > "$LANE/owner"
trap 'rm -rf "$LANE"; echo "=== window '"$name"' lane released $(date -u +%FT%TZ)" | tee -a $L/load.log' EXIT
trap 'exit 130' INT TERM
echo "=== window $name lane taken $(date -u +%FT%TZ)" | tee -a $L/load.log
for step in "$@"; do
  w=${step%%:*}; systems=${step#*:}
  "$HERE/run.sh" "$w" ${systems//,/ }
done
