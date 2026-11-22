#!/usr/bin/env bash
# BENCH3: build this directory's compact evidence from the full run outputs in the ledger.
set -eu
L=${L:-/Users/antoniopenapena/.t3/worktrees/nabedra-database/nabedra-hardening-4cde81a5fa19a0c7/.nabedra/workflows/hardening/bench3}
cd "$(dirname "$0")"
all=(); nab=(); soak=()
for d in "$L"/runs/*/; do
  d=${d%/}; all+=("$d"); n=$(basename "$d")
  case $n in *scylla*) ;; *) nab+=("$n") ;; esac
  case $n in soak*) soak+=("$d") ;; esac
done
python3.11 collect.py runs "${all[@]}"
python3.11 gstats.py runs "$L/runs" "${nab[@]}" > memory-and-caps.json
python3.11 rates.py --json "${nab[@]/#/$L/runs/}" > rates.json
python3.11 rates.py "${nab[@]/#/$L/runs/}" > rates.txt
python3.11 storage_soak.py "${soak[@]}" > storage-soak.json
for r in 100 200; do
  python3.11 peak_at.py $r "$L"/runs/{update-cr-cand,update-cr-main,get-cand,get-main,mix-cand,mix-main}
done > memory-equal-load.txt
cp "$L/load.log" load.log
cp "$L/w4-top.log" w4-top.log
rm -rf __pycache__
