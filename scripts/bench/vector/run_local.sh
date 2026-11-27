#!/usr/bin/env bash
# Runs bench_local on one set, pinned to the server CPUs, and appends JSON lines to the results.
#
#   run_local.sh <set> <gt dir name> <run name> [bench_local options...]
#   e.g. run_local.sh cohere-1m gt-1000000-1000 cohere-sweep --ops cosine --units 8,16,32
#
# Env: SMOKE=1 runs on a contended machine (lines carry "contended": true in meta); otherwise the
# run refuses unless no cargo runs or the lead wrote WINDOW-GRANTED. REPS (default 1; 3 for gate
# numbers), CPUS (default 0-7: physical cores 0-3, the competitors' set), DAY (results directory).
set -euo pipefail
SET=${1:?set}; GT=${2:?gt dir}; NAME=${3:?run name}; shift 3
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
DATA=${BENCH_DATA:-/home/dev/bench-data}
BENCH=/home/dev/logs/oltp-vector/bench
OUT=$BENCH/${DAY:-$(date +%F)}
BIN=${BIN:-$REPO/server/target/release/examples/bench_local}
CPUS=${CPUS:-0-7}
REPS=${REPS:-1}

free=$(df -BG --output=avail /mnt/c | tail -1 | tr -dc 0-9)
if (( free < 25 )); then echo "STOP: ${free} GB free on /mnt/c" >&2; exit 3; fi
cargo=$(pgrep -c cargo || true)
if [[ "${SMOKE:-0}" != 1 && "$cargo" -gt 0 && ! -f $BENCH/WINDOW-GRANTED ]]; then
  echo "refusing a measured run: $cargo cargo processes and no $BENCH/WINDOW-GRANTED" >&2
  exit 4
fi
[[ "${SMOKE:-0}" == 1 ]] && NAME="$NAME-smoke"
mkdir -p "$OUT"
SCRATCH=$DATA/scratch/$NAME
for rep in $(seq 1 "$REPS"); do
  "$HERE/meta.sh" "$REPO" > "$OUT/$NAME.meta.json"
  rm -rf "$SCRATCH"; mkdir -p "$SCRATCH"
  echo "run $NAME rep $rep on CPUs $CPUS"
  /usr/bin/time -v taskset -c "$CPUS" "$BIN" --data "$DATA/$SET" --gt "$DATA/$SET/$GT" \
    --meta "$OUT/$NAME.meta.json" --out "$OUT/$NAME.jsonl" --label "$NAME/rep$rep" \
    --scratch "$SCRATCH" "$@" 2> >(tee -a "$OUT/$NAME.stderr" >&2)
  rm -rf "$SCRATCH"
done
"${BENCH_PY:-/home/dev/bench-venv/bin/python}" -I "$HERE/summarize.py" "$OUT/$NAME.jsonl" | tee "$OUT/$NAME.summary.md"
