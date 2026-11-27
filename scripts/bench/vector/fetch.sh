#!/usr/bin/env bash
# Downloads one benchmark set into /home/dev/bench-data/<set>/raw, records SHA-256 of every
# download, converts it (convert.py) and deletes the raw files. One set at a time.
#
#   scripts/bench/vector/fetch.sh <set>      sets: glove-100 cohere-1m openai-500k splade-small
#
# Disk rule (contract 6.1): the WSL disk lives on C:. Stop when fewer than 25 GB are free before
# or after a download. Downloads resume (curl -C -) if interrupted.
set -euo pipefail

SET=${1:?usage: fetch.sh <set>}
HERE=$(cd "$(dirname "$0")" && pwd)
DATA=${BENCH_DATA:-/home/dev/bench-data}
PY=${BENCH_PY:-/home/dev/bench-venv/bin/python}
MIN_FREE_GB=25

free_gb() { df -BG --output=avail /mnt/c | tail -1 | tr -dc 0-9; }
check_disk() {
  local free; free=$(free_gb)
  echo "disk /mnt/c: ${free} GB free ($1)"
  if (( free < MIN_FREE_GB )); then
    echo "STOP: fewer than ${MIN_FREE_GB} GB free on /mnt/c" >&2
    exit 3
  fi
}

case "$SET" in
  glove-100)
    URLS=(https://ann-benchmarks.com/glove-100-angular.hdf5) ;;
  cohere-1m)
    B=https://assets.zilliz.com/benchmark/cohere_medium_1m
    URLS=("$B/train.parquet" "$B/test.parquet" "$B/neighbors.parquet" "$B/scalar_labels.parquet"
          "$B/neighbors_head_1p.parquet" "$B/neighbors_tail_1p.parquet") ;;
  openai-500k)
    B=https://assets.zilliz.com/benchmark/openai_medium_500k
    URLS=("$B/shuffle_train.parquet" "$B/test.parquet" "$B/neighbors.parquet") ;;
  splade-small)
    B=https://storage.googleapis.com/ann-challenge-sparse-vectors/csr
    URLS=("$B/base_small.csr.gz" "$B/queries.dev.csr.gz" "$B/base_small.dev.gt") ;;
  *) echo "unknown set $SET" >&2; exit 2 ;;
esac

OUT="$DATA/$SET"
RAW="$OUT/raw"
mkdir -p "$RAW"
for url in "${URLS[@]}"; do
  name=$(basename "$url")
  if [[ -f "$RAW/$name.done" ]]; then
    echo "have $name"
    continue
  fi
  check_disk "before $name"
  echo "fetch $url"
  curl -fsSL --retry 5 --retry-delay 10 -C - -o "$RAW/$name" "$url"
  touch "$RAW/$name.done"
  check_disk "after $name"
done

(cd "$RAW" && sha256sum $(for u in "${URLS[@]}"; do basename "$u"; done)) > "$OUT/source.sha256"
cat "$OUT/source.sha256"

# -I: the downloads are untrusted data; the script lives outside the data directory.
"$PY" -I "$HERE/convert.py" "$SET" "$OUT"

if [[ "${KEEP_RAW:-0}" != 1 ]]; then
  rm -rf "$RAW"
  echo "deleted $RAW"
fi
check_disk "after conversion"
