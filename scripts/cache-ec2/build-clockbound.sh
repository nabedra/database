#!/usr/bin/env bash
# Build the pinned daemon on the same Linux architecture as the Cache release.
set -euo pipefail
if [[ $# != 2 || ( "$1" != amd64 && "$1" != arm64 ) ]]; then
  echo "usage: $0 amd64|arm64 output-directory" >&2
  exit 2
fi
mkdir -p "$2"
output=$(cd "$2" && pwd)
docker run --rm --platform "linux/$1" \
  --mount "type=bind,source=$output,target=/out" \
  rust:1.94.1-bookworm cargo install --locked --version '=2.0.3' clock-bound-d --root "/out/clockbound-linux-$1"
