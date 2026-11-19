#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 output-directory" >&2
  exit 2
fi

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
mkdir -p "$1"
output=$(cd "$1" && pwd)
for arch in amd64 arm64; do
  staging=$(mktemp -d "$output/.cache-linux-$arch.XXXXXXXX")
  docker buildx build --platform "linux/$arch" --build-arg NABEDRA_ENGINE=cache \
    --target cache-release --output "type=local,dest=$staging" "$repo"
  tar -C "$staging" -czf "$output/nabedra-cache-linux-$arch.tar.gz" nabedra-db
  rm "$staging/nabedra-db"
  rmdir "$staging"
done

(
  cd "$output"
  shasum -a 256 nabedra-cache-linux-amd64.tar.gz nabedra-cache-linux-arm64.tar.gz > SHA256SUMS
)
