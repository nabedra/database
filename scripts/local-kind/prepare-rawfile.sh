#!/usr/bin/env bash
set -euo pipefail
output=${1:?usage: prepare-rawfile.sh OUTPUT_DIRECTORY}
mkdir -p "$output"
chart=rawfile-localpv-0.15.3.tgz
if [[ ! -f "$output/$chart" ]]; then
  gh release download v0.15.3 --repo openebs/rawfile-localpv --pattern "$chart" --dir "$output"
fi
(
  cd "$output"
  printf '%s  %s\n' 57da44022c61e8e6b08650c34308997fb06f48bc66f37afacf0fdcc61ee08bcb "$chart" | shasum -a 256 -c
)
helm template live3-rawfile "$output/$chart" --namespace default \
  --set global.analytics.enabled=false --set analytics.enabled=false \
  --set capabilities.snapshots.enabled=false --set capabilities.apiServer.enabled=false \
  --set crds.enabled=false --set 'snapshotClasses[0].enabled=false' \
  --set-string node.storagePools.default.reservedCapacity=6GiB \
  --set node.storagePools.default.reservedCapacityMode=subtract-from-total \
  > "$output/rawfile.yaml"
