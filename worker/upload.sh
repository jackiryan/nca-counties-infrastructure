#!/usr/bin/env bash
#
# Upload the snapshot made by ../scripts/snapshot_backend.sh to the nca-counties R2 bucket.
# Object keys are the paths under snapshot/, e.g. tiles/counties.pmtiles.
#
#   ./upload.sh            to the real bucket
#   ./upload.sh --local    to the local bucket that `npm run dev` uses

set -euo pipefail

cd "$(dirname "$0")"
SNAPSHOT="${SNAPSHOT:-../snapshot}"
target="${1:---remote}"
[[ $target == --remote || $target == --local ]] || { echo "usage: $0 [--remote|--local]" >&2; exit 1; }
[[ -f $SNAPSHOT/tiles/counties.pmtiles ]] || { echo "no snapshot in $SNAPSHOT; run ../scripts/snapshot_backend.sh" >&2; exit 1; }

list=$(mktemp)
trap 'rm -f "$list"' EXIT
(cd "$SNAPSHOT" && find tiles api -type f | LC_ALL=C sort) | while read -r key; do
    printf '{"key":"%s","file":"%s/%s"}\n' "$key" "$SNAPSHOT" "$key"
done | paste -sd, - | sed 's/^/[/; s/$/]/' > "$list"

npx wrangler r2 bulk put nca-counties "$target" --filename "$list"
