#!/usr/bin/env bash
#
# Snapshot the Postgres/martin/FastAPI backend into static files for the nca-counties
# Worker (see worker/README.md). The data never changes after seeding, so everything the
# frontend can ask for is saved once:
#
#   snapshot/tiles/counties.pmtiles
#       County shapes with the id, name and state_abbr properties the frontend reads, built
#       with tippecanoe from the counties table.
#   snapshot/api/climate-variables/<gwl>/<relative|absolute>/<var>.json
#       The API's response to /climate-variables?gwl=<gwl>&var=<var>&relative=<True|False>
#       for every warming level and climate variable, saved byte for byte.
#
# Needs the old backend running, plus tippecanoe and curl on this computer. Settings:
#   PSQL     psql command for the database (default: psql "$DATABASE_URL"). For the VPS:
#            PSQL="ssh jackiepi-xyz-uswest docker exec -i nca-counties-infrastructure-ar-db-1 psql -U postgres -d ar_climate_data"
#   API_URL  where the FastAPI service is reachable (default: https://jackiepi.xyz/api)
#   OUT      output directory (default: snapshot)

set -euo pipefail

cd "$(dirname "$0")/.."
PSQL="${PSQL:-psql \"\$DATABASE_URL\"}"
API_URL="${API_URL:-https://jackiepi.xyz/api}"
OUT="${OUT:-snapshot}"

GWLS=(1.0 2.0 3.0 4.0)
VARS=(tavg pr_above_nonzero_99th prmax1day prmax5yr tmax1day tmax_days_ge_100f
    tmax_days_ge_105f tmax_days_ge_95f tmean_jja tmin_days_ge_70f tmin_days_le_0f
    tmin_days_le_32f tmin_jja pr_annual pr_days_above_nonzero_99th)

command -v tippecanoe >/dev/null || { echo "tippecanoe is not installed (brew install tippecanoe)" >&2; exit 1; }
mkdir -p "$OUT/tiles"

# One GeoJSON feature per line. FETCH_COUNT makes psql read through a cursor, so neither
# end holds the whole result (about 100 MB) in memory.
echo "Exporting county shapes"
eval "$PSQL" -X -q -At -v ON_ERROR_STOP=1 -v FETCH_COUNT=100 > "$OUT/counties.ndjson" <<'SQL'
SELECT json_build_object(
    'type', 'Feature',
    'properties', json_build_object('id', id, 'name', name, 'state_abbr', state_abbr),
    'geometry', ST_AsGeoJSON(geom, 6)::json
) FROM counties ORDER BY id;
SQL
echo "  $(wc -l < "$OUT/counties.ndjson" | tr -d ' ') counties"

# z10 is about 10 m per tile pixel, more detail than county shapes need; the frontend
# enlarges z10 tiles past that (maxZoom on its source). Every county must be in every
# tile it touches for the choropleth, hence no feature or size limits and no tiny-polygon
# reduction. Shared borders are simplified once so neighboring counties still meet.
echo "Building tiles"
tippecanoe --force --quiet -o "$OUT/tiles/counties.pmtiles" \
    --layer=counties_gwl --minimum-zoom=0 --maximum-zoom=10 \
    --no-feature-limit --no-tile-size-limit --no-tiny-polygon-reduction \
    --detect-shared-borders --read-parallel \
    "$OUT/counties.ndjson"
rm "$OUT/counties.ndjson"

# One request at a time: the VPS has little memory to spare.
echo "Saving API responses from $API_URL"
for gwl in "${GWLS[@]}"; do
    for relative in True False; do
        kind=$([[ $relative == True ]] && echo relative || echo absolute)
        dir="$OUT/api/climate-variables/$gwl/$kind"
        mkdir -p "$dir"
        for var in "${VARS[@]}"; do
            curl -fsS --retry 3 -o "$dir/$var.json" \
                "$API_URL/climate-variables?gwl=$gwl&var=$var&relative=$relative"
        done
    done
done
echo "  $(find "$OUT/api" -name '*.json' | wc -l | tr -d ' ') responses"
echo "Done: $OUT/"
