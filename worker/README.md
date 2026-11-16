# nca-counties Worker

Serves the [NCACounties](https://github.com/jackiryan/NCACounties) frontend without the Postgres, martin and FastAPI containers. The data never changes after seeding, so `../scripts/snapshot_backend.sh` saves everything the frontend can request into static files. The files live in the `nca-counties` R2 bucket, and this Worker answers the same URLs the VPS used to:

| Request | Answer |
|---|---|
| `GET /tiles/counties/{z}/{x}/{y}` | A county tile from `tiles/counties.pmtiles`: shapes with the `id`, `name` and `state_abbr` properties the frontend reads, zoom 0 to 10. `204` where a tile is empty or past z10. The frontend sets `maxZoom: 10` on this source, so OpenLayers enlarges z10 tiles when zoomed in further. |
| `GET /api/climate-variables?gwl=2.0&var=tavg&relative=True` | The saved FastAPI response, byte for byte. `gwl` is one of 1.0 to 4.0 (`2` works too), `var` one of the 15 climate variables, `relative` True or False (default True). Anything else returns `400`, including `county_id`, which the frontend never sends. |

Responses are cached at the edge with the Cache API, so R2 is only read on a cache miss. `/climate-normals` and `/base_tiles` are not carried over: the frontend doesn't use them, and the MapTiler proxy is the separate `maptiler-proxy` Worker.

## Setup

```bash
npm install
npx wrangler login
npx wrangler r2 bucket create nca-counties
```

## Updating the data

The snapshot needs the old backend running. To redo it:

```bash
PSQL="ssh jackiepi-xyz-uswest docker exec -i nca-counties-infrastructure-ar-db-1 psql -U postgres -d ar_climate_data" \
    ../scripts/snapshot_backend.sh
./upload.sh            # or ./upload.sh --local for npm run dev
```

## Deploying

1. `npm run deploy`. This serves the Worker only at `nca-counties.<account>.workers.dev`; the site is unchanged.
2. Check it against the old backend:
   ```bash
   WORKER_URL=https://nca-counties.<account>.workers.dev BASELINE_URL=https://jackiepi.xyz npm run compare
   ```
3. Go live: uncomment `routes` in `wrangler.jsonc` and `npm run deploy` again. Cloudflare then answers `jackiepi.xyz/api/*` and `jackiepi.xyz/tiles/counties/*` from the Worker, and those requests no longer reach the VPS.
4. Check the live site, twice so the second run is served from the edge cache:
   ```bash
   WORKER_URL=https://jackiepi.xyz npm run compare
   ```

To roll back, comment `routes` out again and `npm run deploy`. Requests go back to nginx, which still proxies to martin and FastAPI as long as the containers are running. Once the Worker has run without problems for a while, stop them on the VPS with `docker compose down`.

## Developing

`npm run dev` serves the Worker at http://localhost:8787 with a local R2 bucket (fill it with `./upload.sh --local`). `WORKER_URL=http://localhost:8787 npm run compare` checks it. `npm run typecheck` regenerates the binding types after changing `wrangler.jsonc`.
