/**
 * Serves the NCA Counties frontend from a static snapshot of the old Postgres/martin/FastAPI
 * backend, kept in R2 (see ../README.md):
 *
 *   GET /tiles/counties/{z}/{x}/{y}     county shapes as Mapbox vector tiles, read from
 *                                        tiles/counties.pmtiles; 204 where a tile is empty
 *   GET /api/climate-variables?gwl=2.0&var=tavg&relative=True
 *                                        the FastAPI response, saved per gwl, var and relative
 *
 * Responses go through the Cache API, so R2 is only read on a cache miss.
 */
import { EtagMismatch, PMTiles, ResolvedValueCache, type RangeResponse, type Source } from 'pmtiles';

const TILES_KEY = 'tiles/counties.pmtiles';
const GWLS = ['1.0', '2.0', '3.0', '4.0'];
// The variables the frontend can plot; scripts/snapshot_backend.sh saves one file for each
const CLIMATE_VARS = new Set([
	'tavg',
	'pr_above_nonzero_99th',
	'prmax1day',
	'prmax5yr',
	'tmax1day',
	'tmax_days_ge_100f',
	'tmax_days_ge_105f',
	'tmax_days_ge_95f',
	'tmean_jja',
	'tmin_days_ge_70f',
	'tmin_days_le_0f',
	'tmin_days_le_32f',
	'tmin_jja',
	'pr_annual',
	'pr_days_above_nonzero_99th',
]);
// What FastAPI (pydantic) accepts for a boolean query parameter
const TRUE_VALUES = new Set(['true', '1', 'yes', 'on', 't', 'y']);
const FALSE_VALUES = new Set(['false', '0', 'no', 'off', 'f', 'n']);

// The data is public and the same for everyone, so any origin may read it
const COMMON_HEADERS = {
	'Access-Control-Allow-Origin': '*',
	'Cache-Control': 'public, max-age=86400',
};

/** Reads byte ranges of a PMTiles archive from R2. */
class R2Source implements Source {
	constructor(
		private bucket: R2Bucket,
		private key: string,
	) {}

	getKey(): string {
		return this.key;
	}

	async getBytes(offset: number, length: number, _signal?: AbortSignal, etag?: string): Promise<RangeResponse> {
		const object = await this.bucket.get(this.key, {
			range: { offset, length },
			onlyIf: etag ? { etagMatches: etag } : undefined,
		});
		if (object === null) {
			throw new Error(`${this.key} is missing from the bucket`);
		}
		// An object without a body means onlyIf failed: the archive was replaced while this
		// isolate had parts of the old one cached. PMTiles clears its cache and retries.
		if (!('body' in object)) {
			throw new EtagMismatch(`${this.key} changed`);
		}
		return { data: await object.arrayBuffer(), etag: object.etag };
	}
}

// Kept per isolate: the archive's header and directories are cached between requests.
let archive: PMTiles | undefined;

function getArchive(env: Env): PMTiles {
	archive ??= new PMTiles(new R2Source(env.DATA, TILES_KEY), new ResolvedValueCache(25));
	return archive;
}

function errorResponse(status: number, detail: string): Response {
	return Response.json({ detail }, { status, headers: { 'Access-Control-Allow-Origin': '*' } });
}

async function tileResponse(env: Env, z: number, x: number, y: number): Promise<Response> {
	const pmtiles = getArchive(env);
	const tile = await pmtiles.getZxy(z, x, y);
	if (!tile) {
		return new Response(null, { status: 204, headers: COMMON_HEADERS });
	}
	// getZxy has already decompressed the tile, and Cloudflare compresses it on the way to the
	// browser. No Content-Encoding here: a gzipped response that goes through the Cache API
	// comes back out of it gzipped a second time.
	return new Response(tile.data, { headers: { ...COMMON_HEADERS, 'Content-Type': 'application/x-protobuf' } });
}

async function objectResponse(env: Env, key: string, contentType: string): Promise<Response> {
	const object = await env.DATA.get(key);
	if (object === null) {
		return errorResponse(404, 'No data found for the provided filters');
	}
	return new Response(object.body, { headers: { ...COMMON_HEADERS, 'Content-Type': contentType } });
}

/**
 * Works out which stored file answers a request. Returns the cache key (the same for
 * requests that differ only in how they spell a parameter, e.g. gwl=2 and gwl=2.0) and how
 * to build the response, or an error response.
 */
function route(url: URL, env: Env): { key: string; load: () => Promise<Response> } | Response {
	const tile = url.pathname.match(/^\/tiles\/counties\/(\d+)\/(\d+)\/(\d+)(?:\.(?:pbf|mvt))?$/);
	if (tile) {
		const [z, x, y] = tile.slice(1).map(Number);
		if (z > 26 || x >= 2 ** z || y >= 2 ** z) {
			return errorResponse(400, 'Tile coordinates are out of range');
		}
		return { key: `/tiles/counties/${z}/${x}/${y}`, load: () => tileResponse(env, z, x, y) };
	}

	if (url.pathname === '/api/climate-variables') {
		const params = url.searchParams;
		if (params.has('county_id')) {
			return errorResponse(400, 'county_id is not supported; filter the full response instead');
		}
		const gwlNumber = Number(params.get('gwl') ?? NaN);
		const gwl = Number.isFinite(gwlNumber) ? gwlNumber.toFixed(1) : '';
		if (!GWLS.includes(gwl)) {
			return errorResponse(400, `gwl must be one of ${GWLS.join(', ')}`);
		}
		const climateVar = params.get('var') ?? '';
		if (!CLIMATE_VARS.has(climateVar)) {
			return errorResponse(400, `var must be one of ${[...CLIMATE_VARS].join(', ')}`);
		}
		const relativeParam = (params.get('relative') ?? 'true').toLowerCase();
		if (!TRUE_VALUES.has(relativeParam) && !FALSE_VALUES.has(relativeParam)) {
			return errorResponse(400, 'relative must be True or False');
		}
		const kind = TRUE_VALUES.has(relativeParam) ? 'relative' : 'absolute';
		const key = `api/climate-variables/${gwl}/${kind}/${climateVar}.json`;
		return { key: `/${key}`, load: () => objectResponse(env, key, 'application/json') };
	}

	return errorResponse(404, 'Not Found');
}

export default {
	async fetch(request, env, ctx): Promise<Response> {
		if (request.method === 'OPTIONS') {
			return new Response(null, {
				status: 204,
				headers: {
					'Access-Control-Allow-Origin': '*',
					'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
					'Access-Control-Max-Age': '86400',
				},
			});
		}
		if (request.method !== 'GET' && request.method !== 'HEAD') {
			return errorResponse(405, 'Method Not Allowed');
		}

		const url = new URL(request.url);
		const target = route(url, env);
		if (target instanceof Response) {
			return target;
		}

		const cacheKey = new Request(new URL(target.key, url.origin));
		const cached = await caches.default.match(cacheKey);
		if (cached) {
			return cached;
		}
		const response = await target.load();
		if (response.status === 200) {
			ctx.waitUntil(caches.default.put(cacheKey, response.clone()));
		}
		return response;
	},
} satisfies ExportedHandler<Env>;
