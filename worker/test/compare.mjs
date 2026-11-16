// Checks a running nca-counties Worker against the snapshot and, if BASELINE_URL is set,
// against the old martin/FastAPI backend.
//
//   WORKER_URL=http://localhost:8787 npm run compare
//   WORKER_URL=https://nca-counties.<account>.workers.dev BASELINE_URL=https://jackiepi.xyz npm run compare
//
// API: every response must match its snapshot file byte for byte. Tiles: sample tiles at
// every zoom must hold the same counties, with the same properties, as martin's. Exits 1 on
// any failure.
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { VectorTile } from '@mapbox/vector-tile';
import { PbfReader } from 'pbf';

const WORKER_URL = process.env.WORKER_URL?.replace(/\/$/, '');
const BASELINE_URL = process.env.BASELINE_URL?.replace(/\/$/, '');
const SNAPSHOT = process.env.SNAPSHOT ?? fileURLToPath(new URL('../../snapshot', import.meta.url));
if (!WORKER_URL) {
	console.error('Set WORKER_URL to the Worker to check, e.g. http://localhost:8787');
	process.exit(2);
}

const GWLS = ['1.0', '2.0', '3.0', '4.0'];
const VARS = [
	'tavg', 'pr_above_nonzero_99th', 'prmax1day', 'prmax5yr', 'tmax1day', 'tmax_days_ge_100f',
	'tmax_days_ge_105f', 'tmax_days_ge_95f', 'tmean_jja', 'tmin_days_ge_70f', 'tmin_days_le_0f',
	'tmin_days_le_32f', 'tmin_jja', 'pr_annual', 'pr_days_above_nonzero_99th',
];
// Places with awkward shapes: many tiny independent cities (Richmond), small New England
// counties, Alaska and the Aleutians near the antimeridian, Hawaii, and the middle of the US
const PLACES = {
	kansas: [-98.5, 39.0],
	richmond: [-77.43, 37.54],
	boston: [-71.06, 42.36],
	anchorage: [-149.9, 61.22],
	aleutians: [-176.6, 51.88],
	honolulu: [-157.86, 21.31],
};

let failures = 0;
function fail(message) {
	failures++;
	console.log(`  FAIL ${message}`);
}

function tileAt([lon, lat], z) {
	const n = 2 ** z;
	const x = Math.floor(((lon + 180) / 360) * n);
	const rad = (lat * Math.PI) / 180;
	const y = Math.floor(((1 - Math.log(Math.tan(rad) + 1 / Math.cos(rad)) / Math.PI) / 2) * n);
	return [z, x, y];
}

// Counties in a tile, as id -> "name|state_abbr"
async function tileCounties(base, [z, x, y]) {
	const response = await fetch(`${base}/tiles/counties/${z}/${x}/${y}`);
	if (response.status === 204) return { status: 204, counties: new Map() };
	if (response.status !== 200) return { status: response.status, counties: new Map() };
	const tile = new VectorTile(new PbfReader(new Uint8Array(await response.arrayBuffer())));
	const counties = new Map();
	for (const layer of Object.values(tile.layers)) {
		for (let i = 0; i < layer.length; i++) {
			const { id, name, state_abbr } = layer.feature(i).properties;
			counties.set(id, `${name}|${state_abbr}`);
		}
	}
	return { status: 200, counties, headers: response.headers };
}

async function checkApi() {
	console.log('API');
	let matched = 0;
	for (const gwl of GWLS) {
		for (const relative of ['True', 'False']) {
			const kind = relative === 'True' ? 'relative' : 'absolute';
			for (const v of VARS) {
				const query = `gwl=${gwl}&var=${v}&relative=${relative}`;
				const response = await fetch(`${WORKER_URL}/api/climate-variables?${query}`);
				const body = Buffer.from(await response.arrayBuffer());
				const expected = await readFile(`${SNAPSHOT}/api/climate-variables/${gwl}/${kind}/${v}.json`);
				if (response.status !== 200) fail(`${query}: status ${response.status}`);
				else if (!body.equals(expected)) fail(`${query}: body differs from the snapshot`);
				else if (!response.headers.get('content-type')?.startsWith('application/json')) fail(`${query}: content-type ${response.headers.get('content-type')}`);
				else matched++;
			}
		}
	}
	console.log(`  ${matched}/${GWLS.length * VARS.length * 2} responses match the snapshot`);

	// Other spellings of the same request, as the frontend sends them (gwl=2 from the slider)
	const aliases = [
		['gwl=2&var=tavg&relative=True', '2.0/relative/tavg'],
		['gwl=3&var=tavg&relative=false', '3.0/absolute/tavg'],
		['gwl=1.0&var=pr_annual', '1.0/relative/pr_annual'],
	];
	for (const [query, file] of aliases) {
		const body = Buffer.from(await (await fetch(`${WORKER_URL}/api/climate-variables?${query}`)).arrayBuffer());
		if (!body.equals(await readFile(`${SNAPSHOT}/api/climate-variables/${file}.json`))) fail(`${query} does not return ${file}.json`);
	}
	for (const query of ['gwl=5&var=tavg', 'gwl=2.0&var=nope', 'var=tavg', 'gwl=2.0&var=tavg&relative=maybe']) {
		const { status } = await fetch(`${WORKER_URL}/api/climate-variables?${query}`);
		if (status !== 400) fail(`${query}: status ${status}, expected 400`);
	}

	if (BASELINE_URL) {
		let same = 0;
		const sample = [['1.0', 'True', 'tavg'], ['2.0', 'False', 'pr_annual'], ['4.0', 'False', 'tmin_days_le_32f'], ['3.0', 'True', 'prmax5yr']];
		for (const [gwl, relative, v] of sample) {
			const query = `gwl=${gwl}&var=${v}&relative=${relative}`;
			const [a, b] = await Promise.all(
				[WORKER_URL, BASELINE_URL].map(async (base) => Buffer.from(await (await fetch(`${base}/api/climate-variables?${query}`)).arrayBuffer())),
			);
			if (a.equals(b)) same++;
			else fail(`${query}: differs from ${BASELINE_URL}`);
		}
		console.log(`  ${same}/${sample.length} sampled responses match ${BASELINE_URL}`);
	}
}

async function checkTiles() {
	console.log('Tiles');
	let checked = 0;
	for (let z = 0; z <= 10; z++) {
		const tiles = new Map(Object.entries(PLACES).map(([name, place]) => [tileAt(place, z).join('/'), name]));
		for (const [key, name] of tiles) {
			const coords = key.split('/').map(Number);
			const worker = await tileCounties(WORKER_URL, coords);
			if (worker.status !== 200 || worker.counties.size === 0) {
				fail(`${key} (${name}): status ${worker.status}, ${worker.counties.size} counties`);
				continue;
			}
			if (worker.headers.get('access-control-allow-origin') !== '*') fail(`${key}: no CORS header`);
			checked++;
			// Below z5 (the frontend's starting zoom) martin builds a tile from nearly every
			// county, more than the 1 GB VPS can safely spare, so those aren't compared
			if (!BASELINE_URL || z < 5) continue;

			const baseline = await tileCounties(BASELINE_URL, coords);
			const missing = [...baseline.counties.keys()].filter((id) => !worker.counties.has(id));
			const extra = [...worker.counties.keys()].filter((id) => !baseline.counties.has(id));
			const renamed = [...worker.counties].filter(([id, label]) => baseline.counties.has(id) && baseline.counties.get(id) !== label);
			if (missing.length || renamed.length) {
				fail(`${key} (${name}): ${missing.length} of martin's ${baseline.counties.size} counties missing [${missing.slice(0, 8).map((id) => baseline.counties.get(id)).join(', ')}], ${renamed.length} with different properties`);
			}
			if (extra.length) {
				// tippecanoe's tile buffer is a little wider than martin's, so a few extra
				// counties along tile edges are expected and harmless
				console.log(`  note ${key} (${name}): ${extra.length} counties martin does not include`);
			}
		}
	}
	console.log(`  ${checked} sample tiles served${BASELINE_URL ? `, those at z5 and up compared with ${BASELINE_URL}` : ''}`);

	// Past the archive's maximum zoom and over open ocean there is no tile
	for (const coords of [tileAt(PLACES.kansas, 11), tileAt([-140, 30], 6)]) {
		const { status } = await tileCounties(WORKER_URL, coords);
		if (status !== 204) fail(`${coords.join('/')}: status ${status}, expected 204`);
	}
}

await checkApi();
await checkTiles();
console.log(failures ? `${failures} failures` : 'All checks passed');
process.exit(failures ? 1 : 0);
