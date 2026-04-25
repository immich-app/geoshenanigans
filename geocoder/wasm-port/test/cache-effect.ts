// Cache-effect benchmark.  Measures latency curves for three workload
// shapes:
//
//   - REPEAT: identical lat/lng N times → tests "same coord" effects
//     (data already in CPU cache, JIT primed, branch predictor warm).
//   - NEARBY: tiny perturbations around the same point → tests "same
//     S2 cell" effects (page cache hot, but each query is technically
//     different work).
//   - FAR:    widely-spaced lat/lng in a long rotation → tests cold
//     working set (different cells each time, page cache thrashes).
//
// Reports per-iteration latency for each shape so we can see warmup
// curves and steady-state ratios.

import { readFileSync } from "node:fs";
import { join } from "node:path";

import { Geocoder as WasmGeocoder } from "../pkg/geocoder_wasm.js";
import { Geocoder as TsGeocoder } from "../../ts-port/src/index.js";

const dataDir = process.env.GEOCODER_DATA ?? "/home/zack/geocoder-data-v14";
const rustUrl = process.env.RUST_URL ?? "http://localhost:3556";
const apiKey = process.env.API_KEY ?? "test";
const N = Number(process.env.N ?? 200);

// wasm32 + JS Uint8Array share a 4 GiB ceiling; cap per-file size to
// keep the wasm constructor from OOM'ing on the planet build.
const SKIP_OVER_BYTES = 600 * 1024 * 1024;
function readOptional(name: string): Buffer | null {
  try {
    const path = join(dataDir, name);
    const stat = require("node:fs").statSync(path);
    if (stat.size > SKIP_OVER_BYTES) return null;
    return readFileSync(path);
  } catch { return null; }
}

const wasmBuffers = {
  admin_cells:    readFileSync(join(dataDir, "admin_cells.bin")),
  admin_entries:  readFileSync(join(dataDir, "admin_entries.bin")),
  admin_polygons: readFileSync(join(dataDir, "admin_polygons.bin")),
  admin_vertices: readFileSync(join(dataDir, "admin_vertices.bin")),
  strings_core:        readOptional("strings_core.bin"),
  strings_street:      readOptional("strings_street.bin"),
  strings_addr:        readOptional("strings_addr.bin"),
  strings_postcode:    readOptional("strings_postcode.bin"),
  strings_poi:         readOptional("strings_poi.bin"),
  strings_layout:      readFileSync(join(dataDir, "strings_layout.json"), "utf8"),
  place_nodes:                  readOptional("place_nodes.bin"),
  place_cells:                  readOptional("place_cells.bin"),
  place_entries:                readOptional("place_entries.bin"),
  postcode_centroids:           readOptional("postcode_centroids.bin"),
  postcode_centroid_cells:      readOptional("postcode_centroid_cells.bin"),
  postcode_centroid_entries:    readOptional("postcode_centroid_entries.bin"),
  poi_records:    readOptional("poi_records.bin"),
  poi_vertices:   readOptional("poi_vertices.bin"),
  poi_cells:      readOptional("poi_cells.bin"),
  poi_entries:    readOptional("poi_entries.bin"),
  poi_meta:       (() => { try { return readFileSync(join(dataDir, "poi_meta.json"), "utf8"); } catch { return ""; } })(),
  geo_cells:      readOptional("geo_cells.bin"),
  street_ways:    readOptional("street_ways.bin"),
  street_nodes:   readOptional("street_nodes.bin"),
  street_entries: readOptional("street_entries.bin"),
  addr_points:    readOptional("addr_points.bin"),
  addr_vertices:  readOptional("addr_vertices.bin"),
  addr_entries:   readOptional("addr_entries.bin"),
  interp_ways:    readOptional("interp_ways.bin"),
  interp_nodes:   readOptional("interp_nodes.bin"),
  interp_entries: readOptional("interp_entries.bin"),
  way_postcodes:  readOptional("way_postcodes.bin"),
  addr_postcodes: readOptional("addr_postcodes.bin"),
};

const tsGeo = new TsGeocoder(dataDir);
const wasmGeo = new WasmGeocoder(wasmBuffers);

// Spread of test coords for FAR scenario.
const FAR_COORDS: Array<[number, number]> = [
  [51.5074,  -0.1278], [40.7128, -74.0060], [35.6762, 139.6503],
  [48.8566,   2.3522], [-33.8688,151.2093], [37.7749,-122.4194],
  [55.7558, 37.6173], [-22.9068,-43.1729], [-1.2921, 36.8219],
  [19.4326, -99.1332], [13.7563,100.5018], [25.2048, 55.2708],
  [-34.6037,-58.3816], [59.3293, 18.0686], [1.3521, 103.8198],
  [52.5200, 13.4050], [41.9028, 12.4964], [30.0444, 31.2357],
  [39.61435,  3.38995],[52.07333, -1.01192],
];

const REPEAT_LAT = 51.5074, REPEAT_LNG = -0.1278;

interface Bench {
  name: string;
  run: (lat: number, lng: number) => Promise<unknown> | unknown;
}

const benches: Bench[] = [
  { name: "WASM",     run: (lat, lng) => wasmGeo.reverse(lat, lng) },
  { name: "TS",       run: (lat, lng) => tsGeo.reverse(lat, lng) },
  { name: "RustHTTP", run: async (lat, lng) => {
      const r = await fetch(`${rustUrl}/reverse?lat=${lat}&lon=${lng}&key=${apiKey}`);
      await r.json();
    } },
];

function pct(arr: number[], p: number): number {
  const s = [...arr].sort((a, b) => a - b);
  return s[Math.floor((s.length - 1) * p)];
}

async function runScenario(label: string, coordFn: (i: number) => [number, number]) {
  console.log(`\n=== ${label} (${N} iterations) ===`);
  for (const b of benches) {
    // small warmup so JIT compiles the call site
    for (let i = 0; i < 20; i++) await b.run(REPEAT_LAT, REPEAT_LNG);
    const lats: number[] = new Array(N);
    for (let i = 0; i < N; i++) {
      const [lat, lng] = coordFn(i);
      const t = performance.now();
      await b.run(lat, lng);
      lats[i] = performance.now() - t;
    }
    const first = lats[0];
    const next4 = (lats[1] + lats[2] + lats[3] + lats[4]) / 4;
    const tail = lats.slice(20);
    const tailMean = tail.reduce((a, x) => a + x, 0) / tail.length;
    console.log(`${b.name.padEnd(8)}  1st=${first.toFixed(3)}ms  next4=${next4.toFixed(3)}ms  ` +
      `tail-mean=${tailMean.toFixed(3)}ms  p50=${pct(tail, 0.5).toFixed(3)}  p99=${pct(tail, 0.99).toFixed(3)}`);
  }
}

await runScenario("REPEAT (same coord)", () => [REPEAT_LAT, REPEAT_LNG]);
await runScenario("NEARBY (jitter ±0.0001°, same S2 cell)", (i) => [
  REPEAT_LAT + (((i * 16807) % 1000) - 500) * 1e-7,
  REPEAT_LNG + (((i * 48271) % 1000) - 500) * 1e-7,
]);
await runScenario("FAR (cycling 20 distant coords)", (i) => FAR_COORDS[i % FAR_COORDS.length]);

console.log(`\nProcess RSS: ${(process.memoryUsage.rss() / 1024 / 1024).toFixed(0)} MiB`);
