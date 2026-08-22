// Three-way benchmark: WASM port (in-process), TS port (in-process),
// and the Rust HTTP server. Same v14 admin+place+postcode index for
// all three.

import { readFileSync } from "node:fs";
import { join } from "node:path";

import { Geocoder as WasmGeocoder } from "../pkg/geocoder_wasm.js";
import { Geocoder as TsGeocoder } from "../../ts-port/src/index.js";

const dataDir = process.env.GEOCODER_DATA ?? "/home/zack/geocoder-data-v14";
const rustUrl = process.env.RUST_URL ?? "http://localhost:3556";
const apiKey = process.env.API_KEY ?? "test";
const iterations = Number(process.env.ITERATIONS ?? 1000);

const COORDS: Array<[number, number]> = [
  [51.5074,  -0.1278], [40.7128, -74.0060], [35.6762, 139.6503],
  [48.8566,   2.3522], [-33.8688,151.2093], [37.7749,-122.4194],
  [39.61435,  3.38995],[52.07333, -1.01192],[55.7558, 37.6173],
  [-22.9068,-43.1729], [-1.2921, 36.8219], [19.4326, -99.1332],
  [13.7563,100.5018],  [25.2048, 55.2708], [-34.6037,-58.3816],
  [59.3293, 18.0686],  [1.3521, 103.8198], [52.5200, 13.4050],
  [41.9028, 12.4964],  [30.0444, 31.2357],
];

function pct(sorted: number[], p: number): number {
  return sorted[Math.floor((sorted.length - 1) * p)];
}

async function bench(name: string, fn: (lat: number, lng: number) => Promise<unknown> | unknown) {
  // Warm
  for (let i = 0; i < 100; i++) await fn(COORDS[i % COORDS.length][0], COORDS[i % COORDS.length][1]);

  const lats: number[] = [];
  const t0 = performance.now();
  for (let i = 0; i < iterations; i++) {
    const [lat, lng] = COORDS[i % COORDS.length];
    const t = performance.now();
    await fn(lat, lng);
    lats.push(performance.now() - t);
  }
  const total = performance.now() - t0;
  lats.sort((a, b) => a - b);
  const qps = (iterations / total) * 1000;

  console.log(`\n${name}`);
  console.log(`  Total:  ${total.toFixed(0)} ms over ${iterations} queries`);
  console.log(`  QPS:    ${qps.toFixed(0)}`);
  console.log(`  p50:    ${pct(lats, 0.50).toFixed(3)} ms`);
  console.log(`  p95:    ${pct(lats, 0.95).toFixed(3)} ms`);
  console.log(`  p99:    ${pct(lats, 0.99).toFixed(3)} ms`);
  console.log(`  max:    ${lats[lats.length - 1].toFixed(3)} ms`);
}

function readOptional(name: string): Buffer | null {
  try { return readFileSync(join(dataDir, name)); } catch { return null; }
}

console.log(`Loading TS port…`);
const tT = performance.now();
const tsGeo = new TsGeocoder(dataDir);
console.log(`  TS loaded in ${(performance.now() - tT).toFixed(0)} ms`);

console.log(`Loading WASM port…`);
const tW = performance.now();
const wasmGeo = new WasmGeocoder({
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
});
console.log(`  WASM loaded in ${(performance.now() - tW).toFixed(0)} ms`);

await bench("WASM port (in-process)", (lat, lng) => wasmGeo.reverse(lat, lng));
await bench("TS port (in-process)", (lat, lng) => tsGeo.reverse(lat, lng));
await bench(`Rust HTTP @ ${rustUrl}`, async (lat, lng) => {
  const r = await fetch(`${rustUrl}/reverse?lat=${lat}&lon=${lng}&key=${apiKey}`);
  await r.json();
});

console.log("\nProcess RSS:", `${(process.memoryUsage.rss() / 1024 / 1024).toFixed(0)} MiB`);
