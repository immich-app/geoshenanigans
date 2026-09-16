// Per-implementation throughput driver.  Runs a single port in
// isolation so the reported RSS reflects only that port's working set.
//
// Configs (set via TARGET env): wasm | ts | rust
// Other knobs:
//   N=<int>             requests per scenario (default 5000)
//   GEOCODER_DATA=<path>
//   RUST_URL=<url>
//   API_KEY=<key>
//
// Output: per-scenario throughput (req/s) + p50/p99 + final RSS.

import { openSync, readFileSync, readSync, statSync } from "node:fs";
import { join } from "node:path";

const target = process.env.TARGET ?? "wasm"; // wasm | ts | rust
const N = Number(process.env.N ?? 5000);
const dataDir = process.env.GEOCODER_DATA ?? "/home/zack/geocoder-data-v14";
const rustUrl = process.env.RUST_URL ?? "http://localhost:3556";
const apiKey = process.env.API_KEY ?? "test";

// 20 distinct coords cycled — same set as cache-effect.ts FAR scenario
// so results are comparable.
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

// ---- WASM loader (chunked-on-demand) ----
// Files above this size load via chunked-on-demand instead of being
// copied into wasm linear memory.  Lower threshold = lower wasm
// linear memory floor, but more JS↔wasm callbacks per query.
const CHUNK_THRESHOLD = Number(process.env.CHUNK_THRESHOLD ?? 100 * 1024 * 1024);
const fdMap = new Map<number, number>();
let nextHandle = 1;
function openChunked(path: string) {
  const fd = openSync(path, "r");
  const len = statSync(path).size;
  const handle = nextHandle++;
  fdMap.set(handle, fd);
  return { handle, len };
}
function readEither(name: string) {
  let stat;
  try { stat = statSync(join(dataDir, name)); } catch { return null; }
  if (stat.size > CHUNK_THRESHOLD) return { chunked: openChunked(join(dataDir, name)) };
  return { inline: readFileSync(join(dataDir, name)) };
}

let runner: (lat: number, lng: number) => Promise<unknown> | unknown;
let loadMs = 0;

if (target === "wasm") {
  const { Geocoder, set_js_read } = await import("../pkg/geocoder_wasm.js");
  // Reusable read buffer — avoids per-callback Uint8Array allocations.
  const READ_BUF = new Uint8Array(128 * 1024);
  set_js_read((handle: number, off: number, len: number): Uint8Array => {
    const fd = fdMap.get(handle)!;
    if (len > READ_BUF.length) {
      const big = new Uint8Array(len);
      readSync(fd, big, 0, len, off);
      return big;
    }
    const view = new Uint8Array(READ_BUF.buffer, 0, len);
    readSync(fd, view, 0, len, off);
    return view;
  });
  const t0 = performance.now();
  const buffers: Record<string, unknown> = {
    strings_layout: readFileSync(join(dataDir, "strings_layout.json"), "utf8"),
    poi_meta: (() => { try { return readFileSync(join(dataDir, "poi_meta.json"), "utf8"); } catch { return ""; } })(),
  };
  // Every binary file is now chunk-aware — no MUST_BE_INLINE list.
  // CHUNK_THRESHOLD picks per-file inline-vs-chunked: 0 chunks
  // everything (lowest RSS), large threshold inlines small files
  // (lower latency for hot lookups).
  const allFiles = [
    "admin_cells","admin_entries","admin_polygons","admin_vertices",
    "strings_core","strings_street","strings_addr","strings_postcode","strings_poi",
    "place_nodes","place_cells","place_entries",
    "postcode_centroids","postcode_centroid_cells","postcode_centroid_entries",
    "poi_records","poi_vertices","poi_cells","poi_entries",
    "geo_cells","street_ways","street_nodes","street_entries",
    "addr_points","addr_vertices","addr_entries",
    "interp_ways","interp_nodes","interp_entries",
    "way_postcodes","addr_postcodes",
    "admin_parents","way_parents",
    "postal_polygons","postal_vertices",
  ];
  for (const name of allFiles) {
    const path = join(dataDir, `${name}.bin`);
    let stat;
    try { stat = statSync(path); } catch { continue; }
    if (stat.size <= CHUNK_THRESHOLD) {
      buffers[name] = readFileSync(path);
    } else {
      buffers[`${name}_chunked`] = openChunked(path);
    }
  }
  const geo = new Geocoder(buffers);
  // Drop JS-side Uint8Array references — the wasm constructor has
  // already copied the bytes into linear memory.  Without this, the
  // JS heap holds ~2.5 GiB of duplicate data on top of the wasm copy.
  for (const k of Object.keys(buffers)) delete (buffers as Record<string, unknown>)[k];
  // Bun.gc(true) is synchronous; node-style globalThis.gc() exists if --expose-gc.
  (globalThis as { Bun?: { gc: (sync: boolean) => void } }).Bun?.gc(true);
  loadMs = performance.now() - t0;
  runner = (lat, lng) => geo.reverse(lat, lng);
} else if (target === "ts") {
  const { Geocoder } = await import("../../ts-port/src/index.js");
  const t0 = performance.now();
  const geo = new Geocoder(dataDir);
  loadMs = performance.now() - t0;
  runner = (lat, lng) => geo.reverse(lat, lng);
} else if (target === "rust") {
  loadMs = 0;
  runner = async (lat, lng) => {
    const r = await fetch(`${rustUrl}/reverse?lat=${lat}&lon=${lng}&key=${apiKey}`);
    await r.json();
  };
} else {
  throw new Error(`unknown TARGET=${target}`);
}

console.log(`target=${target} N=${N} load=${loadMs.toFixed(0)}ms`);

function pct(arr: number[], p: number): number {
  const s = [...arr].sort((a, b) => a - b);
  return s[Math.floor((s.length - 1) * p)];
}

async function scenario(label: string, coordFn: (i: number) => [number, number]) {
  // warmup
  for (let i = 0; i < 50; i++) await runner(REPEAT_LAT, REPEAT_LNG);
  const lats = new Array<number>(N);
  const startWall = performance.now();
  for (let i = 0; i < N; i++) {
    const [lat, lng] = coordFn(i);
    const t = performance.now();
    await runner(lat, lng);
    lats[i] = performance.now() - t;
  }
  const elapsed = (performance.now() - startWall) / 1000;
  const rps = N / elapsed;
  const tail = lats.slice(50);
  const tailMean = tail.reduce((a, x) => a + x, 0) / tail.length;
  console.log(
    `${label.padEnd(8)}  ${rps.toFixed(0).padStart(7)} req/s  ` +
    `tail-mean=${tailMean.toFixed(3).padStart(7)}ms  ` +
    `p50=${pct(tail, 0.5).toFixed(3)}  p99=${pct(tail, 0.99).toFixed(3)}`
  );
}

await scenario("REPEAT", () => [REPEAT_LAT, REPEAT_LNG]);
await scenario("NEARBY", (i) => [
  REPEAT_LAT + (((i * 16807) % 1000) - 500) * 1e-7,
  REPEAT_LNG + (((i * 48271) % 1000) - 500) * 1e-7,
]);
await scenario("FAR", (i) => FAR_COORDS[i % FAR_COORDS.length]);

const rss = process.memoryUsage.rss();
console.log(`\nProcess RSS: ${(rss / 1024 / 1024).toFixed(0)} MiB`);
