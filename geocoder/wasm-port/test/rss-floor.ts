// Measure the true minimum RSS achievable for the WASM port.
// Loads everything, drops JS-side refs, forces GC, prints RSS at
// load time and after a small warmup.  Avoids the long-loop callback
// churn that inflates JSC heap residency.

import { openSync, readFileSync, readSync, statSync } from "node:fs";
import { join } from "node:path";

import { Geocoder, set_js_read } from "../pkg/geocoder_wasm.js";

const dataDir = process.env.GEOCODER_DATA ?? "/home/zack/geocoder-data-v14";
const CHUNK_THRESHOLD = Number(process.env.CHUNK_THRESHOLD ?? 100 * 1024 * 1024);

const fdMap = new Map<number, number>();
let nextHandle = 1;
set_js_read((handle: number, off: number, len: number): Uint8Array => {
  const fd = fdMap.get(handle)!;
  const buf = new Uint8Array(len);
  readSync(fd, buf, 0, len, off);
  return buf;
});
function openChunked(path: string) {
  const fd = openSync(path, "r");
  const len = statSync(path).size;
  const handle = nextHandle++;
  fdMap.set(handle, fd);
  return { handle, len };
}

const MUST_BE_INLINE = new Set([
  "strings_core","strings_street","strings_addr","strings_postcode","strings_poi",
  "place_nodes","place_cells","place_entries",
  "postcode_centroids","postcode_centroid_cells","postcode_centroid_entries",
  "poi_records","poi_cells","poi_entries",
  "admin_parents",
  "postal_polygons","postal_vertices",
  "admin_cells","admin_entries","admin_polygons",
  // admin_vertices + poi_vertices now chunkable (refactored read paths)
]);
const allFiles = [
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
  "admin_vertices",
];

const buffers: Record<string, unknown> = {
  // admin_cells/entries/polygons must stay inline (admin_polygons is
  // 12 MiB and accessed by reference). admin_vertices is now
  // chunkable so it goes through the for-loop below.
  admin_cells: readFileSync(join(dataDir, "admin_cells.bin")),
  admin_entries: readFileSync(join(dataDir, "admin_entries.bin")),
  admin_polygons: readFileSync(join(dataDir, "admin_polygons.bin")),
  strings_layout: readFileSync(join(dataDir, "strings_layout.json"), "utf8"),
  poi_meta: (() => { try { return readFileSync(join(dataDir, "poi_meta.json"), "utf8"); } catch { return ""; } })(),
};

let inlineBytes = 0, chunkedBytes = 0;
for (const name of allFiles) {
  const path = join(dataDir, `${name}.bin`);
  let stat;
  try { stat = statSync(path); } catch { continue; }
  if (MUST_BE_INLINE.has(name) || stat.size <= CHUNK_THRESHOLD) {
    if (!buffers[name]) buffers[name] = readFileSync(path);
    inlineBytes += stat.size;
    if (stat.size > 50 * 1024 * 1024) console.log(`  inline: ${name} (${(stat.size / 1024 / 1024).toFixed(0)} MiB)`);
  } else {
    buffers[`${name}_chunked`] = openChunked(path);
    chunkedBytes += stat.size;
    if (stat.size > 50 * 1024 * 1024) console.log(`  chunked: ${name} (${(stat.size / 1024 / 1024).toFixed(0)} MiB)`);
  }
}

console.log(`Inline bytes:  ${(inlineBytes / 1024 / 1024).toFixed(0)} MiB`);
console.log(`Chunked bytes: ${(chunkedBytes / 1024 / 1024 / 1024).toFixed(2)} GiB`);
console.log(`RSS before construct: ${(process.memoryUsage.rss() / 1024 / 1024).toFixed(0)} MiB`);

const geo = new Geocoder(buffers);
console.log(`RSS after construct (refs still held): ${(process.memoryUsage.rss() / 1024 / 1024).toFixed(0)} MiB`);

for (const k of Object.keys(buffers)) delete buffers[k];
(globalThis as { Bun?: { gc: (sync: boolean) => void } }).Bun?.gc(true);
console.log(`RSS after drop+GC: ${(process.memoryUsage.rss() / 1024 / 1024).toFixed(0)} MiB`);

// Single warm query to force first chunk reads
geo.reverse(51.5074, -0.1278);
console.log(`RSS after 1 query: ${(process.memoryUsage.rss() / 1024 / 1024).toFixed(0)} MiB`);

// 100 distinct queries
const COORDS: Array<[number, number]> = [
  [51.5074,-0.1278],[40.7128,-74.006],[35.6762,139.6503],[48.8566,2.3522],
  [-33.8688,151.2093],[37.7749,-122.4194],[55.7558,37.6173],[-22.9068,-43.1729],
  [-1.2921,36.8219],[19.4326,-99.1332],
];
for (let i = 0; i < 100; i++) geo.reverse(...COORDS[i % COORDS.length]);
(globalThis as { Bun?: { gc: (sync: boolean) => void } }).Bun?.gc(true);
console.log(`RSS after 100 queries (post-GC): ${(process.memoryUsage.rss() / 1024 / 1024).toFixed(0)} MiB`);
