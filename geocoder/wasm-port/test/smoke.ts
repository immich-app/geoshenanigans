// WASM port smoke test — load the v14 planet index and reverse-geocode
// the same coordinate set as the TS and Rust ports for parity.
//
// Files >CHUNK_THRESHOLD bytes are passed as `key_chunked: { handle, len }`
// descriptors instead of Uint8Array.  The Rust side then calls back into
// JS via `set_js_read` for every page miss (64 KiB pages, 256 MiB LRU
// cache).  This is what lets us load the >4 GiB files (geo_cells,
// addr_points, addr_vertices, street_nodes) without blowing past the
// wasm32 4 GiB linear memory cap.

import { openSync, readFileSync, readSync, statSync } from "node:fs";
import { join } from "node:path";

import { Geocoder, set_js_read } from "../pkg/geocoder_wasm.js";

const dataDir = process.env.GEOCODER_DATA ?? "/home/zack/geocoder-data-v14";

// In-memory threshold.  Anything larger gets a chunked descriptor.  The
// total of all in-memory buffers must stay under wasm32's 4 GiB linear
// memory cap; 600 MiB per file keeps the planet load well below that.
const CHUNK_THRESHOLD = 600 * 1024 * 1024;

const fdMap = new Map<number, number>();
let nextHandle = 1;

set_js_read((handle: number, off: number, len: number): Uint8Array => {
  const fd = fdMap.get(handle);
  if (fd === undefined) throw new Error(`unknown chunked handle ${handle}`);
  const buf = new Uint8Array(len);
  readSync(fd, buf, 0, len, off);
  return buf;
});

function openChunked(path: string): { handle: number; len: number } {
  const fd = openSync(path, "r");
  const len = statSync(path).size;
  const handle = nextHandle++;
  fdMap.set(handle, fd);
  return { handle, len };
}

type LoadEntry = { inline: Buffer | null; chunked: { handle: number; len: number } | null };

function readEither(name: string): LoadEntry {
  let stat;
  try { stat = statSync(join(dataDir, name)); } catch { return { inline: null, chunked: null }; }
  if (stat.size > CHUNK_THRESHOLD) {
    return { inline: null, chunked: openChunked(join(dataDir, name)) };
  }
  return { inline: readFileSync(join(dataDir, name)), chunked: null };
}

console.log(`Loading WASM port from ${dataDir}…`);
const t0 = performance.now();

const fileNames = [
  "place_nodes", "place_cells", "place_entries",
  "postcode_centroids", "postcode_centroid_cells", "postcode_centroid_entries",
  "poi_records", "poi_vertices", "poi_cells", "poi_entries",
  "geo_cells", "street_ways", "street_nodes", "street_entries",
  "addr_points", "addr_vertices", "addr_entries",
  "interp_ways", "interp_nodes", "interp_entries",
  "way_postcodes", "addr_postcodes",
  "admin_parents", "way_parents",
  "postal_polygons", "postal_vertices",
];

const buffers: Record<string, unknown> = {
  admin_cells:    readFileSync(join(dataDir, "admin_cells.bin")),
  admin_entries:  readFileSync(join(dataDir, "admin_entries.bin")),
  admin_polygons: readFileSync(join(dataDir, "admin_polygons.bin")),
  admin_vertices: readFileSync(join(dataDir, "admin_vertices.bin")),
  strings_layout: readFileSync(join(dataDir, "strings_layout.json"), "utf8"),
  poi_meta: (() => { try { return readFileSync(join(dataDir, "poi_meta.json"), "utf8"); } catch { return ""; } })(),
};

for (const tier of ["strings_core", "strings_street", "strings_addr", "strings_postcode", "strings_poi"]) {
  const e = readEither(`${tier}.bin`);
  if (e.inline) buffers[tier] = e.inline;
  else if (e.chunked) buffers[`${tier}_chunked`] = e.chunked;
}

for (const name of fileNames) {
  const e = readEither(`${name}.bin`);
  if (e.inline) buffers[name] = e.inline;
  else if (e.chunked) {
    buffers[`${name}_chunked`] = e.chunked;
    console.log(`[wasm-port] ${name}: chunked (${(e.chunked.len / 2 ** 30).toFixed(2)} GiB)`);
  }
}

const geo = new Geocoder(buffers);
console.log(`Loaded in ${(performance.now() - t0).toFixed(0)} ms`);

const TESTS: Array<[string, number, number]> = [
  ["London",      51.5074, -0.1278],
  ["NYC",         40.7128, -74.0060],
  ["Tokyo",       35.6762, 139.6503],
  ["Paris",       48.8566,   2.3522],
  ["Sydney",     -33.8688, 151.2093],
  ["SF",          37.7749, -122.4194],
  ["Cala Bona",   39.61435,   3.38995],
  ["Silverstone", 52.07333,  -1.01192],
  ["Moscow",      55.7558,  37.6173],
  ["Rio",        -22.9068, -43.1729],
];

for (const [label, lat, lng] of TESTS) {
  const t = performance.now();
  const result = geo.reverse(lat, lng);
  const ms = performance.now() - t;
  console.log(`\n${label} (${lat}, ${lng})  —  ${ms.toFixed(2)} ms`);
  console.log(`  display: ${(result as { display_name?: string }).display_name}`);
  console.log(`  fields:`, JSON.stringify((result as { address: object }).address));
}
