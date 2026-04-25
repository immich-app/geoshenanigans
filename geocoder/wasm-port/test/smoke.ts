// WASM port smoke test — load the v14 admin index and reverse-geocode
// the same coordinate set as the TS and Rust ports for parity.

import { readFileSync } from "node:fs";
import { join } from "node:path";

import { Geocoder } from "../pkg/geocoder_wasm.js";

const dataDir = process.env.GEOCODER_DATA ?? "/home/zack/geocoder-data-v14";

function readOptional(name: string): Buffer | null {
  try { return readFileSync(join(dataDir, name)); } catch { return null; }
}

console.log(`Loading WASM port from ${dataDir}…`);
const t0 = performance.now();

const buffers = {
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
};

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
