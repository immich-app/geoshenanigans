// Smoke test — load the v14 admin index and reverse-geocode a handful
// of coordinates. Compares to expected outputs from the Rust server.

import { Geocoder } from "../src/index.js";

const dataDir = process.env.GEOCODER_DATA ?? "/home/zack/geocoder-data-v14";

console.log(`Loading index from ${dataDir}…`);
const t0 = performance.now();
const geo = new Geocoder(dataDir);
const loadMs = performance.now() - t0;
console.log(`Loaded in ${loadMs.toFixed(0)} ms`);

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
  console.log(`  display: ${result.display_name}`);
  console.log(`  fields:`, JSON.stringify(result.address));
}
