// Benchmark TS port vs. Rust HTTP server. Both load the same v14 admin
// data, so the algorithmic work per query is comparable. Reports cold
// start, warm throughput, and latency percentiles.

import { Geocoder } from "../src/index.js";

const dataDir = process.env.GEOCODER_DATA ?? "/home/zack/geocoder-data-v14";
const rustUrl = process.env.RUST_URL ?? "http://localhost:3556";
const apiKey = process.env.API_KEY ?? "test";
const iterations = Number(process.env.ITERATIONS ?? 1000);

// Diverse coordinate set — repeats during the benchmark so cache effects
// (CPU, page cache) are realistic.
const COORDS: Array<[number, number]> = [
  [51.5074,  -0.1278],   // London
  [40.7128, -74.0060],   // NYC
  [35.6762, 139.6503],   // Tokyo
  [48.8566,   2.3522],   // Paris
  [-33.8688,151.2093],   // Sydney
  [37.7749,-122.4194],   // SF
  [39.61435,  3.38995],  // Cala Bona
  [52.07333, -1.01192],  // Silverstone
  [55.7558, 37.6173],    // Moscow
  [-22.9068,-43.1729],   // Rio
  [-1.2921, 36.8219],    // Nairobi
  [19.4326, -99.1332],   // Mexico City
  [13.7563,100.5018],    // Bangkok
  [25.2048, 55.2708],    // Dubai
  [-34.6037,-58.3816],   // Buenos Aires
  [59.3293, 18.0686],    // Stockholm
  [1.3521, 103.8198],    // Singapore
  [52.5200, 13.4050],    // Berlin
  [41.9028, 12.4964],    // Rome
  [30.0444, 31.2357],    // Cairo
];

function percentile(sorted: number[], p: number): number {
  const idx = Math.floor((sorted.length - 1) * p);
  return sorted[idx];
}

async function bench(name: string, runOne: (lat: number, lng: number) => Promise<unknown> | unknown) {
  // Warm
  for (let i = 0; i < 100; i++) {
    await runOne(COORDS[i % COORDS.length][0], COORDS[i % COORDS.length][1]);
  }

  const latencies: number[] = [];
  const t0 = performance.now();
  for (let i = 0; i < iterations; i++) {
    const [lat, lng] = COORDS[i % COORDS.length];
    const t = performance.now();
    await runOne(lat, lng);
    latencies.push(performance.now() - t);
  }
  const totalMs = performance.now() - t0;
  latencies.sort((a, b) => a - b);
  const qps = (iterations / totalMs) * 1000;

  console.log(`\n${name}`);
  console.log(`  Total: ${totalMs.toFixed(0)} ms over ${iterations} queries`);
  console.log(`  QPS:   ${qps.toFixed(0)}`);
  console.log(`  p50:   ${percentile(latencies, 0.50).toFixed(3)} ms`);
  console.log(`  p95:   ${percentile(latencies, 0.95).toFixed(3)} ms`);
  console.log(`  p99:   ${percentile(latencies, 0.99).toFixed(3)} ms`);
  console.log(`  max:   ${latencies[latencies.length - 1].toFixed(3)} ms`);
}

console.log(`Loading TS port from ${dataDir}…`);
const t0 = performance.now();
const geo = new Geocoder(dataDir);
const tsLoadMs = performance.now() - t0;
console.log(`TS port loaded in ${tsLoadMs.toFixed(0)} ms`);

await bench("TS port (in-process call)", (lat, lng) => geo.reverse(lat, lng));

await bench(`Rust server (HTTP @ ${rustUrl})`, async (lat, lng) => {
  const r = await fetch(`${rustUrl}/reverse?lat=${lat}&lon=${lng}&key=${apiKey}`);
  await r.json();
});

console.log("\nProcess RSS:", `${(process.memoryUsage.rss() / 1024 / 1024).toFixed(0)} MiB`);
