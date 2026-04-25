// Index loader — reads the admin-tier files into Buffer slabs that
// stay alive for the lifetime of the Geocoder instance.  Reads are
// blocking (fs.readFileSync) since this is once-per-startup.  Buffers
// are then handed off to the various readers and the StringPool.

import { readFileSync, existsSync, statSync } from "node:fs";
import { join } from "node:path";

import { StringPool } from "./string-pool.js";
import { PoiMeta } from "./poi-meta.js";
import { STR_TIER_FILENAMES } from "./types.js";

// readFileSync reads into the Node heap and ENOMEMs on multi-gigabyte
// files (geo_cells, addr_vertices, etc.).  Use Bun.mmap when available
// to expose the file without copying. Falls back to readFileSync for
// smaller files where the cost is negligible.
//
// JS Uint8Array max is 2^32-1 bytes (~4 GiB).  Files larger than that
// cannot be addressed as a single Buffer in any current JS runtime.
// We skip them with a warning — the geocoder degrades to whatever
// tiers the smaller files can support (e.g. planet `geo_cells.bin` is
// 7.7 GiB, so street/addr primary feature is unavailable on planet
// from the TS port; continent extracts are <4 GiB and work fine).
const MMAP_THRESHOLD = 256 * 1024 * 1024; // 256 MiB
const MAX_BUFFER_SIZE = 0xFFFFFFFF; // 4 GiB - 1
declare const Bun: { mmap?: (path: string) => Uint8Array } | undefined;

function loadFile(path: string, required: boolean): Buffer | null {
  if (!existsSync(path)) {
    if (required) throw new Error(`Required file missing: ${path}`);
    return null;
  }
  const size = statSync(path).size;
  if (size > MAX_BUFFER_SIZE) {
    console.warn(`[ts-port] skipping ${path}: ${(size / 2 ** 30).toFixed(1)} GiB exceeds JS 4 GiB Buffer limit`);
    if (required) throw new Error(`Required file ${path} exceeds 4 GiB; cannot load in JS runtime`);
    return null;
  }
  if (size > MMAP_THRESHOLD && typeof Bun !== "undefined" && Bun?.mmap) {
    const u8 = Bun.mmap(path);
    return Buffer.from(u8.buffer, u8.byteOffset, u8.byteLength);
  }
  return readFileSync(path);
}

export interface LoadedIndex {
  adminCells: Buffer;
  adminEntries: Buffer;
  adminPolygons: Buffer;
  adminVertices: Buffer;
  placeNodes: Buffer | null;
  placeCells: Buffer | null;
  placeEntries: Buffer | null;
  postcodeCentroids: Buffer | null;
  postcodeCells: Buffer | null;
  postcodeEntries: Buffer | null;
  poiRecords: Buffer | null;
  poiVertices: Buffer | null;
  poiCells: Buffer | null;
  poiEntries: Buffer | null;
  poiMeta: PoiMeta;
  // streets / addrs / interpolation / parent chains
  geoCells: Buffer | null;
  streetWays: Buffer | null;
  streetNodes: Buffer | null;
  streetEntries: Buffer | null;
  addrPoints: Buffer | null;
  addrVertices: Buffer | null;
  addrEntries: Buffer | null;
  interpWays: Buffer | null;
  interpNodes: Buffer | null;
  interpEntries: Buffer | null;
  wayParents: Buffer | null;
  adminParents: Buffer | null;
  wayPostcodes: Buffer | null;
  addrPostcodes: Buffer | null;
  strings: StringPool;
}

function loadOptional(path: string): Buffer | null {
  return loadFile(path, false);
}

export function loadIndex(dir: string): LoadedIndex {
  // Required (admin tier)
  const adminCells = loadFile(join(dir, "admin_cells.bin"), true)!;
  const adminEntries = loadFile(join(dir, "admin_entries.bin"), true)!;
  const adminPolygons = loadFile(join(dir, "admin_polygons.bin"), true)!;
  const adminVertices = loadFile(join(dir, "admin_vertices.bin"), true)!;

  // Optional — present for full-quality deployments, absent for minimal admin.
  const placeNodes = loadOptional(join(dir, "place_nodes.bin"));
  const placeCells = loadOptional(join(dir, "place_cells.bin"));
  const placeEntries = loadOptional(join(dir, "place_entries.bin"));
  const postcodeCentroids = loadOptional(join(dir, "postcode_centroids.bin"));
  const postcodeCells = loadOptional(join(dir, "postcode_centroid_cells.bin"));
  const postcodeEntries = loadOptional(join(dir, "postcode_centroid_entries.bin"));
  const poiRecords = loadOptional(join(dir, "poi_records.bin"));
  const poiVertices = loadOptional(join(dir, "poi_vertices.bin"));
  const poiCells = loadOptional(join(dir, "poi_cells.bin"));
  const poiEntries = loadOptional(join(dir, "poi_entries.bin"));
  const poiMeta = PoiMeta.load(dir);
  const geoCells = loadOptional(join(dir, "geo_cells.bin"));
  const streetWays = loadOptional(join(dir, "street_ways.bin"));
  const streetNodes = loadOptional(join(dir, "street_nodes.bin"));
  const streetEntries = loadOptional(join(dir, "street_entries.bin"));
  const addrPoints = loadOptional(join(dir, "addr_points.bin"));
  const addrVertices = loadOptional(join(dir, "addr_vertices.bin"));
  const addrEntries = loadOptional(join(dir, "addr_entries.bin"));
  const interpWays = loadOptional(join(dir, "interp_ways.bin"));
  const interpNodes = loadOptional(join(dir, "interp_nodes.bin"));
  const interpEntries = loadOptional(join(dir, "interp_entries.bin"));
  const wayParents = loadOptional(join(dir, "way_parents.bin"));
  const adminParents = loadOptional(join(dir, "admin_parents.bin"));
  const wayPostcodes = loadOptional(join(dir, "way_postcodes.bin"));
  const addrPostcodes = loadOptional(join(dir, "addr_postcodes.bin"));

  // String pool — load each tier file that exists, parse layout.
  const layoutPath = join(dir, "strings_layout.json");
  if (!existsSync(layoutPath)) {
    throw new Error(`Required ${layoutPath} missing`);
  }
  const layoutJson = readFileSync(layoutPath, "utf8");
  const tierBuffers = STR_TIER_FILENAMES.map((fn) =>
    loadOptional(join(dir, fn)),
  );
  const strings = new StringPool(layoutJson, tierBuffers);

  return {
    adminCells,
    adminEntries,
    adminPolygons,
    adminVertices,
    placeNodes,
    placeCells,
    placeEntries,
    postcodeCentroids,
    postcodeCells,
    postcodeEntries,
    poiRecords,
    poiVertices,
    poiCells,
    poiEntries,
    poiMeta,
    geoCells,
    streetWays,
    streetNodes,
    streetEntries,
    addrPoints,
    addrVertices,
    addrEntries,
    interpWays,
    interpNodes,
    interpEntries,
    wayParents,
    adminParents,
    wayPostcodes,
    addrPostcodes,
    strings,
  };
}
