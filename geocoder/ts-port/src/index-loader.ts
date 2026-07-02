// Index loader — reads the admin-tier files into Buffer slabs that
// stay alive for the lifetime of the Geocoder instance.  Reads are
// blocking (fs.readFileSync) since this is once-per-startup.  Buffers
// are then handed off to the various readers and the StringPool.

import { readFileSync, existsSync, statSync } from "node:fs";
import { join } from "node:path";

import { StringPool } from "./string-pool.js";
import { PoiMeta } from "./poi-meta.js";
import { STR_TIER_FILENAMES } from "./types.js";
import { ByteSource, ChunkedFile } from "./byte-source.js";

// Files smaller than MMAP_THRESHOLD load via readFileSync.
// Bigger files use Bun.mmap when available (zero-copy) to avoid heap
// pressure.  Files >4 GiB exceed the JS Buffer cap entirely and load
// via a ChunkedFile (fs.readSync with an LRU page cache).
const MMAP_THRESHOLD = 256 * 1024 * 1024; // 256 MiB
const MAX_BUFFER_SIZE = 0xFFFFFFFF; // 4 GiB - 1
declare const Bun: { mmap?: (path: string) => Uint8Array } | undefined;

function loadFile(path: string, required: boolean): ByteSource | null {
  if (!existsSync(path)) {
    if (required) throw new Error(`Required file missing: ${path}`);
    return null;
  }
  const size = statSync(path).size;
  if (size > MAX_BUFFER_SIZE) {
    return new ChunkedFile(path);
  }
  if (size > MMAP_THRESHOLD && typeof Bun !== "undefined" && Bun?.mmap) {
    const u8 = Bun.mmap(path);
    return Buffer.from(u8.buffer, u8.byteOffset, u8.byteLength);
  }
  return readFileSync(path);
}

// Required admin tier files always fit; load as Buffer directly.
function loadBuffer(path: string, required: boolean): Buffer | null {
  if (!existsSync(path)) {
    if (required) throw new Error(`Required file missing: ${path}`);
    return null;
  }
  const size = statSync(path).size;
  if (size > MAX_BUFFER_SIZE) {
    throw new Error(`${path} exceeds 4 GiB — cannot load as Buffer (admin-tier files should be small)`);
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
  geoCells: ByteSource | null;
  streetWays: ByteSource | null;
  streetNodes: ByteSource | null;
  streetEntries: ByteSource | null;
  addrPoints: ByteSource | null;
  addrVertices: ByteSource | null;
  addrEntries: ByteSource | null;
  interpWays: ByteSource | null;
  interpNodes: ByteSource | null;
  interpEntries: ByteSource | null;
  wayParents: Buffer | null;
  adminParents: Buffer | null;
  wayPostcodes: ByteSource | null;
  addrPostcodes: ByteSource | null;
  strings: StringPool;
}

function loadOptional(path: string): ByteSource | null {
  return loadFile(path, false);
}

export function loadIndex(dir: string): LoadedIndex {
  // Required (admin tier) — always small enough for Buffer.
  const adminCells = loadBuffer(join(dir, "admin_cells.bin"), true)!;
  const adminEntries = loadBuffer(join(dir, "admin_entries.bin"), true)!;
  const adminPolygons = loadBuffer(join(dir, "admin_polygons.bin"), true)!;
  const adminVertices = loadBuffer(join(dir, "admin_vertices.bin"), true)!;

  // Optional small-tier files — always fit in a Buffer.
  const placeNodes = loadBuffer(join(dir, "place_nodes.bin"), false);
  const placeCells = loadBuffer(join(dir, "place_cells.bin"), false);
  const placeEntries = loadBuffer(join(dir, "place_entries.bin"), false);
  const postcodeCentroids = loadBuffer(join(dir, "postcode_centroids.bin"), false);
  const postcodeCells = loadBuffer(join(dir, "postcode_centroid_cells.bin"), false);
  const postcodeEntries = loadBuffer(join(dir, "postcode_centroid_entries.bin"), false);
  const poiRecords = loadBuffer(join(dir, "poi_records.bin"), false);
  const poiVertices = loadBuffer(join(dir, "poi_vertices.bin"), false);
  const poiCells = loadBuffer(join(dir, "poi_cells.bin"), false);
  const poiEntries = loadBuffer(join(dir, "poi_entries.bin"), false);
  const poiMeta = PoiMeta.load(dir);
  // Geo / addr / street / interp may be >4 GiB — load via ByteSource.
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
  const wayParents = loadBuffer(join(dir, "way_parents.bin"), false);
  const adminParents = loadBuffer(join(dir, "admin_parents.bin"), false);
  const wayPostcodes = loadOptional(join(dir, "way_postcodes.bin"));
  const addrPostcodes = loadOptional(join(dir, "addr_postcodes.bin"));

  // String pool — load each tier file that exists, parse layout.
  const layoutPath = join(dir, "strings_layout.json");
  if (!existsSync(layoutPath)) {
    throw new Error(`Required ${layoutPath} missing`);
  }
  const layoutJson = readFileSync(layoutPath, "utf8");
  const tierBuffers = STR_TIER_FILENAMES.map((fn) =>
    loadBuffer(join(dir, fn), false),
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
