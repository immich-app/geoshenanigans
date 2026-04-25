// Index loader — reads the admin-tier files into Buffer slabs that
// stay alive for the lifetime of the Geocoder instance.  Reads are
// blocking (fs.readFileSync) since this is once-per-startup.  Buffers
// are then handed off to the various readers and the StringPool.

import { readFileSync, existsSync } from "node:fs";
import { join } from "node:path";

import { StringPool } from "./string-pool.js";
import { PoiMeta } from "./poi-meta.js";
import { STR_TIER_FILENAMES } from "./types.js";

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
  strings: StringPool;
}

function loadOptional(path: string): Buffer | null {
  return existsSync(path) ? readFileSync(path) : null;
}

export function loadIndex(dir: string): LoadedIndex {
  // Required (admin tier)
  const adminCells = readFileSync(join(dir, "admin_cells.bin"));
  const adminEntries = readFileSync(join(dir, "admin_entries.bin"));
  const adminPolygons = readFileSync(join(dir, "admin_polygons.bin"));
  const adminVertices = readFileSync(join(dir, "admin_vertices.bin"));

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
    strings,
  };
}
