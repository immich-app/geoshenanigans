// Simplified port of the Rust server's find_admin.
//
// SCOPE: produces a minimal admin hierarchy (country/state/city/etc.)
// for benchmarking. Skips several Nominatim parity rules that the Rust
// implementation handles (Buenos Aires Comuna promotion, place_area
// rule #3, country-specific level overrides, etc.). The intent is to
// exercise the same hot path (S2 cell lookup → PIP → smallest-area
// pick) so per-query throughput numbers are comparable, not full
// feature parity.

import { S2CellId, S2LatLng } from "nodes2ts";

import { forEachEntry, lookupAdminCell } from "./cell-readers.js";
import { pointInPolygon } from "./geometry.js";
import { StringPool } from "./string-pool.js";
import {
  AdminPolygon,
  ADMIN_POLYGON_SIZE,
  ID_MASK,
  INTERIOR_FLAG,
  AddressDetails,
  DEFAULT_ADMIN_CELL_LEVEL,
  NODE_COORD_SIZE,
} from "./types.js";
import { DEFAULT_ADMIN_LEVEL_MAP, placeTypeToField } from "./admin-config.js";

const ADMIN_LEVEL_COUNT = 16;

function readPolygon(buf: Buffer, polyId: number): AdminPolygon | null {
  const off = polyId * ADMIN_POLYGON_SIZE;
  if (off + ADMIN_POLYGON_SIZE > buf.length) return null;
  return {
    index: polyId,
    vertex_offset: buf.readUInt32LE(off + 0),
    vertex_count: buf.readUInt32LE(off + 4),
    name_id: buf.readUInt32LE(off + 8),
    admin_level: buf.readUInt8(off + 12),
    place_type_override: buf.readUInt8(off + 13),
    area: buf.readFloatLE(off + 16),
    country_code: buf.readUInt16LE(off + 20),
  };
}

export interface FindAdminResult {
  // smallest-area polygon containing the point at each admin_level
  byLevel: (AdminPolygon | null)[];
  // ISO country code from the admin_level=2 polygon, if any
  countryCode: string | null;
}

export function findAdmin(
  lat: number,
  lng: number,
  adminCells: Buffer,
  adminEntries: Buffer,
  adminPolygons: Buffer,
  adminVertices: Buffer,
  cellLevel: number = DEFAULT_ADMIN_CELL_LEVEL,
): FindAdminResult {
  // Build cell + neighbor list for the lookup pass.
  const ll = S2LatLng.fromDegrees(lat, lng);
  const originCell = S2CellId.fromPoint(ll.toPoint()).parentL(cellLevel);
  const neighbors = originCell.getAllNeighbors(cellLevel);
  const cells: bigint[] = [originCell.id, ...neighbors.map((c) => c.id)];

  // best[level] = smallest-area polygon known to contain the point at
  // that admin_level. Tracks PIP-verified-ness so we can skip redundant
  // verification when interior-flag fast path is unambiguous.
  type Slot = { area: number; poly: AdminPolygon; verified: boolean };
  const best: (Slot | null)[] = new Array(ADMIN_LEVEL_COUNT).fill(null);
  const latF = Math.fround(lat);
  const lngF = Math.fround(lng);

  for (const cellId of cells) {
    const entriesOff = lookupAdminCell(adminCells, cellId);
    forEachEntry(adminEntries, entriesOff, (rawId) => {
      const isInterior = (rawId & INTERIOR_FLAG) !== 0;
      const polyId = rawId & ID_MASK;
      const poly = readPolygon(adminPolygons, polyId);
      if (poly === null) return;
      const level = poly.admin_level;
      if (level >= ADMIN_LEVEL_COUNT) return;
      if (poly.area <= 0) return;

      const cur = best[level];
      if (cur && poly.area >= cur.area) return; // not smaller — skip

      // Always PIP when this candidate is smaller than current best —
      // the interior-flag fast path is unreliable at borders (e.g. NYC
      // cell that contains both NY and NJ polygons). Rust does the
      // same: PIP wins over flag.
      const inside = pointInPolygon(
        latF,
        lngF,
        adminVertices,
        poly.vertex_offset * NODE_COORD_SIZE,
        poly.vertex_count,
      );
      if (!inside) return;

      best[level] = { area: poly.area, poly, verified: true };
    });
  }

  // Resolve country code from level 2.
  let countryCode: string | null = null;
  const l2 = best[2]?.poly;
  if (l2 && l2.country_code !== 0) {
    // ISO codes from OSM are uppercase ("GB"), kept verbatim — matches
    // the Rust server output.
    countryCode = String.fromCharCode((l2.country_code >> 8) & 0xff, l2.country_code & 0xff);
  }

  return {
    byLevel: best.map((s) => s?.poly ?? null),
    countryCode,
  };
}

// Project the per-level admin polygons into the AddressDetails fields.
// Names are resolved from the string pool. Per-polygon place_type_override
// (set when the builder linked an admin polygon to a place node) wins
// over the default level→field mapping.
export function applyAdminToAddress(
  result: FindAdminResult,
  address: AddressDetails,
  strings: StringPool,
): void {
  for (let level = 0; level < ADMIN_LEVEL_COUNT; level++) {
    const poly = result.byLevel[level];
    if (!poly) continue;
    const name = strings.get(poly.name_id);
    if (!name) continue;

    // Override-field if the polygon was linked to a specific place type.
    const overrideField = placeTypeToField(poly.place_type_override);
    const field = overrideField ?? DEFAULT_ADMIN_LEVEL_MAP[level];
    if (!field) continue;
    if (!(field in address)) {
      (address as Record<string, string>)[field] = name;
    }
  }
  if (result.countryCode) {
    address.country_code = result.countryCode;
  }
}
