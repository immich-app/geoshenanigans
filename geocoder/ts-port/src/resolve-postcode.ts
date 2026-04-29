// Nearest-centroid postcode resolution. Subset of the Rust resolve_postcode
// (geocoder/server/src/main.rs:2540) — implements only tier 4 (centroid
// PIP), since the higher tiers (addr_postcode, way_postcode, POI
// parent_postcode) require street/addr/POI data that isn't loaded in
// the admin+place port.  Country gate matches Rust: a French "90012"
// won't win over a US "90012" near the Maine border.

import { S2CellId, S2LatLng } from "nodes2ts";

import { forEachEntry, lookupAdminCell } from "./cell-readers.js";
import { POSTCODE_CENTROID_SIZE, DEFAULT_ADMIN_CELL_LEVEL } from "./types.js";
import { StringPool } from "./string-pool.js";

const DEG_TO_RAD = Math.PI / 180;
// 0.05 deg ≈ 5.5 km, matches Nominatim's get_nearest_postcode.
const MAX_DIST_RAD_SQ = (0.05 * DEG_TO_RAD) ** 2;

// Looser version of the Rust centroid_postcode_ok sanitizer — rejects
// obvious non-postcode garbage (empty, super-long).  Country-aware
// validation is deferred.
function centroidPostcodeOk(s: string): boolean {
  if (s.length === 0 || s.length > 20) return false;
  return true;
}

export function resolveNearestPostcode(
  lat: number,
  lng: number,
  countryCode: string | null,
  postcodeCentroids: Buffer,
  postcodeCells: Buffer | null,
  postcodeEntries: Buffer | null,
  strings: StringPool,
  cellLevel: number = DEFAULT_ADMIN_CELL_LEVEL,
): string | null {
  // Pack the country code into the same u16 the on-disk records use.
  const cgate = countryCode && countryCode.length === 2
    ? (countryCode.charCodeAt(0) << 8) | countryCode.charCodeAt(1)
    : 0;

  const cosLat = Math.cos(lat * DEG_TO_RAD);
  let bestDist = Infinity;
  let bestPc: string | null = null;
  const totalCentroids = Math.floor(postcodeCentroids.length / POSTCODE_CENTROID_SIZE);

  function consider(idx: number) {
    if (idx >= totalCentroids) return;
    const off = idx * POSTCODE_CENTROID_SIZE;
    const pcLat = postcodeCentroids.readFloatLE(off + 0);
    const pcLng = postcodeCentroids.readFloatLE(off + 4);
    const postcodeId = postcodeCentroids.readUInt32LE(off + 8);
    const pcCountry = postcodeCentroids.readUInt16LE(off + 12);
    if (cgate !== 0 && pcCountry !== 0 && pcCountry !== cgate) return;
    const dlat = (lat - pcLat) * DEG_TO_RAD;
    const dlng = (lng - pcLng) * DEG_TO_RAD;
    const d = dlat * dlat + dlng * dlng * cosLat * cosLat;
    if (d >= bestDist || d >= MAX_DIST_RAD_SQ) return;
    const s = strings.get(postcodeId);
    if (!centroidPostcodeOk(s)) return;
    bestDist = d;
    bestPc = s;
  }

  if (postcodeCells && postcodeEntries) {
    const ll = S2LatLng.fromDegrees(lat, lng);
    const originCell = S2CellId.fromPoint(ll.toPoint()).parentL(cellLevel);
    const neighbors = originCell.getAllNeighbors(cellLevel);
    const cells: bigint[] = [originCell.id, ...neighbors.map((c) => c.id)];
    for (const cellId of cells) {
      const entriesOff = lookupAdminCell(postcodeCells, cellId);
      forEachEntry(postcodeEntries, entriesOff, consider);
    }
  } else {
    for (let i = 0; i < totalCentroids; i++) consider(i);
  }
  return bestPc;
}
