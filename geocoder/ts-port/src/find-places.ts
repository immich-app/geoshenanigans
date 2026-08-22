// Port of Rust find_places — locates the nearest place_node per
// place_type (city/town/village/suburb/hamlet/neighbourhood/quarter)
// and applies Nominatim's cascading gates (max radius, boundary
// containment, fuzzy bbox).  Source: geocoder/server/src/main.rs:1672.

import { S2CellId, S2LatLng } from "nodes2ts";

import { AdminPolygon, AddressDetails, PLACE_NODE_SIZE, NODE_COORD_SIZE, DEFAULT_ADMIN_CELL_LEVEL } from "./types.js";
import { forEachEntry, lookupAdminCell } from "./cell-readers.js";
import { pointInPolygon } from "./geometry.js";
import { StringPool } from "./string-pool.js";

interface PlaceCandidate {
  distSq: number;
  lat: number;
  lng: number;
  name_id: number;
}

const PLACE_TYPE_CITY = 0;
const PLACE_TYPE_TOWN = 1;
const PLACE_TYPE_VILLAGE = 2;
const PLACE_TYPE_SUBURB = 3;
const PLACE_TYPE_HAMLET = 4;
const PLACE_TYPE_NEIGHBOURHOOD = 5;
const PLACE_TYPE_QUARTER = 6;

const DEG_TO_RAD = Math.PI / 180;

// Max squared-distance gates per place_type (radians²).  Mirrors
// Nominatim reverse_place_diameter values.
const MAX_CITY_RAD_SQ    = (0.16 * DEG_TO_RAD) ** 2;
const MAX_TOWN_RAD_SQ    = (0.08 * DEG_TO_RAD) ** 2;
const MAX_VILLAGE_RAD_SQ = (0.04 * DEG_TO_RAD) ** 2;
const MAX_RANK20_RAD_SQ  = (0.02 * DEG_TO_RAD) ** 2;

// Half-side metres for fuzzy bbox per place_type. Matches Rust source.
function fuzzyHalfSideM(pt: number): number {
  let r;
  switch (pt) {
    case 0: r = 15000; break;
    case 1: r = 4000; break;
    case 2: case 3: r = 2000; break;
    case 4: case 5: case 6: r = 1000; break;
    default: r = 500;
  }
  return r / Math.SQRT2;
}

export function findPlaces(
  lat: number,
  lng: number,
  placeCells: Buffer,
  placeEntries: Buffer,
  placeNodes: Buffer,
  adminPolygons: Buffer,
  adminVertices: Buffer,
  currentBoundary: AdminPolygon | null,
  municipalityBoundary: AdminPolygon | null,
  strings: StringPool,
  address: AddressDetails,
  cellLevel: number = DEFAULT_ADMIN_CELL_LEVEL,
): void {
  const ll = S2LatLng.fromDegrees(lat, lng);
  const originCell = S2CellId.fromPoint(ll.toPoint()).parentL(cellLevel);
  const neighbors = originCell.getAllNeighbors(cellLevel);
  const cells: bigint[] = [originCell.id, ...neighbors.map((c) => c.id)];

  const cosLat = Math.cos(lat * DEG_TO_RAD);
  const placeNodeCount = Math.floor(placeNodes.length / PLACE_NODE_SIZE);

  // best[pt] = nearest candidate found so far for each of 7 place types
  const best: (PlaceCandidate | null)[] = new Array(7).fill(null);

  for (const cellId of cells) {
    const entriesOff = lookupAdminCell(placeCells, cellId);
    forEachEntry(placeEntries, entriesOff, (id) => {
      if (id >= placeNodeCount) return;
      const off = id * PLACE_NODE_SIZE;
      const pnLat = placeNodes.readFloatLE(off + 0);
      const pnLng = placeNodes.readFloatLE(off + 4);
      const nameId = placeNodes.readUInt32LE(off + 8);
      const placeType = placeNodes.readUInt8(off + 12);
      if (placeType >= 7) return;

      const dlat = (lat - pnLat) * DEG_TO_RAD;
      const dlng = (lng - pnLng) * DEG_TO_RAD;
      const distSq = dlat * dlat + dlng * dlng * cosLat * cosLat;

      const cur = best[placeType];
      if (cur && distSq >= cur.distSq) return;
      best[placeType] = { distSq, lat: pnLat, lng: pnLng, name_id: nameId };
    });
  }

  // PIP a place node's centroid against a containing-boundary polygon.
  // Returns true if the boundary is missing (degenerate accept).
  function nodeInsideBoundary(pn: PlaceCandidate, boundary: AdminPolygon | null): boolean {
    if (!boundary) return true;
    return pointInPolygon(
      Math.fround(pn.lat),
      Math.fround(pn.lng),
      adminVertices,
      boundary.vertex_offset * NODE_COORD_SIZE,
      boundary.vertex_count,
    );
  }

  // Fuzzy bbox check: reject a candidate whose centroid sits outside the
  // bbox projected from the previously-accepted place node's centre.
  let fuzzy: { lat: number; lng: number; halfM: number } | null = null;
  function insideFuzzy(pn: PlaceCandidate): boolean {
    if (!fuzzy) return true;
    const dlatM = Math.abs((pn.lat - fuzzy.lat) * 111320);
    const cl = Math.cos(fuzzy.lat * DEG_TO_RAD);
    const dlngM = Math.abs((pn.lng - fuzzy.lng) * 111320 * cl);
    return dlatM <= fuzzy.halfM && dlngM <= fuzzy.halfM;
  }

  // City / town / village (ranks 16-19): pick first valid in that
  // priority order. Single slot is filled.
  for (const pt of [PLACE_TYPE_CITY, PLACE_TYPE_TOWN, PLACE_TYPE_VILLAGE]) {
    const cand = best[pt];
    if (!cand) continue;
    const threshold = pt === PLACE_TYPE_CITY ? MAX_CITY_RAD_SQ
      : pt === PLACE_TYPE_TOWN ? MAX_TOWN_RAD_SQ
      : MAX_VILLAGE_RAD_SQ;
    if (cand.distSq > threshold) continue;
    if (!nodeInsideBoundary(cand, municipalityBoundary)) continue;
    const name = strings.get(cand.name_id);
    if (!name) continue;
    if (pt === PLACE_TYPE_CITY) address.city = name;
    else if (pt === PLACE_TYPE_TOWN) address.town = name;
    else address.village = name;
    fuzzy = { lat: cand.lat, lng: cand.lng, halfM: fuzzyHalfSideM(pt) };
    break;
  }

  // Rank 20: suburb vs hamlet — closer one wins, single slot.
  const suburb = best[PLACE_TYPE_SUBURB];
  const hamlet = best[PLACE_TYPE_HAMLET];
  const suburbValid = suburb && suburb.distSq <= MAX_RANK20_RAD_SQ &&
    nodeInsideBoundary(suburb, currentBoundary) && insideFuzzy(suburb);
  const hamletValid = hamlet && hamlet.distSq <= MAX_RANK20_RAD_SQ &&
    nodeInsideBoundary(hamlet, currentBoundary) && insideFuzzy(hamlet);
  if (suburbValid && hamletValid) {
    if (suburb!.distSq <= hamlet!.distSq) {
      address.suburb = strings.get(suburb!.name_id);
      fuzzy = { lat: suburb!.lat, lng: suburb!.lng, halfM: fuzzyHalfSideM(PLACE_TYPE_SUBURB) };
    } else {
      address.hamlet = strings.get(hamlet!.name_id);
      fuzzy = { lat: hamlet!.lat, lng: hamlet!.lng, halfM: fuzzyHalfSideM(PLACE_TYPE_HAMLET) };
    }
  } else if (suburbValid) {
    address.suburb = strings.get(suburb!.name_id);
    fuzzy = { lat: suburb!.lat, lng: suburb!.lng, halfM: fuzzyHalfSideM(PLACE_TYPE_SUBURB) };
  } else if (hamletValid) {
    address.hamlet = strings.get(hamlet!.name_id);
    fuzzy = { lat: hamlet!.lat, lng: hamlet!.lng, halfM: fuzzyHalfSideM(PLACE_TYPE_HAMLET) };
  }

  // Rank 22: quarter vs neighbourhood — closer wins, single slot.
  const quarter = best[PLACE_TYPE_QUARTER];
  const neigh = best[PLACE_TYPE_NEIGHBOURHOOD];
  const quarterValid = quarter && quarter.distSq <= MAX_RANK20_RAD_SQ &&
    nodeInsideBoundary(quarter, currentBoundary) && insideFuzzy(quarter);
  const neighValid = neigh && neigh.distSq <= MAX_RANK20_RAD_SQ &&
    nodeInsideBoundary(neigh, currentBoundary) && insideFuzzy(neigh);
  if (quarterValid && neighValid) {
    if (quarter!.distSq <= neigh!.distSq) {
      address.quarter = strings.get(quarter!.name_id);
    } else {
      address.neighbourhood = strings.get(neigh!.name_id);
    }
  } else if (quarterValid) {
    address.quarter = strings.get(quarter!.name_id);
  } else if (neighValid) {
    address.neighbourhood = strings.get(neigh!.name_id);
  }
}
