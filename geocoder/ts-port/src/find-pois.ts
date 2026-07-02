// Port of Rust find_pois (geocoder/server/src/main.rs:2105). Returns a
// ranked + deduped list of POIs near the query point.  Categories are
// looked up in poi_meta.json (loaded at startup).
//
// Ordering rules (mirror Rust):
//   1. Contained polygons first (the query is physically inside them)
//   2. Within contained, sort by polygon area ascending (most specific)
//   3. Non-contained: by score (importance × proximity decay) descending
// Dedup: drop duplicate names, plus duplicate categories for non-
// contained centroid-only POIs.

import { S2CellId, S2LatLng } from "nodes2ts";

import { forEachEntry, lookupAdminCell } from "./cell-readers.js";
import { pointInPolygon, pointToSegmentDistance } from "./geometry.js";
import { StringPool } from "./string-pool.js";
import { POI_RECORD_SIZE, NODE_COORD_SIZE, NO_DATA, INTERIOR_FLAG, ID_MASK, DEFAULT_ADMIN_CELL_LEVEL } from "./types.js";
import { PoiMeta } from "./poi-meta.js";

const DEG_TO_RAD = Math.PI / 180;
const EARTH_RADIUS_M = 6_371_000;

export interface PoiMatch {
  name: string;
  category: string;
  category_id: number;
  distance_m: number;
  contained: boolean;
  is_point: boolean;
  score: number;
  area: number;
  // String pool offset of the POI's calculated_postcode (parent
  // postal_code polygon at build time). NO_DATA / 0xFFFFFFFF if unset.
  parent_postcode_id: number;
}

export function findPois(
  lat: number,
  lng: number,
  poiCells: Buffer,
  poiEntries: Buffer,
  poiRecords: Buffer,
  poiVertices: Buffer,
  poiMeta: PoiMeta,
  strings: StringPool,
  cellLevel: number = DEFAULT_ADMIN_CELL_LEVEL,
): PoiMatch[] {
  const ll = S2LatLng.fromDegrees(lat, lng);
  const originCell = S2CellId.fromPoint(ll.toPoint()).parentL(cellLevel);
  const neighbors = originCell.getAllNeighbors(cellLevel);
  const cells: bigint[] = [originCell.id, ...neighbors.map((c) => c.id)];

  const cosLat = Math.cos(lat * DEG_TO_RAD);
  const totalPois = Math.floor(poiRecords.length / POI_RECORD_SIZE);
  const totalVerts = Math.floor(poiVertices.length / NODE_COORD_SIZE);
  const latF = Math.fround(lat);
  const lngF = Math.fround(lng);

  const results: PoiMatch[] = [];

  for (const cellId of cells) {
    const entriesOff = lookupAdminCell(poiCells, cellId);
    forEachEntry(poiEntries, entriesOff, (rawId) => {
      const isInterior = (rawId & INTERIOR_FLAG) !== 0;
      const poiId = (rawId & ID_MASK) >>> 0;
      if (poiId >= totalPois) return;
      const off = poiId * POI_RECORD_SIZE;
      const poiLat = poiRecords.readFloatLE(off + 0);
      const poiLng = poiRecords.readFloatLE(off + 4);
      const vertOff = poiRecords.readUInt32LE(off + 8);
      const vertCount = poiRecords.readUInt32LE(off + 12);
      const nameId = poiRecords.readUInt32LE(off + 16);
      const category = poiRecords.readUInt8(off + 20);
      // tier byte at off+21, flags at off+22
      const importanceRaw = poiRecords.readUInt8(off + 23);
      // parent_street_id at off+24 (used by full primary-feature flow,
      // not by find_pois directly).
      const parentPostcodeId = poiRecords.readUInt32LE(off + 28);

      if (nameId === NO_DATA) return;
      const name = strings.get(nameId);
      if (!name) return;

      const refDist = poiMeta.referenceDistance(category);
      const importance = importanceRaw > 0 ? importanceRaw : poiMeta.defaultImportance(category);
      const baseMax = poiMeta.maxDistance(category);

      let distM: number;
      let contained: boolean;
      let areaSqDeg = 0;

      if (vertCount > 0 && vertOff < totalVerts) {
        const vertOffBytes = vertOff * NODE_COORD_SIZE;
        contained = isInterior || pointInPolygon(latF, lngF, poiVertices, vertOffBytes, vertCount);
        if (contained) {
          distM = 0;
          // Shoelace area in squared-degree units. Used as tiebreak for
          // contained polygons — smallest = most specific landmark.
          let a = 0;
          for (let i = 0; i < vertCount; i++) {
            const j = i + 1 < vertCount ? i + 1 : 0;
            const iOff = vertOffBytes + i * NODE_COORD_SIZE;
            const jOff = vertOffBytes + j * NODE_COORD_SIZE;
            const iLat = poiVertices.readFloatLE(iOff);
            const iLng = poiVertices.readFloatLE(iOff + 4);
            const jLat = poiVertices.readFloatLE(jOff);
            const jLng = poiVertices.readFloatLE(jOff + 4);
            a += iLng * jLat - jLng * iLat;
          }
          areaSqDeg = Math.abs(a / 2);
        } else {
          let minDistSq = Infinity;
          for (let i = 0; i < vertCount; i++) {
            const j = i + 1 < vertCount ? i + 1 : 0;
            const iOff = vertOffBytes + i * NODE_COORD_SIZE;
            const jOff = vertOffBytes + j * NODE_COORD_SIZE;
            const iLat = poiVertices.readFloatLE(iOff);
            const iLng = poiVertices.readFloatLE(iOff + 4);
            const jLat = poiVertices.readFloatLE(jOff);
            const jLng = poiVertices.readFloatLE(jOff + 4);
            const d = pointToSegmentDistance(lat, lng, iLat, iLng, jLat, jLng, cosLat);
            if (d < minDistSq) minDistSq = d;
          }
          distM = Math.sqrt(minDistSq) * EARTH_RADIUS_M;
        }
      } else {
        contained = false;
        const dlat = (lat - poiLat) * DEG_TO_RAD;
        const dlng = (lng - poiLng) * DEG_TO_RAD;
        distM = Math.sqrt(dlat * dlat + dlng * dlng * cosLat * cosLat) * EARTH_RADIUS_M;
      }

      // Hard max distance cutoff (importance-scaled)
      if (!contained) {
        if (baseMax === 0) return;
        const effectiveMax = baseMax * (1 + importance / 50);
        if (distM > effectiveMax) return;
      }

      // Drop ISLAND category (matches Rust comment: islands duplicate
      // borough/suburb fields and clutter the places list).
      if (category === 93) return;

      // Score: importance × proximity decay. Contained polygons get a
      // 3× boost so the most-specific landmark surfaces over its
      // contained children (Disneyland > Astro Orbitor).
      const proximityWeight = contained
        ? 3.0
        : 1.0 / (1.0 + Math.pow(distM / refDist, 2));
      const score = importance * proximityWeight;
      if (score < 0.5) return;

      results.push({
        name,
        category: poiMeta.categoryName(category),
        category_id: category,
        distance_m: contained ? 0 : distM,
        contained,
        is_point: vertCount === 0,
        score,
        area: areaSqDeg,
        parent_postcode_id: parentPostcodeId,
      });
    });
  }

  results.sort((a, b) => {
    if (a.contained !== b.contained) return a.contained ? -1 : 1;
    if (a.contained) return a.area - b.area;
    return b.score - a.score;
  });

  const seenNames = new Set<string>();
  const seenPointCats = new Set<string>();
  const out: PoiMatch[] = [];
  for (const r of results) {
    if (seenNames.has(r.name)) continue;
    seenNames.add(r.name);
    if (!r.contained && r.is_point) {
      if (seenPointCats.has(r.category)) continue;
      seenPointCats.add(r.category);
    }
    out.push(r);
    if (out.length >= 5) break;
  }
  return out;
}
