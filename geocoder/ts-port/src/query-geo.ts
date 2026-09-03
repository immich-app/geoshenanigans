// Port of Rust query_geo (geocoder/server/src/main.rs:684).  Returns
// the nearest addr_point, street, and interpolation candidate at the
// query coordinate.  Caller (geocoder.reverse) picks the primary
// feature using these distances + the POI candidate.
//
// geo_cells.bin format (different from admin_cells): each cell record is
//   u64 cell_id + u32 street_off + u32 addr_off + u32 interp_off = 20 bytes.
// Look up by binary search.

import { S2CellId, S2LatLng } from "nodes2ts";

import { distSq, pointToSegmentDistance } from "./geometry.js";
import { forEachEntry } from "./cell-readers.js";
import { StringPool } from "./string-pool.js";
import {
  ADDR_POINT_SIZE,
  WAY_HEADER_SIZE_DEFAULT,
  INTERP_WAY_SIZE_DEFAULT,
  NODE_COORD_SIZE,
  NO_DATA,
  DEFAULT_STREET_CELL_LEVEL,
} from "./types.js";
import { polygonDistanceSq } from "./polygon-distance.js";
import { ByteSource } from "./byte-source.js";

const DEG_TO_RAD = Math.PI / 180;
const GEO_CELL_RECORD_SIZE = 20; // u64 cell + u32 street + u32 addr + u32 interp

interface GeoCellOffsets {
  street: number;
  addr: number;
  interp: number;
}

function lookupGeoCell(buf: ByteSource, target: bigint): GeoCellOffsets {
  const n = Math.floor(buf.length / GEO_CELL_RECORD_SIZE);
  let lo = 0, hi = n;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    const off = mid * GEO_CELL_RECORD_SIZE;
    const id = buf.readBigUInt64LE(off);
    if (id < target) lo = mid + 1;
    else if (id > target) hi = mid;
    else return {
      street: buf.readUInt32LE(off + 8),
      addr:   buf.readUInt32LE(off + 12),
      interp: buf.readUInt32LE(off + 16),
    };
  }
  return { street: NO_DATA, addr: NO_DATA, interp: NO_DATA };
}

export interface AddrCandidate {
  distSq: number;
  index: number;
  lat: number;
  lng: number;
  housenumber_id: number;
  street_id: number;
  parent_way_id: number;
}

export interface StreetCandidate {
  distSq: number;
  way_index: number;
  name_id: number;
  node_offset: number;
  node_count: number;
}

export interface InterpCandidate {
  distSq: number;
  street_id: number;
  number: number;     // computed housenumber along the segment
}

export interface QueryGeoResult {
  addr: AddrCandidate | null;
  street: StreetCandidate | null;
  interp: InterpCandidate | null;
}

export function queryGeo(
  lat: number,
  lng: number,
  geoCells: ByteSource,
  streetEntries: ByteSource | null,
  streetWays: ByteSource | null,
  streetNodes: ByteSource | null,
  addrEntries: ByteSource | null,
  addrPoints: ByteSource | null,
  addrVertices: ByteSource | null,
  interpEntries: ByteSource | null,
  interpWays: ByteSource | null,
  interpNodes: ByteSource | null,
  cellLevel: number = DEFAULT_STREET_CELL_LEVEL,
): QueryGeoResult {
  const ll = S2LatLng.fromDegrees(lat, lng);
  const originCell = S2CellId.fromPoint(ll.toPoint()).parentL(cellLevel);
  const neighbors = originCell.getAllNeighbors(cellLevel);
  const cells: bigint[] = [originCell.id, ...neighbors.map((c) => c.id)];

  const cosLat = Math.cos(lat * DEG_TO_RAD);

  let bestAddrDist = Infinity;
  let bestAddr: AddrCandidate | null = null;
  let bestStreetDist = Infinity;
  let bestStreet: StreetCandidate | null = null;
  let bestInterpDist = Infinity;
  let bestInterp: { distSq: number; iw_offset: number; t: number } | null = null;

  // Tiny dedup ring for street IDs already seen in earlier cells.
  const seenStreets = new Int32Array(64).fill(-1);

  const totalAddr = addrPoints ? Math.floor(addrPoints.length / ADDR_POINT_SIZE) : 0;
  const totalWays = streetWays ? Math.floor(streetWays.length / WAY_HEADER_SIZE_DEFAULT) : 0;
  const totalInterps = interpWays ? Math.floor(interpWays.length / INTERP_WAY_SIZE_DEFAULT) : 0;

  for (const cellId of cells) {
    const offs = lookupGeoCell(geoCells, cellId);

    // Addresses
    if (addrEntries && addrPoints) {
      forEachEntry(addrEntries, offs.addr, (id) => {
        if (id >= totalAddr) return;
        const off = id * ADDR_POINT_SIZE;
        const aLat = addrPoints.readFloatLE(off + 0);
        const aLng = addrPoints.readFloatLE(off + 4);
        const housenumberId = addrPoints.readUInt32LE(off + 8);
        const streetId = addrPoints.readUInt32LE(off + 12);
        const parentWayId = addrPoints.readUInt32LE(off + 16);
        const vertOff = addrPoints.readUInt32LE(off + 20);
        const vertCount = addrPoints.readUInt32LE(off + 24);

        const dlat = (aLat - lat) * DEG_TO_RAD;
        const dlng = (aLng - lng) * DEG_TO_RAD;
        let dist = distSq(dlat, dlng, cosLat);

        if (vertCount > 0 && vertOff !== NO_DATA && addrVertices) {
          dist = polygonDistanceSq(lat, lng, addrVertices, vertOff * NODE_COORD_SIZE, vertCount, cosLat);
        }
        if (dist < bestAddrDist) {
          bestAddrDist = dist;
          bestAddr = { distSq: dist, index: id, lat: aLat, lng: aLng, housenumber_id: housenumberId, street_id: streetId, parent_way_id: parentWayId };
        }
      });
    }

    // Streets
    if (streetEntries && streetWays && streetNodes) {
      forEachEntry(streetEntries, offs.street, (id) => {
        if (id >= totalWays) return;
        const slot = id & 0x3F;
        if (seenStreets[slot] === id) return;
        seenStreets[slot] = id;

        const wayOff = id * WAY_HEADER_SIZE_DEFAULT;
        const nodeOffset = streetWays.readUInt32LE(wayOff + 0);
        const nodeCount = streetWays.readUInt8(wayOff + 4);
        const nameId = streetWays.readUInt32LE(wayOff + 8);
        if (nodeCount < 2) return;

        const nodesBase = nodeOffset * NODE_COORD_SIZE;
        if (nodesBase + nodeCount * NODE_COORD_SIZE > streetNodes.length) return;
        for (let i = 0; i < nodeCount - 1; i++) {
          const a = nodesBase + i * NODE_COORD_SIZE;
          const b = nodesBase + (i + 1) * NODE_COORD_SIZE;
          const aLat = streetNodes.readFloatLE(a);
          const aLng = streetNodes.readFloatLE(a + 4);
          const bLat = streetNodes.readFloatLE(b);
          const bLng = streetNodes.readFloatLE(b + 4);
          const d = pointToSegmentDistance(lat, lng, aLat, aLng, bLat, bLng, cosLat);
          if (d < bestStreetDist) {
            bestStreetDist = d;
            bestStreet = { distSq: d, way_index: id, name_id: nameId, node_offset: nodeOffset, node_count: nodeCount };
          }
        }
      });
    }

    // Interpolation
    if (interpEntries && interpWays && interpNodes) {
      forEachEntry(interpEntries, offs.interp, (id) => {
        if (id >= totalInterps) return;
        const iwOff = id * INTERP_WAY_SIZE_DEFAULT;
        const nodeOffset = interpWays.readUInt32LE(iwOff + 0);
        const nodeCount = interpWays.readUInt8(iwOff + 4);
        const streetId = interpWays.readUInt32LE(iwOff + 8);
        const startNumber = interpWays.readUInt32LE(iwOff + 12);
        const endNumber = interpWays.readUInt32LE(iwOff + 16);
        const interpolation = interpWays.readUInt8(iwOff + 20);
        if (startNumber === 0 || endNumber === 0) return;
        if (nodeCount < 2) return;

        const nodesBase = nodeOffset * NODE_COORD_SIZE;
        // Sum total length first (radians²).
        let totalLen = 0;
        for (let i = 0; i < nodeCount - 1; i++) {
          const a = nodesBase + i * NODE_COORD_SIZE;
          const b = nodesBase + (i + 1) * NODE_COORD_SIZE;
          const aLat = interpNodes.readFloatLE(a);
          const aLng = interpNodes.readFloatLE(a + 4);
          const bLat = interpNodes.readFloatLE(b);
          const bLng = interpNodes.readFloatLE(b + 4);
          const dlat = (bLat - aLat) * DEG_TO_RAD;
          const dlng = (bLng - aLng) * DEG_TO_RAD;
          totalLen += distSq(dlat, dlng, cosLat);
        }
        if (totalLen === 0) return;

        let bestSegDist = Infinity;
        let bestSegT = 0;
        let prevAcc = 0;
        for (let i = 0; i < nodeCount - 1; i++) {
          const a = nodesBase + i * NODE_COORD_SIZE;
          const b = nodesBase + (i + 1) * NODE_COORD_SIZE;
          const aLat = interpNodes.readFloatLE(a);
          const aLng = interpNodes.readFloatLE(a + 4);
          const bLat = interpNodes.readFloatLE(b);
          const bLng = interpNodes.readFloatLE(b + 4);
          const dlat = (bLat - aLat) * DEG_TO_RAD;
          const dlng = (bLng - aLng) * DEG_TO_RAD;
          const segLen = distSq(dlat, dlng, cosLat);

          // Project query onto segment, return (distSq, t) where t∈[0,1]
          const dx = bLng - aLng;
          const dy = bLat - aLat;
          const lenSqRaw = dx * dx + dy * dy;
          let t = lenSqRaw === 0 ? 0 : ((lng - aLng) * dx + (lat - aLat) * dy) / lenSqRaw;
          if (t < 0) t = 0; else if (t > 1) t = 1;
          const projLat = aLat + t * dy;
          const projLng = aLng + t * dx;
          const dlat2 = (lat - projLat) * DEG_TO_RAD;
          const dlng2 = (lng - projLng) * DEG_TO_RAD;
          const dist = distSq(dlat2, dlng2, cosLat);
          if (dist < bestSegDist) {
            bestSegDist = dist;
            bestSegT = (prevAcc + t * segLen) / totalLen;
          }
          prevAcc += segLen;
        }
        if (bestSegDist < bestInterpDist) {
          bestInterpDist = bestSegDist;
          bestInterp = { distSq: bestSegDist, iw_offset: iwOff, t: bestSegT };
        }
      });
    }
  }

  let interpResult: InterpCandidate | null = null;
  if (bestInterp && interpWays) {
    const iwOff = bestInterp.iw_offset;
    const streetId = interpWays.readUInt32LE(iwOff + 8);
    const startNumber = interpWays.readUInt32LE(iwOff + 12);
    const endNumber = interpWays.readUInt32LE(iwOff + 16);
    const interpolation = interpWays.readUInt8(iwOff + 20);
    const raw = startNumber + bestInterp.t * (endNumber - startNumber);
    const step = interpolation === 1 || interpolation === 2 ? 2 : 1;
    let number;
    if (step === 2) {
      number = startNumber + Math.round((raw - startNumber) / 2) * 2;
    } else {
      number = Math.round(raw);
    }
    interpResult = { distSq: bestInterp.distSq, street_id: streetId, number };
  }

  return { addr: bestAddr, street: bestStreet, interp: interpResult };
}

// Find nearest addr_point on a specific way (parent_way_id match).
// Used when the street wins primary selection — refines the result
// with a housenumber.  Mirrors Rust find_addr_on_way.
export function findAddrOnWay(
  lat: number,
  lng: number,
  parentWayIdx: number,
  maxDistSq: number,
  geoCells: ByteSource,
  addrEntries: ByteSource,
  addrPoints: ByteSource,
  cellLevel: number = DEFAULT_STREET_CELL_LEVEL,
): AddrCandidate | null {
  const ll = S2LatLng.fromDegrees(lat, lng);
  const originCell = S2CellId.fromPoint(ll.toPoint()).parentL(cellLevel);
  const neighbors = originCell.getAllNeighbors(cellLevel);
  const cells: bigint[] = [originCell.id, ...neighbors.map((c) => c.id)];

  const cosLat = Math.cos(lat * DEG_TO_RAD);
  const totalAddr = Math.floor(addrPoints.length / ADDR_POINT_SIZE);
  let bestDist = maxDistSq;
  let best: AddrCandidate | null = null;

  for (const cellId of cells) {
    const offs = lookupGeoCell(geoCells, cellId);
    forEachEntry(addrEntries, offs.addr, (id) => {
      if (id >= totalAddr) return;
      const off = id * ADDR_POINT_SIZE;
      const parentWay = addrPoints.readUInt32LE(off + 16);
      if (parentWay !== parentWayIdx) return;
      const aLat = addrPoints.readFloatLE(off + 0);
      const aLng = addrPoints.readFloatLE(off + 4);
      const dlat = (aLat - lat) * DEG_TO_RAD;
      const dlng = (aLng - lng) * DEG_TO_RAD;
      const d = distSq(dlat, dlng, cosLat);
      if (d < bestDist) {
        bestDist = d;
        best = {
          distSq: d, index: id, lat: aLat, lng: aLng,
          housenumber_id: addrPoints.readUInt32LE(off + 8),
          street_id: addrPoints.readUInt32LE(off + 12),
          parent_way_id: parentWay,
        };
      }
    });
  }
  return best;
}
