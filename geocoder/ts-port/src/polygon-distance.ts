// Squared distance from point to polygon (radians²). Returns 0 if the
// point lies inside.  Mirrors Rust polygon_distance_sq.

import { pointInPolygon, pointToSegmentDistance } from "./geometry.js";
import { NODE_COORD_SIZE } from "./types.js";

export function polygonDistanceSq(
  lat: number, lng: number,
  vertsBuf: Buffer, vertOffsetBytes: number, vertCount: number,
  cosLat: number,
): number {
  if (vertCount < 3) return Infinity;
  if (pointInPolygon(Math.fround(lat), Math.fround(lng), vertsBuf, vertOffsetBytes, vertCount)) {
    return 0;
  }
  let best = Infinity;
  for (let i = 0; i < vertCount; i++) {
    const j = i + 1 < vertCount ? i + 1 : 0;
    const a = vertOffsetBytes + i * NODE_COORD_SIZE;
    const b = vertOffsetBytes + j * NODE_COORD_SIZE;
    const aLat = vertsBuf.readFloatLE(a);
    const aLng = vertsBuf.readFloatLE(a + 4);
    const bLat = vertsBuf.readFloatLE(b);
    const bLng = vertsBuf.readFloatLE(b + 4);
    const d = pointToSegmentDistance(lat, lng, aLat, aLng, bLat, bLng, cosLat);
    if (d < best) best = d;
  }
  return best;
}
