// Polygon geometry primitives. Direct ports of the corresponding Rust
// functions in geocoder/server/src/main.rs:
//   - point_in_polygon (ray-casting in lat/lng plane, f32 input)
//   - polygon_distance_sq, point_to_segment_distance (Euclidean in degrees,
//     scaled by cos(lat) for distance comparisons)
//
// Vertices are stored as f32 lat/lng pairs in NodeCoord buffers
// (NODE_COORD_SIZE = 8 bytes). Caller passes a buffer + offset + count;
// readers index in-place to avoid allocation per record.

import { NODE_COORD_SIZE } from "./types.js";

// Ray-casting PIP. Vertices form a closed ring (first == last is fine
// but not required by the algorithm itself; our builder emits closed rings).
// Matches Rust `point_in_polygon(lat: f32, lng: f32, vertices: &[NodeCoord]) -> bool`.
export function pointInPolygon(
  lat: number,
  lng: number,
  vertsBuf: Buffer,
  vertOffsetBytes: number,
  vertCount: number,
): boolean {
  if (vertCount < 3) return false;
  let inside = false;
  let j = vertCount - 1;
  for (let i = 0; i < vertCount; i++) {
    const iOff = vertOffsetBytes + i * NODE_COORD_SIZE;
    const jOff = vertOffsetBytes + j * NODE_COORD_SIZE;
    const lat_i = vertsBuf.readFloatLE(iOff);
    const lng_i = vertsBuf.readFloatLE(iOff + 4);
    const lat_j = vertsBuf.readFloatLE(jOff);
    const lng_j = vertsBuf.readFloatLE(jOff + 4);
    // f32 comparison — match Rust's f32 truncation by writing through Float32Array
    const intersects = (lng_i > lng) !== (lng_j > lng) &&
      lat < ((lat_j - lat_i) * (lng - lng_i)) / (lng_j - lng_i) + lat_i;
    if (intersects) inside = !inside;
    j = i;
  }
  return inside;
}

// Squared planar distance scaled by cos(lat). Useful for nearest-feature
// comparisons within a small region (a few km). Matches Rust dist_sq.
export function distSq(dlat: number, dlng: number, cosLat: number): number {
  const x = dlng * cosLat;
  const y = dlat;
  return x * x + y * y;
}

const DEG_TO_RAD = Math.PI / 180;

// Squared distance (radians²) from point P to segment AB on the lat/lng
// plane.  Mirrors Rust point_to_segment_distance.
export function pointToSegmentDistance(
  px: number, py: number,
  ax: number, ay: number,
  bx: number, by: number,
  cosLat: number,
): number {
  const dx = bx - ax;
  const dy = by - ay;
  const lenSq = dx * dx + dy * dy;
  let t = 0;
  if (lenSq !== 0) {
    t = ((px - ax) * dx + (py - ay) * dy) / lenSq;
    if (t < 0) t = 0;
    else if (t > 1) t = 1;
  }
  const projX = ax + t * dx;
  const projY = ay + t * dy;
  const dlat = (px - projX) * DEG_TO_RAD;
  const dlng = (py - projY) * DEG_TO_RAD;
  return distSq(dlat, dlng, cosLat);
}
