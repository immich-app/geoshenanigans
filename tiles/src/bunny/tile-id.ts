/** The storage zone folder every tile file lives under unless a run says otherwise. */
export const DEFAULT_PREFIX = 'v1';

/** The highest zoom whose tile ids are exact doubles (4^26 < 2^53), as for zxyToTileId. */
export const MAX_SAFE_ZOOM = 26;

/** The first tile id of zoom `z`: the number of tiles in zooms 0..z-1, (4^z - 1) / 3. */
export function zoomStart(z: number): number {
  if (!Number.isInteger(z) || z < 0 || z > MAX_SAFE_ZOOM) {
    throw new Error(`Zoom must be an integer 0..${MAX_SAFE_ZOOM}, got ${z}`);
  }
  return (4 ** z - 1) / 3;
}

/** One past the last tile id of zoom `z`. */
export function zoomEnd(z: number): number {
  return zoomStart(z) + 4 ** z;
}

/** The tile-id range [from, to) holding every position of zooms minZoom..maxZoom. */
export function zoomRange(minZoom: number, maxZoom: number): { from: number; to: number } {
  if (minZoom > maxZoom) {
    throw new Error(`min zoom ${minZoom} is above max zoom ${maxZoom}`);
  }
  return { from: zoomStart(minZoom), to: zoomEnd(maxZoom) };
}

export function tileIdZoom(id: number): number {
  if (!Number.isSafeInteger(id) || id < 0 || id >= zoomEnd(MAX_SAFE_ZOOM)) {
    throw new Error(`Not a tile id: ${id}`);
  }
  let z = 0;
  while (zoomEnd(z) <= id) {
    z++;
  }
  return z;
}

/** The inverse of zxyToTileId: the Hilbert curve position within the zoom back to x, y. */
export function tileIdToZxy(id: number): [z: number, x: number, y: number] {
  const z = tileIdZoom(id);
  const n = 2 ** z;
  // Arithmetic instead of bitwise operators: positions past 2^31 (z16 and up) overflow int32.
  let t = id - zoomStart(z);
  let x = 0;
  let y = 0;
  for (let s = 1; s < n; s *= 2) {
    const rx = Math.floor(t / 2) % 2;
    const ry = (t % 2) ^ rx;
    if (ry === 0) {
      if (rx === 1) {
        x = s - 1 - x;
        y = s - 1 - y;
      }
      [x, y] = [y, x];
    }
    x += s * rx;
    y += s * ry;
    t = Math.floor(t / 4);
  }
  return [z, x, y];
}

/** The zone path of a tile position, `${prefix}/${z}/${x}/${y}.mvt`, relative to the zone root. */
export function tilePath(prefix: string, id: number): string {
  const [z, x, y] = tileIdToZxy(id);
  const folder = prefix.replace(/^\/+|\/+$/g, '');
  return folder ? `${folder}/${z}/${x}/${y}.mvt` : `${z}/${x}/${y}.mvt`;
}
