import { Compression, Entry, Header, Metadata } from './types';

const tileZoomValues: number[] = [
  0, 1, 5, 21, 85, 341, 1365, 5461, 21845, 87381, 349525, 1398101, 5592405, 22369621, 89478485, 357913941, 1431655765,
  5726623061, 22906492245, 91625968981, 366503875925, 1466015503701, 5864062014805, 23456248059221, 93824992236885,
  375299968947541, 1501199875790165,
];

function rotate(n: number, xy: number[], rx: number, ry: number): void {
  if (ry === 0) {
    if (rx === 1) {
      xy[0] = n - 1 - xy[0];
      xy[1] = n - 1 - xy[1];
    }
    const t = xy[0];
    xy[0] = xy[1];
    xy[1] = t;
  }
}

/**
 * Convert Z,X,Y to a Hilbert TileID.
 */
export function zxyToTileId(z: number, x: number, y: number): number {
  if (z > 26) {
    throw Error('Tile zoom level exceeds max safe number limit (26)');
  }
  if (x > 2 ** z - 1 || y > 2 ** z - 1) {
    throw Error('tile x/y outside zoom level bounds');
  }

  const acc = tileZoomValues[z];
  const n = 2 ** z;
  let rx = 0;
  let ry = 0;
  let d = 0;
  const xy = [x, y];
  let s = n / 2;
  while (s > 0) {
    rx = (xy[0] & s) > 0 ? 1 : 0;
    ry = (xy[1] & s) > 0 ? 1 : 0;
    d += s * s * ((3 * rx) ^ ry);
    rotate(s, xy, rx, ry);
    s = s / 2;
  }
  return acc + d;
}

function toNum(low: number, high: number): number {
  return (high >>> 0) * 0x100000000 + (low >>> 0);
}

function readVarintRemainder(lowBits: number, bufferPosition: BufferPosition): number {
  const buffer = bufferPosition.buf;
  let byte = buffer[bufferPosition.pos++];
  let highBits = (byte & 0x70) >> 4;
  if (byte < 0x80) {
    return toNum(lowBits, highBits);
  }
  byte = buffer[bufferPosition.pos++];
  highBits |= (byte & 0x7f) << 3;
  if (byte < 0x80) {
    return toNum(lowBits, highBits);
  }
  byte = buffer[bufferPosition.pos++];
  highBits |= (byte & 0x7f) << 10;
  if (byte < 0x80) {
    return toNum(lowBits, highBits);
  }
  byte = buffer[bufferPosition.pos++];
  highBits |= (byte & 0x7f) << 17;
  if (byte < 0x80) {
    return toNum(lowBits, highBits);
  }
  byte = buffer[bufferPosition.pos++];
  highBits |= (byte & 0x7f) << 24;
  if (byte < 0x80) {
    return toNum(lowBits, highBits);
  }
  byte = buffer[bufferPosition.pos++];
  highBits |= (byte & 0x01) << 31;
  if (byte < 0x80) {
    return toNum(lowBits, highBits);
  }
  throw new Error('Expected varint not more than 10 bytes');
}

export interface BufferPosition {
  buf: Uint8Array;
  pos: number;
}

/** @hidden */
export function readVarint(bufferPosition: BufferPosition): number {
  const buffer = bufferPosition.buf;
  let byte = buffer[bufferPosition.pos++];
  let val = byte & 0x7f;
  if (byte < 0x80) {
    return val;
  }
  byte = buffer[bufferPosition.pos++];
  val |= (byte & 0x7f) << 7;
  if (byte < 0x80) {
    return val;
  }
  byte = buffer[bufferPosition.pos++];
  val |= (byte & 0x7f) << 14;
  if (byte < 0x80) {
    return val;
  }
  byte = buffer[bufferPosition.pos++];
  val |= (byte & 0x7f) << 21;
  if (byte < 0x80) {
    return val;
  }
  byte = buffer[bufferPosition.pos];
  val |= (byte & 0x0f) << 28;

  return readVarintRemainder(val, bufferPosition);
}

export const tileJSON = (args: { header: Header; metadata: Metadata; url: URL; version: string }) => {
  const { header, metadata, url, version } = args;
  return {
    tilejson: '3.0.0',
    scheme: 'xyz',
    tiles: [
      `${url.protocol}//` +
        url.hostname +
        `${url.port ? `:${url.port}` : ''}` +
        `/v${version}` +
        '/{z}/{x}/{y}' +
        '.mvt',
    ],
    vector_layers: metadata.vector_layers,
    attribution: metadata.attribution,
    description: metadata.description,
    name: metadata.name,
    version: metadata.version,
    bounds: [header.minLon, header.minLat, header.maxLon, header.maxLat],
    center: [header.centerLon, header.centerLat, header.centerZoom],
    minzoom: header.minZoom,
    maxzoom: header.maxZoom,
  };
};

export function deserializeIndex(buffer: ArrayBuffer): Entry[] {
  const p = { buf: new Uint8Array(buffer), pos: 0 };
  const numEntries = readVarint(p);

  const entries: Entry[] = [];

  let lastId = 0;
  for (let i = 0; i < numEntries; i++) {
    const v = readVarint(p);
    entries.push({ tileId: lastId + v, offset: 0, length: 0, runLength: 1 });
    lastId += v;
  }

  for (let i = 0; i < numEntries; i++) {
    entries[i].runLength = readVarint(p);
  }

  for (let i = 0; i < numEntries; i++) {
    entries[i].length = readVarint(p);
  }

  for (let i = 0; i < numEntries; i++) {
    const v = readVarint(p);
    if (v === 0 && i > 0) {
      entries[i].offset = entries[i - 1].offset + entries[i - 1].length;
    } else {
      entries[i].offset = v - 1;
    }
  }

  return entries;
}

/**
 * Low-level function for looking up a TileID or leaf directory inside a directory.
 */
export function findTile(entries: Entry[], tileId: number): Entry | undefined {
  let m = 0;
  let n = entries.length - 1;
  while (m <= n) {
    const k = (n + m) >> 1;
    const cmp = tileId - entries[k].tileId;
    if (cmp > 0) {
      m = k + 1;
    } else if (cmp < 0) {
      n = k - 1;
    } else {
      return entries[k];
    }
  }

  // at this point, m > n
  if (n >= 0) {
    if (entries[n].runLength === 0) {
      return entries[n];
    }
    if (tileId - entries[n].tileId < entries[n].runLength) {
      return entries[n];
    }
  }
  return;
}

export function getUint64(v: DataView, offset: number): number {
  const wh = v.getUint32(offset + 4, true);
  const wl = v.getUint32(offset + 0, true);
  return wh * 2 ** 32 + wl;
}

/**
 * Parse raw header bytes into a Header object.
 */
export async function bytesToHeader(buff: ArrayBuffer): Promise<Header> {
  const v = new DataView(await new Response(buff).arrayBuffer());
  const specVersion = v.getUint8(7);
  if (specVersion !== 3) {
    throw Error(`Archive is spec version ${specVersion} but this code only supports version 3`);
  }

  return {
    specVersion: specVersion,
    rootDirectoryOffset: getUint64(v, 8),
    rootDirectoryLength: getUint64(v, 16),
    jsonMetadataOffset: getUint64(v, 24),
    jsonMetadataLength: getUint64(v, 32),
    leafDirectoryOffset: getUint64(v, 40),
    leafDirectoryLength: getUint64(v, 48),
    tileDataOffset: getUint64(v, 56),
    tileDataLength: getUint64(v, 64),
    numAddressedTiles: getUint64(v, 72),
    numTileEntries: getUint64(v, 80),
    numTileContents: getUint64(v, 88),
    clustered: v.getUint8(96) === 1,
    internalCompression: v.getUint8(97),
    tileCompression: v.getUint8(98),
    tileType: v.getUint8(99),
    minZoom: v.getUint8(100),
    maxZoom: v.getUint8(101),
    minLon: v.getInt32(102, true) / 10000000,
    minLat: v.getInt32(106, true) / 10000000,
    maxLon: v.getInt32(110, true) / 10000000,
    maxLat: v.getInt32(114, true) / 10000000,
    centerZoom: v.getUint8(118),
    centerLon: v.getInt32(119, true) / 10000000,
    centerLat: v.getInt32(123, true) / 10000000,
  };
}

export async function decompress(buf: ArrayBuffer, compression: Compression): Promise<ArrayBuffer> {
  if (compression === Compression.None || compression === Compression.Unknown) {
    return buf;
  }
  if (compression === Compression.Gzip) {
    const stream = new Response(buf).body;
    const result = stream?.pipeThrough(new DecompressionStream('gzip'));
    return new Response(result).arrayBuffer();
  }
  throw Error('Compression method not supported');
}

export function getHeaderCacheKey(archiveName: string): string {
  return archiveName;
}

export function getDirectoryCacheKey(
  fileName: string,
  fileHash: string,
  range: { offset: number; length: number },
): string {
  return `${fileName}|${fileHash}|${range.offset}|${range.length}`;
}
