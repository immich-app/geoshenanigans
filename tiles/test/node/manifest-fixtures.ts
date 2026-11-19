import { createHash } from 'node:crypto';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { gzipSync } from 'node:zlib';
import { ManifestEntry } from '../../src/bunny/manifest';

/** Small seeded PRNG (mulberry32), so a failing property case can be replayed from its seed. */
export function rng(seed: number) {
  let a = seed >>> 0;
  const next = () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  return {
    next,
    int: (min: number, max: number) => min + Math.floor(next() * (max - min + 1)),
    chance: (p: number) => next() < p,
  };
}
export type Rng = ReturnType<typeof rng>;

/** Ids from here on are big tiles (4 MB and up), larger than a worker's read slab. */
export const BIG_CONTENT = 9_000_000;

/** Tile bytes for a content id: deterministic, 1 B to ~3 KB (or big ones). Id 0 is the 95-byte "ocean" tile. */
const contentCache = new Map<number, Uint8Array>();
const hashCache = new Map<number, string>();

export function contentBytes(id: number): Uint8Array {
  const cached = contentCache.get(id);
  if (cached) {
    return cached;
  }
  const r = rng(id * 7919 + 17);
  const size = id === 0 ? 95 : id >= BIG_CONTENT ? 4_000_000 + (id - BIG_CONTENT) * 300_000 : r.int(1, 3000);
  const bytes = new Uint8Array(size);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = r.int(0, 255);
  }
  bytes[0] = id & 0xff;
  contentCache.set(id, bytes);
  return bytes;
}

export function contentHash(id: number): string {
  let hash = hashCache.get(id);
  if (!hash) {
    hash = createHash('sha256').update(contentBytes(id)).digest('hex').slice(0, 16).toUpperCase();
    hashCache.set(id, hash);
  }
  return hash;
}

/** Content per position (index = tileId - base); null where the archive has no tile. */
export interface Build {
  base: number;
  contents: (number | null)[];
}

/** The manifest a build must produce: maximal runs of positions with the same bytes. */
export function expectedEntries(build: Build): ManifestEntry[] {
  const entries: ManifestEntry[] = [];
  build.contents.forEach((content, i) => {
    if (content === null) {
      return;
    }
    const hash = contentHash(content);
    const last = entries[entries.length - 1];
    if (last && last.tileId + last.runLength === build.base + i && last.hash === hash) {
      last.runLength++;
    } else {
      entries.push({ tileId: build.base + i, runLength: 1, hash });
    }
  });
  return entries;
}

export interface LayoutOptions {
  /** Chance of cutting a run of equal content into separate entries at each position. */
  splitChance?: number;
  /** Entries per leaf directory (random between 1 and this). */
  maxLeafEntries?: number;
  /** Entries kept in the root directory before the first leaf. */
  rootTiles?: number;
  /** Chance that a leaf is split into nested leaves one level down. */
  nestChance?: number;
  /** Chance of writing an already-seen content again at a new offset (same bytes, another offset). */
  duplicateChance?: number;
  minZoom?: number;
  maxZoom?: number;
  metadata?: object;
}

interface Entry {
  tileId: number;
  offset: number;
  length: number;
  runLength: number;
}

function writeVarint(out: number[], value: number) {
  while (value >= 0x80) {
    out.push((value % 0x80) | 0x80);
    value = Math.floor(value / 0x80);
  }
  out.push(value);
}

function serialize(entries: Entry[]): Buffer {
  const out: number[] = [];
  writeVarint(out, entries.length);
  let lastId = 0;
  for (const entry of entries) {
    writeVarint(out, entry.tileId - lastId);
    lastId = entry.tileId;
  }
  entries.forEach((entry) => writeVarint(out, entry.runLength));
  entries.forEach((entry) => writeVarint(out, entry.length));
  entries.forEach((entry, i) => {
    const previous = entries[i - 1];
    writeVarint(out, previous && entry.offset === previous.offset + previous.length ? 0 : entry.offset + 1);
  });
  return gzipSync(Buffer.from(out));
}

export interface GeneratedArchive {
  bytes: Buffer;
  /** Distinct (offset, length) tiles in the data section. */
  tileContents: number;
  entries: number;
  leaves: number;
  /** Byte offset of each position's tile in the data section (null when absent). */
  offsets: (number | null)[];
}

/**
 * A clustered PMTiles v3 archive of the build: tile data in order of first use, repeats pointing back (the ocean
 * tile, mostly), runs randomly cut into several entries, leaves of random sizes, some nested.
 */
export function generateArchive(build: Build, r: Rng, options: LayoutOptions = {}): GeneratedArchive {
  const entries: Entry[] = [];
  const data: Buffer[] = [];
  let dataLength = 0;
  const offsetOf = new Map<number, number>();
  const offsets: (number | null)[] = build.contents.map(() => null);
  for (let i = 0; i < build.contents.length;) {
    const content = build.contents[i];
    if (content === null) {
      i++;
      continue;
    }
    let end = i + 1;
    while (end < build.contents.length && build.contents[end] === content && !r.chance(options.splitChance ?? 0.2)) {
      end++;
    }
    let offset = offsetOf.get(content);
    const bytes = contentBytes(content);
    if (offset === undefined || r.chance(options.duplicateChance ?? 0)) {
      offset = dataLength;
      data.push(Buffer.from(bytes));
      dataLength += bytes.length;
      if (!offsetOf.has(content)) {
        offsetOf.set(content, offset);
      }
    }
    entries.push({ tileId: build.base + i, offset, length: bytes.length, runLength: end - i });
    for (let p = i; p < end; p++) {
      offsets[p] = offset;
    }
    i = end;
  }
  const tileContents = new Set(entries.map((entry) => `${entry.offset}:${entry.length}`)).size;

  // Directories: root tiles first, then leaves; a nested leaf's directory holds pointers to its sub-leaves.
  const leafBlobs: Buffer[] = [];
  let leafLength = 0;
  const addLeaf = (blob: Buffer) => {
    const offset = leafLength;
    leafBlobs.push(blob);
    leafLength += blob.length;
    return { offset, length: blob.length };
  };
  const rootTiles = Math.min(options.rootTiles ?? 0, entries.length);
  const root: Entry[] = entries.slice(0, rootTiles);
  let leaves = 0;
  for (let i = rootTiles; i < entries.length;) {
    const size = r.int(1, options.maxLeafEntries ?? 8);
    const chunk = entries.slice(i, i + size);
    i += size;
    let pointer: { offset: number; length: number };
    if (chunk.length > 1 && r.chance(options.nestChance ?? 0)) {
      const cut = r.int(1, chunk.length - 1);
      const parts = [chunk.slice(0, cut), chunk.slice(cut)];
      const subs = parts.map((part) => ({ part, at: addLeaf(serialize(part)) }));
      leaves += 2;
      pointer = addLeaf(serialize(subs.map(({ part, at }) => ({ tileId: part[0].tileId, ...at, runLength: 0 }))));
    } else {
      pointer = addLeaf(serialize(chunk));
      leaves++;
    }
    root.push({ tileId: chunk[0].tileId, ...pointer, runLength: 0 });
  }

  const rootBlob = serialize(root);
  const metadataBlob = gzipSync(Buffer.from(JSON.stringify(options.metadata ?? { name: 'generated' })));
  const rootOffset = 127;
  const metadataOffset = rootOffset + rootBlob.length;
  const leafOffset = metadataOffset + metadataBlob.length;
  const dataOffset = leafOffset + leafLength;
  const header = Buffer.alloc(127);
  header.write('PMTiles', 0, 'latin1');
  header[7] = 3;
  const fields = [
    rootOffset,
    rootBlob.length,
    metadataOffset,
    metadataBlob.length,
    leafOffset,
    leafLength,
    dataOffset,
    dataLength,
    entries.reduce((sum, entry) => sum + entry.runLength, 0),
    entries.length,
    tileContents,
  ];
  fields.forEach((value, i) => header.writeBigUInt64LE(BigInt(value), 8 + i * 8));
  header[96] = 1; // clustered
  header[97] = 2; // internal gzip
  header[98] = 2; // tiles gzip
  header[99] = 1; // MVT
  header[100] = options.minZoom ?? 0;
  header[101] = options.maxZoom ?? 15;
  return {
    bytes: Buffer.concat([header, rootBlob, metadataBlob, ...leafBlobs, ...data]),
    tileContents,
    entries: entries.length,
    leaves,
    offsets,
  };
}

/** A random build: mostly ocean (id 0) in long stretches, land of a few repeated and many unique tiles, some gaps. */
export function randomBuild(r: Rng, length: number, base = 0, gapChance = 0): Build {
  const contents: (number | null)[] = [];
  let next = 1;
  while (contents.length < length) {
    const stretch = r.int(1, 60);
    const kind = r.next();
    for (let i = 0; i < stretch && contents.length < length; i++) {
      if (r.chance(gapChance)) {
        contents.push(null);
      } else if (kind < 0.45) {
        contents.push(0);
      } else if (kind < 0.55) {
        contents.push(r.int(1, 5));
      } else {
        contents.push(1000 + next++);
      }
    }
  }
  return { base, contents };
}

/** The next day: some tiles change, ocean boundaries move, a few positions appear or disappear. */
export function mutateBuild(r: Rng, build: Build, gapChance = 0): Build {
  const contents = [...build.contents];
  const edits = r.int(0, 12);
  let fresh = 1_000_000 + r.int(0, 1_000_000);
  for (let e = 0; e < edits; e++) {
    const start = r.int(0, contents.length - 1);
    const end = Math.min(contents.length, start + r.int(1, 80));
    const kind = r.next();
    for (let p = start; p < end; p++) {
      if (kind < 0.3) {
        contents[p] = fresh++;
      } else if (kind < 0.55) {
        contents[p] = 0;
      } else if (kind < 0.7) {
        // Shift a boundary: copy the neighbour's content over.
        contents[p] = contents[Math.max(0, start - 1)];
      } else if (kind < 0.8 && gapChance > 0) {
        contents[p] = r.chance(0.5) ? null : fresh++;
      } else if (kind < 0.9) {
        contents[p] = r.int(1, 5);
      }
    }
  }
  return { base: build.base, contents };
}

export async function scratchDir() {
  const dir = await mkdtemp(join(tmpdir(), 'bunny-manifest-'));
  return { dir, path: (name: string) => join(dir, name), cleanup: () => rm(dir, { recursive: true, force: true }) };
}

export async function writeArchiveFile(path: string, build: Build, r: Rng, options?: LayoutOptions) {
  const archive = generateArchive(build, r, options);
  await writeFile(path, archive.bytes);
  return archive;
}
