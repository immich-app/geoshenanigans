import { createHash } from 'node:crypto';
import { copyFile, mkdtemp, open, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ArchiveRanges } from '../../src/bunny/archive-walk';
import { buildArchive, FixtureEntry, LeafPointer } from '../pmtiles-fixture';

export interface FixtureArchive {
  path: string;
  /** One past the last tile id: every position below it has a tile. */
  positions: number;
  /** Expected uppercase SHA-256 for a tile id. */
  sha256(tileId: number): string;
  entries: number;
}

/** A small deterministic PRNG so fixtures and fault schedules repeat run to run. */
export function random(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/**
 * Writes an archive with every position in [0, positions): ocean runs sharing one 95-byte tile, unique tiles,
 * a few repeats of earlier tiles (deduplicated by offset), spread over leaf directories of `leafSize` entries.
 */
export async function writeFixtureArchive(dir: string, positions: number, seed = 1, leafSize = 200) {
  const next = random(seed);
  const chunks: Uint8Array[] = [];
  let dataLength = 0;
  const add = (bytes: Uint8Array) => {
    const offset = dataLength;
    chunks.push(bytes);
    dataLength += bytes.length;
    return { offset, length: bytes.length };
  };
  const ocean = add(new Uint8Array(95).fill(7));
  const uniques: { offset: number; length: number }[] = [];
  const entries: FixtureEntry[] = [];
  for (let id = 0; id < positions;) {
    const roll = next();
    if (roll < 0.15) {
      const runLength = Math.min(positions - id, 1 + Math.floor(next() * 300));
      entries.push({ tileId: id, runLength, ...ocean });
      id += runLength;
    } else if (roll < 0.2 && uniques.length > 0) {
      entries.push({ tileId: id, runLength: 1, ...uniques[Math.floor(next() * uniques.length)] });
      id++;
    } else {
      const bytes = new Uint8Array(20 + Math.floor(next() * 400));
      for (let i = 0; i < bytes.length; i++) {
        bytes[i] = Math.floor(next() * 256);
      }
      const tile = add(bytes);
      uniques.push(tile);
      entries.push({ tileId: id, runLength: 1, ...tile });
      id++;
    }
  }
  const leaves: FixtureEntry[][] = [];
  for (let i = 0; i < entries.length; i += leafSize) {
    leaves.push(entries.slice(i, i + leafSize));
  }
  const root: LeafPointer[] = leaves.map((leaf, i) => ({ tileId: leaf[0].tileId, leaf: i }));
  const tileData = new Uint8Array(dataLength);
  let at = 0;
  for (const chunk of chunks) {
    tileData.set(chunk, at);
    at += chunk.length;
  }
  const path = join(dir, `fixture-${positions}-${seed}.pmtiles`);
  await writeFile(path, await buildArchive({ root, leaves, tileData }));

  const sha = new Map<number, string>();
  const hashes = new Map<number, string>();
  const shaOf = (offset: number, length: number) => {
    let value = hashes.get(offset);
    if (!value) {
      value = createHash('sha256')
        .update(tileData.subarray(offset, offset + length))
        .digest('hex')
        .toUpperCase();
      hashes.set(offset, value);
    }
    return value;
  };
  for (const entry of entries) {
    const value = shaOf(entry.offset, entry.length);
    for (let id = entry.tileId; id < entry.tileId + entry.runLength; id++) {
      sha.set(id, value);
    }
  }
  return {
    path,
    positions,
    entries: entries.length,
    sha256: (tileId: number) => sha.get(tileId)!,
  } satisfies FixtureArchive;
}

/** A scratch dir under the OS temp dir (set TMPDIR to keep it off a RAM-backed /tmp). */
export async function scratchDir(): Promise<{ dir: string; remove: () => Promise<void> }> {
  const dir = await mkdtemp(join(tmpdir(), 'bunny-test-'));
  return { dir, remove: () => rm(dir, { recursive: true, force: true }) };
}

/**
 * Copies the archive at `path` to `copy` with one byte of tile data changed: the header, root directory and size stay
 * the same (and with them the v1 fingerprint), the tile bytes don't.
 */
export async function withOtherTileBytes(path: string, copy: string): Promise<void> {
  const ranges = await ArchiveRanges.open(path);
  const at = ranges.tileDataOffset + 3;
  await ranges.close();
  await copyFile(path, copy);
  const file = await open(copy, 'r+');
  try {
    const byte = Buffer.alloc(1);
    await file.read(byte, 0, 1, at);
    byte[0] ^= 0xff;
    await file.write(byte, 0, 1, at);
  } finally {
    await file.close();
  }
}
