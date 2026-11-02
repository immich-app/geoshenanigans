import type { RangeReader } from '../src/deploy/archive';
import { Compression, TileType } from '../src/pmtiles/types';

/** A tile entry as written to a directory. */
export type FixtureEntry = { tileId: number; offset: number; length: number; runLength: number };
/** A root entry pointing at leaves[leaf]. */
export type LeafPointer = { tileId: number; leaf: number };

export type FixtureOptions = {
  root: (FixtureEntry | LeafPointer)[];
  leaves?: FixtureEntry[][];
  tileData: Uint8Array;
  metadata?: object;
  tileCompression?: Compression;
  clustered?: boolean;
};

const HEADER_SIZE_BYTES = 127;

function writeVarint(out: number[], value: number) {
  while (value >= 0x80) {
    out.push((value & 0x7f) | 0x80);
    value = Math.floor(value / 128);
  }
  out.push(value);
}

// PMTiles v3 directory encoding: tile ID deltas, run lengths, lengths, then offsets (0 = right after the previous).
function serializeDirectory(entries: FixtureEntry[]): Uint8Array {
  const out: number[] = [];
  writeVarint(out, entries.length);
  let lastId = 0;
  for (const entry of entries) {
    writeVarint(out, entry.tileId - lastId);
    lastId = entry.tileId;
  }
  for (const entry of entries) {
    writeVarint(out, entry.runLength);
  }
  for (const entry of entries) {
    writeVarint(out, entry.length);
  }
  entries.forEach((entry, i) => {
    const previous = entries[i - 1];
    writeVarint(out, previous && entry.offset === previous.offset + previous.length ? 0 : entry.offset + 1);
  });
  return new Uint8Array(out);
}

async function gzip(bytes: Uint8Array): Promise<Uint8Array> {
  const stream = new Blob([bytes]).stream().pipeThrough(new CompressionStream('gzip'));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

/** Builds a PMTiles v3 archive: header, root directory, metadata, leaf directories, tile data. */
export async function buildArchive(options: FixtureOptions): Promise<Uint8Array> {
  const leaves = await Promise.all((options.leaves ?? []).map((leaf) => gzip(serializeDirectory(leaf))));
  const leafOffsets: number[] = [];
  let leafLength = 0;
  for (const leaf of leaves) {
    leafOffsets.push(leafLength);
    leafLength += leaf.length;
  }
  const rootEntries = options.root.map((entry) =>
    'leaf' in entry
      ? { tileId: entry.tileId, offset: leafOffsets[entry.leaf], length: leaves[entry.leaf].length, runLength: 0 }
      : entry,
  );
  const root = await gzip(serializeDirectory(rootEntries));
  const metadata = await gzip(new TextEncoder().encode(JSON.stringify(options.metadata ?? { name: 'fixture' })));
  const tileEntries = [...options.root, ...(options.leaves ?? []).flat()].filter(
    (entry): entry is FixtureEntry => !('leaf' in entry),
  );

  const rootOffset = HEADER_SIZE_BYTES;
  const metadataOffset = rootOffset + root.length;
  const leafOffset = metadataOffset + metadata.length;
  const dataOffset = leafOffset + leafLength;
  const archive = new Uint8Array(dataOffset + options.tileData.length);
  const view = new DataView(archive.buffer);
  archive.set(new TextEncoder().encode('PMTiles'), 0);
  view.setUint8(7, 3);
  const fields = [
    rootOffset,
    root.length,
    metadataOffset,
    metadata.length,
    leafOffset,
    leafLength,
    dataOffset,
    options.tileData.length,
    tileEntries.reduce((sum, entry) => sum + entry.runLength, 0),
    tileEntries.length,
    new Set(tileEntries.map((entry) => entry.offset)).size,
  ];
  fields.forEach((value, i) => view.setBigUint64(8 + i * 8, BigInt(value), true));
  view.setUint8(96, options.clustered === false ? 0 : 1);
  view.setUint8(97, Compression.Gzip);
  view.setUint8(98, options.tileCompression ?? Compression.Gzip);
  view.setUint8(99, TileType.Mvt);
  view.setUint8(101, 15);
  archive.set(root, rootOffset);
  archive.set(metadata, metadataOffset);
  leaves.forEach((leaf, i) => archive.set(leaf, leafOffset + leafOffsets[i]));
  archive.set(options.tileData, dataOffset);
  return archive;
}

/** Reads ranges of an in-memory archive, failing on reads past its end like a file would. */
export function memoryReader(bytes: Uint8Array): RangeReader {
  return async (offset, length) => {
    if (offset + length > bytes.length) {
      throw new Error(`Read of ${length} bytes at ${offset} is past the end (${bytes.length})`);
    }
    return bytes.slice(offset, offset + length).buffer as ArrayBuffer;
  };
}
