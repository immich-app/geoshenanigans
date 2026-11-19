import { createHash } from 'node:crypto';
import { open } from 'node:fs/promises';
import { Archive, RangeReader } from '../deploy/archive';
import { Entry } from '../pmtiles/types';
import { decompress, deserializeIndex } from '../pmtiles/utils';
import { FileSnapshot, fileSnapshot } from './archive-identity-worker';

/** Positions [tileId, tileId + runLength) all hold the tile bytes at (offset, length) of the tile data section. */
export interface TileRun {
  tileId: number;
  runLength: number;
  offset: number;
  length: number;
}

const HEADER_SIZE_BYTES = 127;
const MAX_DEPTH = 3;
/** A planet leaf holds ~61k entries, ~10 MiB as objects; lookups come in tile-id order, so few are needed. */
const LEAF_CACHE_SIZE = 16;

/**
 * A PMTiles archive on disk walked by tile-id range: leaves outside the range are never read, so a job that starts
 * deep into z15 does not decode every leaf before it.
 */
export class ArchiveRanges {
  /**
   * Leaves by offset, least recently used first, kept while they load too, so concurrent find()s in one leaf read and
   * decode it once.
   */
  private leafCache = new Map<number, Promise<Entry[]>>();

  private constructor(
    readonly archive: Archive,
    readonly path: string,
    readonly size: number,
    /**
     * The v1 archive fingerprint seeds recorded before the v2 identity (archive-identity.ts): the header, root
     * directory and size only, so archives with other tile bytes can share it. Only for checking a seed state an
     * older bunny:seed left, together with the whole file's MD5.
     */
    readonly legacyFingerprint: string,
    /** The file this reads, as it was when opened (see FileSnapshot). */
    readonly source: FileSnapshot,
    private read: RangeReader,
    private root: Entry[],
    private closeFile: () => Promise<void>,
  ) {}

  /**
   * Opens the archive at `path`; given `source` (from identifyArchive), only if the file it opened is that one,
   * unchanged, so what it reads is what the identity is of.
   */
  static async open(path: string, source?: FileSnapshot): Promise<ArchiveRanges> {
    const file = await open(path);
    const read: RangeReader = async (position, length) => {
      const buffer = Buffer.alloc(length);
      let done = 0;
      while (done < length) {
        const { bytesRead } = await file.read(buffer, done, length - done, position + done);
        if (bytesRead === 0) {
          throw new Error(`Unexpected end of ${path} reading ${length} bytes at ${position}`);
        }
        done += bytesRead;
      }
      return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + length) as ArrayBuffer;
    };
    try {
      const stats = await file.stat({ bigint: true });
      const opened = fileSnapshot(stats);
      if (source !== undefined && opened !== source) {
        throw new Error('it is no longer the file its identity was computed from: it was replaced or written to');
      }
      const archive = await Archive.open(read);
      const { header } = archive;
      const size = Number(stats.size);
      const rootBytes = await read(header.rootDirectoryOffset, header.rootDirectoryLength);
      const root = deserializeIndex(await decompress(rootBytes, header.internalCompression));
      const legacyFingerprint = createHash('sha256')
        .update(new Uint8Array(await read(0, HEADER_SIZE_BYTES)))
        .update(new Uint8Array(rootBytes))
        .update(String(size))
        .digest('hex');
      return new ArchiveRanges(archive, path, size, legacyFingerprint, opened, read, root, () => file.close());
    } catch (error) {
      await file.close();
      throw new Error(`${path}: ${error instanceof Error ? error.message : String(error)}`, { cause: error });
    }
  }

  get tileDataOffset(): number {
    return this.archive.header.tileDataOffset;
  }

  /** Every run that overlaps [from, to), whole (not clipped to the range), in tile-id order. */
  runs(from: number, to: number): AsyncGenerator<TileRun> {
    return this.walk(this.root, from, to, Number.POSITIVE_INFINITY, 0);
  }

  /** The ids of the archive's first tile and one past its last, or null if it has none. */
  async bounds(): Promise<{ first: number; end: number } | null> {
    const first = await this.edge('first');
    const last = await this.edge('last');
    return first && last ? { first: first.tileId, end: last.tileId + last.runLength } : null;
  }

  /** The run holding `tileId`, or null if the archive has no tile there. */
  async find(tileId: number): Promise<TileRun | null> {
    let entries = this.root;
    for (let depth = 0; depth <= MAX_DEPTH; depth++) {
      const entry = lastAtOrBefore(entries, tileId);
      if (!entry) {
        return null;
      }
      if (entry.runLength > 0) {
        return tileId < entry.tileId + entry.runLength ? toRun(entry) : null;
      }
      entries = await this.leaf(entry);
    }
    throw new Error('Directory nesting too deep');
  }

  close(): Promise<void> {
    return this.closeFile();
  }

  private async edge(which: 'first' | 'last'): Promise<TileRun | null> {
    let entries = this.root;
    for (let depth = 0; depth <= MAX_DEPTH; depth++) {
      const entry = which === 'first' ? entries[0] : entries.at(-1);
      if (!entry) {
        return null;
      }
      if (entry.runLength > 0) {
        return toRun(entry);
      }
      entries = await this.leaf(entry);
    }
    throw new Error('Directory nesting too deep');
  }

  private async *walk(entries: Entry[], from: number, to: number, end: number, depth: number): AsyncGenerator<TileRun> {
    if (depth > MAX_DEPTH) {
      throw new Error('Directory nesting too deep');
    }
    for (let i = 0; i < entries.length; i++) {
      const entry = entries[i];
      if (entry.tileId >= to) {
        return;
      }
      // A leaf covers ids up to the next entry; a run ends no later than that.
      const next = i + 1 < entries.length ? entries[i + 1].tileId : end;
      if (next <= from) {
        continue;
      }
      if (entry.runLength === 0) {
        yield* this.walk(await this.leaf(entry), from, to, next, depth + 1);
      } else if (entry.tileId + entry.runLength > from) {
        yield toRun(entry);
      }
    }
  }

  private leaf(entry: Entry): Promise<Entry[]> {
    const cached = this.leafCache.get(entry.offset);
    if (cached) {
      this.leafCache.delete(entry.offset);
      this.leafCache.set(entry.offset, cached);
      return cached;
    }
    const loading = this.loadLeaf(entry);
    if (this.leafCache.size >= LEAF_CACHE_SIZE) {
      this.leafCache.delete(this.leafCache.keys().next().value!);
    }
    this.leafCache.set(entry.offset, loading);
    // A failed read is not kept: the next caller tries again.
    loading.catch(() => {
      if (this.leafCache.get(entry.offset) === loading) {
        this.leafCache.delete(entry.offset);
      }
    });
    return loading;
  }

  private async loadLeaf(entry: Entry): Promise<Entry[]> {
    const { header } = this.archive;
    const bytes = await this.read(header.leafDirectoryOffset + entry.offset, entry.length);
    const entries = deserializeIndex(await decompress(bytes, header.internalCompression));
    if (entries.length === 0) {
      throw new Error(`Empty leaf directory at offset ${entry.offset}`);
    }
    return entries;
  }
}

function toRun(entry: Entry): TileRun {
  return { tileId: entry.tileId, runLength: entry.runLength, offset: entry.offset, length: entry.length };
}

function lastAtOrBefore(entries: Entry[], tileId: number): Entry | undefined {
  let lo = 0;
  let hi = entries.length - 1;
  let found: Entry | undefined;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (entries[mid].tileId <= tileId) {
      found = entries[mid];
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return found;
}
