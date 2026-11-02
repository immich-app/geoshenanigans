import { DirectoryString } from '../pmtiles.service';
import { Entry } from '../pmtiles/types';

/**
 * Deployed layout: tile data is split into ~1 MB objects (`chunk_<id>.pmtiles`, id = floor(offset / 1 MB) of the
 * chunk's first tile), and each D1 row holds up to 50 directory entries, keyed by its first tile ID, pointing into
 * those chunks.
 */
export const CHUNK_SIZE_BYTES = 1_000_000;
export const ENTRIES_PER_ROW = 50;

export type Chunk = { chunkId: number; startByte: number; endByte: number };
export type DirectoryRow = { startTileId: number; entry: string };

/**
 * Assigns tiles to chunks while walking a clustered archive's entries in order. A chunk is handed to `onComplete`
 * once a later tile starts a new chunk, i.e. when nothing can extend it any more.
 */
export class ChunkPlanner {
  private chunks = new Map<number, Chunk>();
  private current: Chunk | undefined;

  constructor(private onComplete: (chunk: Chunk) => void) {}

  /** Places a tile and returns its chunk and offset within that chunk. */
  place(entry: Pick<Entry, 'offset' | 'length'>): { chunkId: number; offset: number } {
    const chunkId = Math.floor(entry.offset / CHUNK_SIZE_BYTES);
    const endByte = entry.offset + entry.length;

    // Deduplicated tiles point back at data that an earlier, already complete chunk holds.
    if (this.current && chunkId < this.current.chunkId) {
      const chunk = this.chunks.get(chunkId);
      if (!chunk || entry.offset < chunk.startByte || endByte > chunk.endByte) {
        throw new Error(`Tile at ${entry.offset}+${entry.length} points outside completed chunk ${chunkId}`);
      }
      return { chunkId, offset: entry.offset - chunk.startByte };
    }

    if (chunkId !== this.current?.chunkId) {
      if (this.current) {
        this.onComplete(this.current);
      }
      this.current = { chunkId, startByte: entry.offset, endByte };
      this.chunks.set(chunkId, this.current);
    }

    if (entry.offset < this.current.startByte) {
      throw new Error(`Tile at ${entry.offset} starts before chunk ${chunkId} (${this.current.startByte})`);
    }
    this.current.endByte = Math.max(this.current.endByte, endByte);
    return { chunkId, offset: entry.offset - this.current.startByte };
  }

  /** Completes the last chunk. */
  finish() {
    if (this.current) {
      this.onComplete(this.current);
      this.current = undefined;
    }
  }

  get chunkCount() {
    return this.chunks.size;
  }
}

/** Places a run of directory entries and packs them into D1 rows. */
export function toDirectoryRows(entries: Entry[], planner: ChunkPlanner): DirectoryRow[] {
  const rows: DirectoryRow[] = [];
  for (let start = 0; start < entries.length; start += ENTRIES_PER_ROW) {
    const rowEntries = entries.slice(start, start + ENTRIES_PER_ROW).map((entry) => ({
      tileId: entry.tileId,
      length: entry.length,
      runLength: entry.runLength,
      ...planner.place(entry),
    }));
    const entry = DirectoryString.fromDirectory({
      tileIdStart: rowEntries[0].tileId,
      offsetStart: rowEntries[0].offset,
      entries: rowEntries,
    }).toString();
    rows.push({ startTileId: rowEntries[0].tileId, entry });
  }
  return rows;
}
