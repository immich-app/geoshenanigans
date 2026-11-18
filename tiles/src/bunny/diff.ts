/**
 * Compares two manifests position by position. The two builds' runs need not line up: the walk advances over
 * whichever run ends first, so a run split across several entries in the other build, runs that start or end at
 * different IDs and moved ocean boundaries all compare by what each position holds.
 */
import { Entry } from '../pmtiles/types';
import { ManifestBatch, manifestBatches, ManifestHeader, readManifestHeader } from './manifest';

/**
 * Consecutive positions of one kind: `changed` holds other bytes in the new build, `added` exists only in the new
 * build, `removed` only in the old one. Changed and added positions need uploading, removed ones deleting.
 */
export interface DiffRange {
  start: number;
  count: number;
  kind: DiffKind;
}

export type DiffKind = 'changed' | 'added' | 'removed';

export interface ZoomCounts {
  changed: number;
  added: number;
  removed: number;
}

export interface DiffTotals {
  oldBuild: string;
  newBuild: string;
  positionsOld: number;
  positionsNew: number;
  unchanged: number;
  changed: number;
  added: number;
  removed: number;
  ranges: number;
  /** Indexed by zoom. */
  byZoom: ZoomCounts[];
}

// First tile ID of each zoom (the PMTiles Hilbert numbering), up to zoom 26.
const ZOOM_STARTS: number[] = [];
for (let z = 0, start = 0; z <= 27; z++) {
  ZOOM_STARTS.push(start);
  start += 4 ** z;
}

export function zoomOf(tileId: number): number {
  let z = 0;
  while (z < 26 && ZOOM_STARTS[z + 1] <= tileId) {
    z++;
  }
  return z;
}

function countByZoom(byZoom: ZoomCounts[], kind: DiffKind, start: number, count: number) {
  let position = start;
  const end = start + count;
  while (position < end) {
    const z = zoomOf(position);
    const zoomEnd = Math.min(end, ZOOM_STARTS[z + 1]);
    while (byZoom.length <= z) {
      byZoom.push({ changed: 0, added: 0, removed: 0 });
    }
    byZoom[z][kind] += zoomEnd - position;
    position = zoomEnd;
  }
}

/** Reads one manifest a batch at a time; the current run is [start, end) with hash (hi, lo). */
class Cursor {
  private batch: ManifestBatch | undefined;
  private index = 0;
  done = false;
  start = 0;
  end = 0;
  hi = 0;
  lo = 0;

  constructor(private batches: AsyncIterator<ManifestBatch>) {}

  /** Moves to the next run; false when the batch is used up and load() must run first. */
  advance(): boolean {
    const batch = this.batch;
    if (!batch || this.index >= batch.count) {
      return false;
    }
    const i = this.index++;
    this.start = batch.tileIds[i];
    this.end = this.start + batch.runLengths[i];
    this.hi = batch.hashHi[i];
    this.lo = batch.hashLo[i];
    return true;
  }

  async load(): Promise<void> {
    const next = await this.batches.next();
    if (next.done) {
      this.done = true;
      this.batch = undefined;
    } else {
      this.batch = next.value;
      this.index = 0;
    }
  }

  /** Steps to the next run, loading batches as needed. */
  async step(): Promise<void> {
    while (!this.done && !this.advance()) {
      await this.load();
    }
  }

  async close() {
    await this.batches.return?.();
  }
}

/** The differences between two manifests, as ranges in tile-ID order. `totals` is complete once iteration ends. */
export class ManifestDiff implements AsyncIterable<DiffRange> {
  readonly totals: DiffTotals;
  private started = false;

  constructor(
    private oldPath: string,
    private newPath: string,
    oldHeader: ManifestHeader,
    newHeader: ManifestHeader,
  ) {
    this.totals = {
      oldBuild: oldHeader.buildKey,
      newBuild: newHeader.buildKey,
      positionsOld: oldHeader.positions,
      positionsNew: newHeader.positions,
      unchanged: 0,
      changed: 0,
      added: 0,
      removed: 0,
      ranges: 0,
      byZoom: [],
    };
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<DiffRange> {
    if (this.started) {
      throw new Error('A manifest diff can only be iterated once');
    }
    this.started = true;
    const totals = this.totals;
    const a = new Cursor(manifestBatches(this.oldPath));
    const b = new Cursor(manifestBatches(this.newPath));
    const out: DiffRange[] = [];
    let pending: DiffRange | undefined;
    const emit = (kind: DiffKind | undefined, start: number, count: number) => {
      if (!kind) {
        totals.unchanged += count;
        return;
      }
      totals[kind] += count;
      countByZoom(totals.byZoom, kind, start, count);
      if (pending && pending.kind === kind && pending.start + pending.count === start) {
        pending.count += count;
        return;
      }
      if (pending) {
        out.push(pending);
      }
      pending = { start, count, kind };
    };

    try {
      await a.step();
      await b.step();
      // Everything below `position` is accounted for; each cursor's run ends after it (or the cursor is done).
      let position = Math.min(a.done ? Infinity : a.start, b.done ? Infinity : b.start);
      while (!a.done || !b.done) {
        const inA = !a.done && a.start <= position;
        const inB = !b.done && b.start <= position;
        const nextA = a.done ? Infinity : inA ? a.end : a.start;
        const nextB = b.done ? Infinity : inB ? b.end : b.start;
        const next = Math.min(nextA, nextB);
        if (inA || inB) {
          const kind =
            inA && inB ? (a.hi === b.hi && a.lo === b.lo ? undefined : 'changed') : inA ? 'removed' : 'added';
          emit(kind, position, next - position);
        }
        position = next;
        if (!a.done && a.end <= position && !a.advance()) {
          await a.step();
        }
        if (!b.done && b.end <= position && !b.advance()) {
          await b.step();
        }
        if (out.length > 0) {
          totals.ranges += out.length;
          yield* out.splice(0);
        }
      }
      if (pending) {
        totals.ranges++;
        yield pending;
      }
    } finally {
      await Promise.all([a.close(), b.close()]);
    }
  }
}

export async function diffManifests(oldPath: string, newPath: string): Promise<ManifestDiff> {
  const [oldHeader, newHeader] = await Promise.all([readManifestHeader(oldPath), readManifestHeader(newPath)]);
  return new ManifestDiff(oldPath, newPath, oldHeader, newHeader);
}

export interface TilePosition {
  tileId: number;
  offset: number;
  length: number;
}

/**
 * The archive tile behind every changed and added position of the ranges, in tile-ID order: what the uploader
 * reads. `entries` is the new build's Archive.tileEntries(); ranges must come in tile-ID order, as a diff yields
 * them. Removed ranges are skipped.
 */
export async function* changedTiles(
  entries: AsyncIterable<Entry[]>,
  ranges: AsyncIterable<DiffRange> | Iterable<DiffRange>,
): AsyncGenerator<TilePosition> {
  const iterator = entries[Symbol.asyncIterator]();
  let run: Entry[] = [];
  let index = 0;
  let previousEnd = 0;
  try {
    for await (const range of ranges) {
      if (range.kind === 'removed') {
        continue;
      }
      if (range.start < previousEnd) {
        throw new Error(`Range at ${range.start} is out of order`);
      }
      const end = range.start + range.count;
      previousEnd = end;
      for (let position = range.start; position < end;) {
        while (index >= run.length || run[index].tileId + run[index].runLength <= position) {
          if (index < run.length) {
            index++;
            continue;
          }
          const next = await iterator.next();
          if (next.done) {
            throw new Error(`Tile ${position} is not in the archive`);
          }
          run = next.value;
          index = 0;
        }
        const entry = run[index];
        if (entry.tileId > position) {
          throw new Error(`Tile ${position} is not in the archive`);
        }
        const entryEnd = Math.min(end, entry.tileId + entry.runLength);
        for (; position < entryEnd; position++) {
          yield { tileId: position, offset: entry.offset, length: entry.length };
        }
      }
    }
  } finally {
    await iterator.return?.();
  }
}
