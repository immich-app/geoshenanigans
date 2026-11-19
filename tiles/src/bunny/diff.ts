/**
 * Compares two manifests position by position. The two builds' runs need not line up: the walk advances over
 * whichever run ends first, so a run split across several entries in the other build, runs that start or end at
 * different IDs and moved ocean boundaries all compare by what each position holds.
 */
import { ManifestBatch, manifestBatches, ManifestHeader, readManifestHeader } from './manifest';
import { tileIdZoom, zoomEnd } from './tile-id';

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

/** The zoom of a tile ID; throws for an ID past zoom MAX_SAFE_ZOOM (26). */
export function zoomOf(tileId: number): number {
  return tileIdZoom(tileId);
}

/** Ranges come from manifests, whose reader refuses positions past zoom MAX_SAFE_ZOOM (manifestBatches). */
function countByZoom(byZoom: ZoomCounts[], kind: DiffKind, start: number, count: number) {
  const end = start + count;
  let position = start;
  while (position < end) {
    const z = tileIdZoom(position);
    const stop = Math.min(end, zoomEnd(z));
    while (byZoom.length <= z) {
      byZoom.push({ changed: 0, added: 0, removed: 0 });
    }
    byZoom[z][kind] += stop - position;
    position = stop;
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

/**
 * A range of a sync plan. `changed` and `added` positions get the new build's bytes, `removed` ones are deleted.
 * With `verify`, a sync that failed part-way may have written there already, so the file is DESCRIBEd first and only
 * written when its checksum isn't the new one: re-PUTting identical bytes clears Bunny's replica list.
 */
export interface SyncRange extends DiffRange {
  verify: boolean;
}

export interface SyncPlanTotals {
  /** Live to new, as diffManifests counts it; complete once iteration ends. */
  diff: DiffTotals;
  pendingBuilds: string[];
  /** Positions to DESCRIBE before writing. */
  verify: number;
  /** Of those, positions where the live and new builds agree, so only a pending build can have changed them. */
  verifyOnly: number;
  ranges: number;
}

/** Steps through the ranges of a diff; the current one is [start, end). */
class RangeCursor {
  done = false;
  start = 0;
  end = 0;
  kind: DiffKind = 'changed';

  constructor(private ranges: AsyncIterator<DiffRange>) {}

  async next() {
    const next = await this.ranges.next();
    if (next.done) {
      this.done = true;
    } else {
      this.start = next.value.start;
      this.end = next.value.start + next.value.count;
      this.kind = next.value.kind;
    }
  }

  async close() {
    await this.ranges.return?.();
  }
}

/**
 * What a sync must write to take the tiles zone from the live build to a new one, when earlier syncs may have
 * written part of other builds (state.json's `pending`) without making them live. Outside the positions where a
 * pending build differs from the live one the zone holds the live build, so the live-to-new diff applies as is.
 * Inside them the zone holds the live or a pending build's bytes, so every such position is planned with `verify`:
 * those the live-to-new diff lists keep its kind; the others hold the same bytes in the live and new builds (or
 * neither has them), so they are `changed` back to them (or `removed`, where only a pending build had a file).
 */
export class SyncPlan implements AsyncIterable<SyncRange> {
  readonly totals: SyncPlanTotals;
  private started = false;

  constructor(
    private diff: ManifestDiff,
    private pending: ManifestDiff[],
  ) {
    this.totals = {
      diff: diff.totals,
      pendingBuilds: pending.map((p) => p.totals.newBuild),
      verify: 0,
      verifyOnly: 0,
      ranges: 0,
    };
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<SyncRange> {
    if (this.started) {
      throw new Error('A sync plan can only be iterated once');
    }
    this.started = true;
    const totals = this.totals;
    const live = new RangeCursor(this.diff[Symbol.asyncIterator]());
    const pending = this.pending.map((diff) => new RangeCursor(diff[Symbol.asyncIterator]()));
    const cursors = [live, ...pending];
    const out: SyncRange[] = [];
    let current: SyncRange | undefined;
    const emit = (kind: DiffKind, verify: boolean, start: number, count: number) => {
      if (verify) {
        totals.verify += count;
      }
      if (current && current.kind === kind && current.verify === verify && current.start + current.count === start) {
        current.count += count;
        return;
      }
      if (current) {
        out.push(current);
      }
      current = { start, count, kind, verify };
    };

    try {
      await Promise.all(cursors.map((cursor) => cursor.next()));
      let position = Math.min(...cursors.map((cursor) => (cursor.done ? Infinity : cursor.start)));
      while (cursors.some((cursor) => !cursor.done)) {
        let next = Infinity;
        let touched: DiffKind | undefined;
        for (const cursor of cursors) {
          if (!cursor.done) {
            const inside = cursor.start <= position;
            next = Math.min(next, inside ? cursor.end : cursor.start);
            if (inside && cursor !== live) {
              touched = cursor.kind;
            }
          }
        }
        const listed = !live.done && live.start <= position;
        if (listed) {
          emit(live.kind, touched !== undefined, position, next - position);
        } else if (touched) {
          // Relative to the live build: 'added' means it has no file here, so neither has the new one.
          emit(touched === 'added' ? 'removed' : 'changed', true, position, next - position);
          totals.verifyOnly += next - position;
        }
        position = next;
        for (const cursor of cursors) {
          while (!cursor.done && cursor.end <= position) {
            await cursor.next();
          }
        }
        if (out.length > 0) {
          totals.ranges += out.length;
          yield* out.splice(0);
        }
      }
      if (current) {
        totals.ranges++;
        yield current;
      }
    } finally {
      await Promise.all(cursors.map((cursor) => cursor.close()));
    }
  }
}

/**
 * Plans a sync from the live build's manifest to a new one, given the manifests of the builds state.json lists as
 * pending (including the new build itself when this is a re-run). Without pending builds it is the plain diff.
 */
export async function planSync(livePath: string, newPath: string, pendingPaths: string[] = []): Promise<SyncPlan> {
  const [diff, ...pending] = await Promise.all([
    diffManifests(livePath, newPath),
    ...pendingPaths.map((path) => diffManifests(livePath, path)),
  ]);
  return new SyncPlan(diff, pending);
}
