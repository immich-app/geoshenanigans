/**
 * Replication checks through DESCRIBE, the only way to see replicas: regional storage endpoints refuse a zone whose
 * main region is elsewhere. Each stored file reports its SHA-256 and the replica regions holding it.
 */
import { setTimeout as sleep } from 'node:timers/promises';
import { BunnyStorage, StorageError, StorageObject } from './storage';
import { tileIdZoom, tilePath } from './tile-id';

/** The tiles zone's replica regions (the main region, DE, is not listed in ReplicatedZones). */
export const REPLICATION_REGIONS = [
  'UK',
  'SE',
  'ES',
  'CZ',
  'NY',
  'MI',
  'LA',
  'WA',
  'JP',
  'HK',
  'SG',
  'SYD',
  'BR',
  'JH',
];

export interface ReplicationItem {
  path: string;
  /**
   * Hex SHA-256 the file must hold, or its first 16 hex digits or more (a manifest's hash: the first 8 bytes of the
   * SHA-256, which DESCRIBE's Checksum starts with).
   */
  sha256: string;
  /** For per-zoom reports. */
  tileId?: number;
}

/** Whether a stored checksum is the expected SHA-256, or starts with an expected prefix of at least 16 hex digits. */
export function checksumMatches(stored: string, expected: string): boolean {
  const a = stored.toUpperCase();
  const b = expected.toUpperCase();
  return a === b || (b.length >= 16 && b.length < a.length && a.startsWith(b));
}

/** The files a replication gate waits for, by index. */
export interface GateItems {
  readonly length: number;
  path(index: number): string;
  sha256(index: number): string;
}

const GATE_INITIAL_CAPACITY = 1024;

/**
 * Tile files for the gate, compactly: 8 bytes of tile ID and 32 of SHA-256 each, in typed arrays outside the JS heap.
 * A forced sync gates tens of millions of files; as objects with path and hex strings they took ~450 bytes each and
 * ran Node out of its default heap at about 14M. Paths are built when a file is DESCRIBEd.
 */
export class TileGateList implements GateItems {
  length = 0;
  private ids = new Float64Array(GATE_INITIAL_CAPACITY);
  private digests = new Uint8Array(GATE_INITIAL_CAPACITY * 32);

  constructor(readonly prefix: string) {}

  /** Bytes held per file, for tests that bound the gate's memory. */
  static readonly BYTES_PER_FILE = 8 + 32;

  push(tileId: number, sha256Hex: string) {
    if (!/^[0-9a-fA-F]{64}$/.test(sha256Hex)) {
      throw new Error(`Not a hex SHA-256: ${sha256Hex}`);
    }
    if (this.length === this.ids.length) {
      const ids = new Float64Array(this.ids.length * 2);
      ids.set(this.ids);
      this.ids = ids;
      const digests = new Uint8Array(this.digests.length * 2);
      digests.set(this.digests);
      this.digests = digests;
    }
    this.ids[this.length] = tileId;
    this.digests.set(Buffer.from(sha256Hex, 'hex'), this.length * 32);
    this.length++;
  }

  tileId(index: number): number {
    return this.ids[index];
  }

  path(index: number): string {
    return tilePath(this.prefix, this.ids[index]);
  }

  sha256(index: number): string {
    const offset = this.digests.byteOffset + index * 32;
    return Buffer.from(this.digests.buffer, offset, 32).toString('hex').toUpperCase();
  }
}

/** ReplicationItem[] as GateItems. */
export function gateItems(items: ReplicationItem[]): GateItems {
  return { length: items.length, path: (index) => items[index].path, sha256: (index) => items[index].sha256 };
}

export type FileState = 'replicated' | 'partial' | 'missing' | 'mismatched';

/** What one DESCRIBE says about a file, and which of `regions` lack it. */
export function classify(
  object: StorageObject | null,
  sha256: string,
  regions: string[],
): { state: FileState; missingRegions: string[] } {
  if (!object) {
    return { state: 'missing', missingRegions: regions };
  }
  if (!checksumMatches(object.checksum, sha256)) {
    return { state: 'mismatched', missingRegions: regions };
  }
  const held = new Set(object.replicatedZones);
  const missingRegions = regions.filter((region) => !held.has(region));
  return { state: missingRegions.length === 0 ? 'replicated' : 'partial', missingRegions };
}

/**
 * Runs `fn` over every item with at most `concurrency` at once, pulling lazily from the source. The first error
 * stops taking items and is thrown once the calls in flight are done.
 */
export async function forEachLimit<T>(
  source: AsyncIterable<T> | Iterable<T>,
  concurrency: number,
  fn: (item: T) => Promise<void>,
  signal?: AbortSignal,
): Promise<void> {
  const iterator =
    Symbol.asyncIterator in source ? source[Symbol.asyncIterator]() : (source as Iterable<T>)[Symbol.iterator]();
  let failed = false;
  const runner = async () => {
    while (!failed && !signal?.aborted) {
      const next = await iterator.next();
      if (next.done) {
        return;
      }
      try {
        await fn(next.value);
      } catch (error) {
        failed = true;
        throw error;
      }
    }
  };
  const results = await Promise.allSettled(Array.from({ length: Math.max(1, concurrency) }, runner));
  const rejected = results.find((result): result is PromiseRejectedResult => result.status === 'rejected');
  if (rejected) {
    throw rejected.reason;
  }
}

/** Errors no retry or later pass can get past: a refused password or zone, or a stopped or closed client. */
const isFinal = (error: unknown) => error instanceof StorageError && (error.fatal || error.stopped);

export interface ReplicationResult {
  /** Every file is stored with the expected bytes and held by every region. */
  complete: boolean;
  /** Files each region still lacked when the wait ended (missing and mismatched files count for every region). */
  missingByRegion: Record<string, number>;
  waitedMs: number;
  /** DESCRIBE still showed other bytes than expected on its last answer. */
  mismatched: string[];
  /** DESCRIBE still found nothing on its last answer. */
  missing: string[];
  /** Files held by the main region whose replication did not finish in time. */
  incomplete: number;
  /** The first of those (up to `stragglerLimit`) and the regions each lacked, so a stuck file can be named. */
  stragglers: Array<{ path: string; regions: string[] }>;
  /** When each region first held every file, from the start of the wait; null if it never did. */
  regionWaitMs: Record<string, number | null>;
  passes: number;
  /** DESCRIBE failures (after retries) on the last pass; those files stay pending. */
  errors: number;
}

export interface WaitOptions {
  timeoutMs: number;
  /** DESCRIBEs in flight (default 256). */
  concurrency?: number;
  /** Time from the start of one pass to the next (default 15 s). */
  intervalMs?: number;
  /**
   * How long a file may keep showing as missing or with other bytes before the gate gives up on it (default 5 min).
   * DESCRIBE can lag an overwrite (see storage.ts), so such a file is DESCRIBEd again on every pass; once only such
   * files are left and each has lasted this long, the gate ends without waiting out the timeout.
   */
  mismatchGraceMs?: number;
  /** Stragglers named in the result (default 20). */
  stragglerLimit?: number;
  signal?: AbortSignal;
  onPass?: (pass: {
    pass: number;
    pending: number;
    elapsedMs: number;
    missingByRegion: Record<string, number>;
  }) => void;
}

/** Regions are bits of a 32-bit mask per file. */
const MAX_GATE_REGIONS = 31;

/**
 * The replication gate: DESCRIBEs every file, then again only the files some region still lacks, until every region
 * holds every file or the timeout passes. A file DESCRIBE shows missing or with other bytes counts as lacking from
 * every region and is asked about again on every pass (DESCRIBE can lag an overwrite); once only such files are left
 * and each has shown so for `mismatchGraceMs`, the gate fails without waiting out the timeout. A DESCRIBE that fails
 * after its retries leaves its file pending; a refused password ends the gate with that error at once.
 * What it keeps per file is a region bitmask and an index (8 bytes), so `items` sets the memory: pass a TileGateList
 * for large sets.
 */
export async function waitForReplication(
  storage: BunnyStorage,
  source: GateItems | ReplicationItem[],
  regions: string[] = REPLICATION_REGIONS,
  options: WaitOptions,
): Promise<ReplicationResult> {
  if (regions.length > MAX_GATE_REGIONS) {
    throw new Error(`The gate follows at most ${MAX_GATE_REGIONS} regions, got ${regions.length}`);
  }
  const items = Array.isArray(source) ? gateItems(source) : source;
  const started = performance.now();
  const elapsed = () => performance.now() - started;
  const graceMs = options.mismatchGraceMs ?? 5 * 60_000;
  const all = regions.length === 0 ? 0 : 2 ** regions.length - 1;
  const bit = new Map(regions.map((region, i) => [region, 2 ** i]));
  /** The regions each file still lacks, as bits. */
  const lacking = new Uint32Array(items.length).fill(all);
  /** Files whose last answer was missing or mismatched, and since when (elapsed ms) they have answered so. */
  const wrong = new Map<number, { state: 'missing' | 'mismatched'; since: number }>();
  const regionWaitMs: Record<string, number | null> = Object.fromEntries(regions.map((region) => [region, null]));
  let pending = new Uint32Array(items.length);
  for (let index = 0; index < pending.length; index++) {
    pending[index] = index;
  }
  let passes = 0;
  let errors = 0;
  let missingByRegion: Record<string, number>;

  for (;;) {
    const passStarted = performance.now();
    passes++;
    errors = 0;
    await forEachLimit(
      pending,
      options.concurrency ?? 256,
      async (index) => {
        let object: StorageObject | null;
        try {
          object = await storage.describe(items.path(index));
        } catch (error) {
          if (isFinal(error)) {
            throw error;
          }
          errors++;
          return;
        }
        if (!object || !checksumMatches(object.checksum, items.sha256(index))) {
          const since = wrong.get(index)?.since ?? elapsed();
          wrong.set(index, { state: object ? 'mismatched' : 'missing', since });
          lacking[index] = all;
          return;
        }
        wrong.delete(index);
        let mask = lacking[index];
        for (const region of object.replicatedZones) {
          const held = bit.get(region);
          if (held !== undefined && mask & held) {
            mask -= held;
          }
        }
        lacking[index] = mask;
      },
      options.signal,
    );
    let kept = 0;
    for (const index of pending) {
      if (lacking[index] !== 0 || wrong.has(index)) {
        pending[kept++] = index;
      }
    }
    pending = pending.slice(0, kept);

    const counts = new Float64Array(regions.length);
    for (const index of pending) {
      const mask = lacking[index];
      for (let i = 0; i < regions.length; i++) {
        if (mask & (2 ** i)) {
          counts[i]++;
        }
      }
    }
    missingByRegion = Object.fromEntries(regions.map((region, i) => [region, counts[i]]));
    for (const region of regions) {
      if (regionWaitMs[region] === null && missingByRegion[region] === 0) {
        regionWaitMs[region] = Math.round(elapsed());
      }
    }
    options.onPass?.({ pass: passes, pending: pending.length, elapsedMs: Math.round(elapsed()), missingByRegion });

    const gaveUp = pending.every((index) => {
      const since = wrong.get(index)?.since;
      return since !== undefined && elapsed() - since >= graceMs;
    });
    if (pending.length === 0 || gaveUp || options.signal?.aborted) {
      break;
    }
    const remaining = options.timeoutMs - elapsed();
    if (remaining <= 0) {
      break;
    }
    const wait = (options.intervalMs ?? 15_000) - (performance.now() - passStarted);
    if (wait > 0) {
      await sleep(Math.min(wait, remaining), undefined, { signal: options.signal }).catch(() => {});
    }
  }

  const paths = (state: 'missing' | 'mismatched') =>
    [...wrong].filter(([, file]) => file.state === state).map(([index]) => items.path(index));
  const stragglerLimit = options.stragglerLimit ?? 20;
  const stragglers: ReplicationResult['stragglers'] = [];
  let incomplete = 0;
  for (const index of pending) {
    if (wrong.has(index)) {
      continue;
    }
    incomplete++;
    if (stragglers.length < stragglerLimit) {
      const mask = lacking[index];
      stragglers.push({ path: items.path(index), regions: regions.filter((_, i) => mask & (2 ** i)) });
    }
  }
  return {
    complete: pending.length === 0,
    missingByRegion,
    waitedMs: Math.round(elapsed()),
    mismatched: paths('mismatched'),
    missing: paths('missing'),
    incomplete,
    stragglers,
    regionWaitMs,
    passes,
    errors,
  };
}

interface Tally {
  checked: number;
  replicated: number;
  partial: number;
  missing: number;
  mismatched: number;
  errors: number;
  /** Files held by each region (with the expected bytes). */
  regions: Record<string, number>;
}

export interface CoverageReport extends Tally {
  /** Share of checked files each region holds. */
  coverage: Record<string, number>;
  byZoom: Record<string, Tally>;
  missingPaths: string[];
  mismatchedPaths: string[];
  errorPaths: string[];
  seconds: number;
}

const newTally = (regions: string[]): Tally => ({
  checked: 0,
  replicated: 0,
  partial: 0,
  missing: 0,
  mismatched: 0,
  errors: 0,
  regions: Object.fromEntries(regions.map((region) => [region, 0])),
});

/** One DESCRIBE per file of a (possibly huge, streamed) source: per-region coverage, overall and per zoom. */
export async function checkReplication(
  storage: BunnyStorage,
  items: AsyncIterable<ReplicationItem> | Iterable<ReplicationItem>,
  regions: string[] = REPLICATION_REGIONS,
  options: {
    concurrency?: number;
    listLimit?: number;
    signal?: AbortSignal;
    onProgress?: (checked: number) => void;
  } = {},
): Promise<CoverageReport> {
  const started = performance.now();
  const listLimit = options.listLimit ?? 100;
  const total = newTally(regions);
  const byZoom: Record<string, Tally> = {};
  const report = { missingPaths: [] as string[], mismatchedPaths: [] as string[], errorPaths: [] as string[] };
  const note = (list: string[], path: string) => list.length < listLimit && list.push(path);

  await forEachLimit(
    items,
    options.concurrency ?? 256,
    async (item) => {
      const tallies = [total];
      if (item.tileId !== undefined) {
        const zoom = String(tileIdZoom(item.tileId));
        tallies.push((byZoom[zoom] ??= newTally(regions)));
      }
      let object: StorageObject | null;
      try {
        object = await storage.describe(item.path);
      } catch (error) {
        if (isFinal(error)) {
          throw error;
        }
        tallies.forEach((tally) => (tally.checked++, tally.errors++));
        note(report.errorPaths, item.path);
        return;
      }
      const { state, missingRegions } = classify(object, item.sha256, regions);
      for (const tally of tallies) {
        tally.checked++;
        tally[state]++;
        if (state === 'replicated' || state === 'partial') {
          const lacking = new Set(missingRegions);
          regions.filter((region) => !lacking.has(region)).forEach((region) => tally.regions[region]++);
        }
      }
      if (state === 'missing') {
        note(report.missingPaths, item.path);
      } else if (state === 'mismatched') {
        note(report.mismatchedPaths, item.path);
      }
      options.onProgress?.(total.checked);
    },
    options.signal,
  );

  return {
    ...total,
    coverage: Object.fromEntries(
      regions.map((region) => [region, total.checked ? total.regions[region] / total.checked : 0]),
    ),
    byZoom,
    ...report,
    seconds: (performance.now() - started) / 1000,
  };
}
