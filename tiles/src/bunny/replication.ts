/**
 * Replication checks through DESCRIBE, the only way to see replicas: regional storage endpoints refuse a zone whose
 * main region is elsewhere. Each stored file reports its SHA-256 and the replica regions holding it.
 */
import { setTimeout as sleep } from 'node:timers/promises';
import { BunnyStorage, StorageObject } from './storage';
import { tileIdZoom } from './tile-id';

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
  /** Hex SHA-256 the file must hold. */
  sha256: string;
  /** For per-zoom reports. */
  tileId?: number;
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
  if (object.checksum.toUpperCase() !== sha256.toUpperCase()) {
    return { state: 'mismatched', missingRegions: regions };
  }
  const held = new Set(object.replicatedZones);
  const missingRegions = regions.filter((region) => !held.has(region));
  return { state: missingRegions.length === 0 ? 'replicated' : 'partial', missingRegions };
}

/** Runs `fn` over every item with at most `concurrency` at once, pulling lazily from the source. */
export async function forEachLimit<T>(
  source: AsyncIterable<T> | Iterable<T>,
  concurrency: number,
  fn: (item: T) => Promise<void>,
  signal?: AbortSignal,
): Promise<void> {
  const iterator =
    Symbol.asyncIterator in source ? source[Symbol.asyncIterator]() : (source as Iterable<T>)[Symbol.iterator]();
  const runner = async () => {
    while (!signal?.aborted) {
      const next = await iterator.next();
      if (next.done) {
        return;
      }
      await fn(next.value);
    }
  };
  await Promise.all(Array.from({ length: Math.max(1, concurrency) }, runner));
}

export interface ReplicationResult {
  /** Every file is stored with the expected bytes and held by every region. */
  complete: boolean;
  /** Files each region still lacked when the wait ended (missing and mismatched files count for every region). */
  missingByRegion: Record<string, number>;
  waitedMs: number;
  /** Stored with other bytes than expected. Never polled again. */
  mismatched: string[];
  /** Not stored at all. Never polled again. */
  missing: string[];
  /** Files held by the main region whose replication did not finish in time. */
  incomplete: number;
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
  signal?: AbortSignal;
  onPass?: (pass: {
    pass: number;
    pending: number;
    elapsedMs: number;
    missingByRegion: Record<string, number>;
  }) => void;
}

/**
 * The replication gate: DESCRIBEs every file, then again only the files some region still lacks, until every region
 * holds every file or the timeout passes. A missing or mismatched file fails the gate at once (it would never
 * replicate the expected bytes) but the others are still followed, for the per-region wait times.
 */
export async function waitForReplication(
  storage: BunnyStorage,
  items: ReplicationItem[],
  regions: string[] = REPLICATION_REGIONS,
  options: WaitOptions,
): Promise<ReplicationResult> {
  const started = performance.now();
  const elapsed = () => performance.now() - started;
  const lacking = items.map(() => regions as string[]);
  const missing = new Set<number>();
  const mismatched = new Set<number>();
  const regionWaitMs: Record<string, number | null> = Object.fromEntries(regions.map((region) => [region, null]));
  let pending = items.map((_, index) => index);
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
        const item = items[index];
        let object: StorageObject | null;
        try {
          object = await storage.describe(item.path);
        } catch {
          errors++;
          return;
        }
        const { state, missingRegions } = classify(object, item.sha256, lacking[index]);
        if (state === 'missing') {
          missing.add(index);
        } else if (state === 'mismatched') {
          mismatched.add(index);
        } else {
          lacking[index] = missingRegions;
        }
      },
      options.signal,
    );
    pending = pending.filter((index) => !missing.has(index) && !mismatched.has(index) && lacking[index].length > 0);

    missingByRegion = Object.fromEntries(regions.map((region) => [region, missing.size + mismatched.size]));
    for (const index of pending) {
      for (const region of lacking[index]) {
        missingByRegion[region]++;
      }
    }
    for (const region of regions) {
      if (regionWaitMs[region] === null && missingByRegion[region] === 0) {
        regionWaitMs[region] = Math.round(elapsed());
      }
    }
    options.onPass?.({ pass: passes, pending: pending.length, elapsedMs: Math.round(elapsed()), missingByRegion });

    if (pending.length === 0 || options.signal?.aborted) {
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

  return {
    complete: pending.length === 0 && missing.size === 0 && mismatched.size === 0,
    missingByRegion,
    waitedMs: Math.round(elapsed()),
    mismatched: [...mismatched].map((index) => items[index].path),
    missing: [...missing].map((index) => items[index].path),
    incomplete: pending.length,
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
      } catch {
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
