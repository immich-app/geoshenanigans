/**
 * The daily sync of the tiles zone to a new Protomaps build (bunny:sync, run by .github/workflows/tiles-sync.yml):
 * 1. read state.json: the live build, its manifest and any builds an earlier sync started without finishing;
 * 2. build the new build's manifest and plan against the live and pending manifests (planSync in diff.ts);
 * 3. guard rail: stop before writing anything when more than maxChangedFraction of the positions changed;
 * 4. record the build as pending (beginBuild), then upload the changed positions only. Re-PUTting identical bytes
 *    clears a file's ReplicatedZones for good, so every position is DESCRIBEd first and only written when it doesn't
 *    hold the new bytes yet: a re-run after any failure skips what is stored, and a state that doesn't match the zone
 *    (a seed of another build adopted by mistake) costs no replication. Positions where a pending build differs from
 *    the live one are planned too, even where the new build matches the live one, to put the live bytes back;
 * 5. wait until every replica region holds every changed file (the replication gate); a timeout fails the run and
 *    leaves the build pending, so the next run checks those files again instead of writing them;
 * 6. upload the TileJSON and styles whose bytes changed, purge the pull zone once, and make the build live in
 *    state.json (its manifest is stored first).
 *
 * Two other modes start a state: --adopt records a build a seed (bunny:seed) already uploaded, checking a sample of
 * positions instead of writing tiles; --force-full writes every position of the build, each DESCRIBEd first, and
 * keeps the files it PUTs in memory for the gate, so it suits a repair or a small zoom range (a planet first load is a
 * seed and then --adopt).
 *
 * Every run ends with a SyncReport, whatever stopped it; onReport sees it after every step.
 */
import { rm } from 'node:fs/promises';
import { join } from 'node:path';
import { ArchiveRanges, TileRun } from './archive-walk';
import { diffManifests, DiffTotals, planSync, SyncRange } from './diff';
import { buildManifest, clipManifest, manifestBatches, ManifestHeader, readManifestHeader } from './manifest';
import { forEachLimit, REPLICATION_REGIONS, ReplicationItem, waitForReplication } from './replication';
import { beginBuild, downloadManifest, ObjectStore, PendingBuild, publishBuild, readState, SyncState } from './state';
import { staticFiles, StyleSource } from './static-files';
import { BunnyStorage, sha256Hex } from './storage';
import { DEFAULT_PREFIX, MAX_SAFE_ZOOM, tilePath, zoomEnd } from './tile-id';
import { newUploadStats, UploadItem, UploadStats, uploadTiles } from './upload';

export type SyncStatus = 'success' | 'skipped' | 'guard_rail' | 'failed';
export type SyncMode = 'diff' | 'force-full' | 'adopt';
export type SyncStep = 'manifest' | 'diff' | 'check' | 'upload' | 'replication' | 'static' | 'purge' | 'state';

/** The run's outcome; the fields up to `error` are the workflow's contract, every one present (null when unknown). */
export interface SyncReport {
  status: SyncStatus;
  build: string;
  previousBuild: string | null;
  osmDate: string | null;
  positionsTotal: number | null;
  /** Positions whose content differs between the live and the new build (changed, added or removed). */
  positionsChanged: number | null;
  changedFraction: number | null;
  /** Files PUT (not counting those already stored with the bytes). */
  filesUploaded: number | null;
  bytesUploaded: number | null;
  retries: number | null;
  rateLimited: number | null;
  replicationComplete: boolean | null;
  /** Seconds from the start of the gate until each region held every file; null for one that never did. */
  replicationWaitSeconds: Record<string, number | null> | null;
  stepSeconds: Partial<Record<SyncStep, number>>;
  error: string | null;
  mode: SyncMode;
  dryRun: boolean;
  prefix: string;
  maxZoom: number | null;
  /** Builds state.json listed as pending when the run planned. */
  pendingBuilds: string[];
  /** Positions DESCRIBEd before any PUT (or sampled, with --adopt). */
  positionsVerified: number | null;
  /** Positions a pending build may have written (the plan's `verify`), checked whatever plainPuts says. */
  positionsPending: number | null;
  /** Files that already held the new bytes, so were not sent. */
  filesSkipped: number | null;
  filesFailed: number | null;
  filesDeleted: number | null;
  /** Files some region still lacked when the gate ended. */
  replicationIncomplete: number | null;
  /** Of the TileJSON and style files, those whose bytes changed and were uploaded. */
  staticUploaded: number | null;
  purged: boolean;
  /** The first failed files. */
  failures: Array<{ path: string; error: string }>;
}

export interface SyncOptions {
  archive: string;
  build: string;
  /** Defaults to the archive metadata's OSM replication time. */
  osmDate?: string | null;
  /** Tile prefix in the tiles zone (default v1). */
  prefix?: string;
  /** Guard rail on the share of positions that changed (default 0.05). */
  maxChangedFraction?: number;
  /** Replication gate (default 120 min). */
  replicationTimeoutMs?: number;
  replicationIntervalMs?: number;
  replicationConcurrency?: number;
  /** How long a file may show as missing or with other bytes in the gate before it fails (default 5 min). */
  replicationMismatchGraceMs?: number;
  regions?: string[];
  /** Only positions of zooms up to this one (tests and staged loads); the manifest is cut to them. */
  maxZoom?: number;
  statePrefix?: string;
  dryRun?: boolean;
  forceFull?: boolean;
  adopt?: boolean;
  /** Random positions --adopt checks (default 10,000). */
  adoptSamples?: number;
  /**
   * PUT positions no pending build touched without a DESCRIBE first: faster, and safe only while the zone surely holds
   * the live build there.
   */
  plainPuts?: boolean;
  /** Where the TileJSON points tiles at, e.g. https://tiles.immich.cloud (needed unless dryRun). */
  publicUrl?: string;
  /** The map-styles build (needed unless dryRun). */
  styles?: StyleSource[];
  /** Manifests go here (each is ~1.3 GB for the planet). */
  workDir: string;
  /** Manifest hashing threads (default every core). */
  threads?: number;
  /** Files in flight while uploading (default 512). */
  concurrency?: number;
  maxFailures?: number;
  progressIntervalMs?: number;
  signal?: AbortSignal;
  log?: (line: string) => void;
  /** Called with the report as it fills in, after every step: persist it so a killed run still has one. */
  onReport?: (report: SyncReport) => void;
  /** Called as each step starts. */
  onStep?: (step: SyncStep) => void;
}

export interface SyncDeps {
  /** The state zone. */
  state: ObjectStore;
  /** The tiles zone; needed unless dryRun. */
  tiles?: BunnyStorage;
  /** One full purge of the pull zone; needed unless dryRun. */
  purge?: () => Promise<unknown>;
}

export const SYNC_DEFAULTS = {
  prefix: DEFAULT_PREFIX,
  maxChangedFraction: 0.05,
  replicationTimeoutMs: 120 * 60_000,
  replicationIntervalMs: 30_000,
  replicationConcurrency: 512,
  concurrency: 512,
  adoptSamples: 10_000,
  maxFailures: 1_000,
  progressIntervalMs: 10_000,
};

const MAX_REPORTED_FAILURES = 20;

export function emptyReport(
  options: Pick<SyncOptions, 'build' | 'prefix' | 'maxZoom' | 'dryRun' | 'forceFull' | 'adopt'>,
) {
  const report: SyncReport = {
    status: 'failed',
    build: options.build,
    previousBuild: null,
    osmDate: null,
    positionsTotal: null,
    positionsChanged: null,
    changedFraction: null,
    filesUploaded: null,
    bytesUploaded: null,
    retries: null,
    rateLimited: null,
    replicationComplete: null,
    replicationWaitSeconds: null,
    stepSeconds: {},
    error: null,
    mode: options.adopt ? 'adopt' : options.forceFull ? 'force-full' : 'diff',
    dryRun: options.dryRun === true,
    prefix: (options.prefix ?? SYNC_DEFAULTS.prefix).replace(/^\/+|\/+$/g, ''),
    maxZoom: options.maxZoom ?? null,
    pendingBuilds: [],
    positionsVerified: null,
    positionsPending: null,
    filesSkipped: null,
    filesFailed: null,
    filesDeleted: null,
    replicationIncomplete: null,
    staticUploaded: null,
    purged: false,
    failures: [],
  };
  return report;
}

function normalisePrefix(prefix: string): string {
  const trimmed = prefix.replace(/^\/+|\/+$/g, '');
  if (!trimmed || trimmed.split('/').some((part) => part === '' || part === '.' || part === '..')) {
    throw new Error(`Invalid prefix ${JSON.stringify(prefix)}`);
  }
  return trimmed;
}

class SyncStop extends Error {}

/** Runs one sync; never throws: a failure is the report's status and error. */
export async function runSync(options: SyncOptions, deps: SyncDeps): Promise<SyncReport> {
  const report = emptyReport(options);
  const run = new SyncRun(options, deps, report);
  try {
    await run.run();
  } catch (error) {
    report.status = 'failed';
    report.error = error instanceof Error ? error.message : String(error);
  } finally {
    await run.close();
  }
  return report;
}

/** Ranges of a sync plan in tile-ID order, held compactly. */
class RangeList implements Iterable<SyncRange> {
  /** False when ranges were left out (past the guard rail): such a list must never be uploaded. */
  complete = true;
  private starts: number[] = [];
  private counts: number[] = [];
  private flags: number[] = [];
  private static KINDS = ['changed', 'added', 'removed'] as const;

  get length() {
    return this.starts.length;
  }

  push(range: SyncRange) {
    this.starts.push(range.start);
    this.counts.push(range.count);
    this.flags.push(RangeList.KINDS.indexOf(range.kind) | (range.verify ? 4 : 0));
  }

  *[Symbol.iterator](): Iterator<SyncRange> {
    if (!this.complete) {
      throw new Error('The plan was cut short at the guard rail');
    }
    for (let i = 0; i < this.starts.length; i++) {
      yield {
        start: this.starts[i],
        count: this.counts[i],
        kind: RangeList.KINDS[this.flags[i] & 3],
        verify: (this.flags[i] & 4) !== 0,
      };
    }
  }
}

interface SyncItem extends UploadItem {
  verify: boolean;
}

/** Tiles kept by offset while planning items: repeats (the ocean tile, mostly) are read and hashed once. */
const TILE_CACHE_ENTRIES = 256;

/**
 * The upload items of a plan: the new build's tile at every changed or added position, in tile-ID order. Removed
 * ranges are skipped.
 */
export async function* planItems(
  archive: ArchiveRanges,
  ranges: AsyncIterable<SyncRange> | Iterable<SyncRange>,
): AsyncGenerator<SyncItem> {
  let run: TileRun | null = null;
  const cache = new Map<number, { bytes: Uint8Array; sha256: string }>();
  for await (const range of ranges) {
    if (range.kind === 'removed') {
      continue;
    }
    for (let id = range.start; id < range.start + range.count; id++) {
      if (!run || id < run.tileId || id >= run.tileId + run.runLength) {
        run = await archive.find(id);
        if (!run) {
          throw new Error(`Tile ${id} is not in ${archive.path}`);
        }
      }
      let tile = cache.get(run.offset);
      if (tile) {
        cache.delete(run.offset);
      } else {
        const bytes = new Uint8Array(await archive.archive.readTileData(run.offset, run.length));
        tile = { bytes, sha256: sha256Hex(bytes) };
        if (cache.size >= TILE_CACHE_ENTRIES) {
          cache.delete(cache.keys().next().value!);
        }
      }
      cache.set(run.offset, tile);
      yield { tileId: id, bytes: tile.bytes, sha256: tile.sha256, verify: range.verify };
    }
  }
}

/** Every position of a manifest as one range per entry, each DESCRIBEd first (--force-full). */
async function* fullPlan(manifest: string): AsyncGenerator<SyncRange> {
  for await (const batch of manifestBatches(manifest)) {
    for (let i = 0; i < batch.count; i++) {
      yield { start: batch.tileIds[i], count: batch.runLengths[i], kind: 'changed', verify: true };
    }
  }
}

/** `count` positions in [0, positions) of a manifest, as tile IDs in order, picked at random. */
async function samplePositions(manifest: string, positions: number, count: number): Promise<number[]> {
  const picks = new Set<number>();
  const wanted = Math.min(count, positions);
  while (picks.size < wanted) {
    picks.add(Math.floor(Math.random() * positions));
  }
  const ordinals = [...picks].sort((a, b) => a - b);
  const ids: number[] = [];
  let seen = 0;
  let next = 0;
  for await (const batch of manifestBatches(manifest)) {
    for (let i = 0; i < batch.count && next < ordinals.length; i++) {
      while (next < ordinals.length && ordinals[next] < seen + batch.runLengths[i]) {
        ids.push(batch.tileIds[i] + (ordinals[next] - seen));
        next++;
      }
      seen += batch.runLengths[i];
    }
  }
  return ids;
}

const seconds = (ms: number) => Math.round(ms / 100) / 10;

/** "UK 3, SYD 12" for the regions with a count. */
const nonZero = (counts: Record<string, number>) =>
  Object.entries(counts)
    .filter(([, count]) => count > 0)
    .map(([region, count]) => `${region} ${count}`)
    .join(', ');

/** The share of positions a diff changed, as the guard rail compares it. */
const changedShare = (totals: DiffTotals, positions: number) =>
  (totals.changed + totals.added + totals.removed) / Math.max(1, positions);

class SyncRun {
  private o: SyncOptions & typeof SYNC_DEFAULTS;
  private log: (line: string) => void;
  private archive?: ArchiveRanges;
  private files: string[] = [];

  constructor(
    options: SyncOptions,
    private deps: SyncDeps,
    private report: SyncReport,
  ) {
    const defined = Object.fromEntries(Object.entries(options).filter(([, value]) => value !== undefined));
    this.o = { ...SYNC_DEFAULTS, ...(defined as SyncOptions) };
    this.log = options.log ?? (() => {});
  }

  async close() {
    await this.archive?.close();
    await Promise.all(this.files.map((file) => rm(file, { force: true })));
  }

  private stopIfAsked() {
    if (this.o.signal?.aborted) {
      throw new SyncStop(`stopped (${String(this.o.signal.reason ?? 'signal')})`);
    }
  }

  private async step<T>(name: SyncStep, fn: () => Promise<T>): Promise<T> {
    this.stopIfAsked();
    this.o.onStep?.(name);
    const started = performance.now();
    try {
      return await fn();
    } finally {
      this.report.stepSeconds[name] = seconds(
        (this.report.stepSeconds[name] ?? 0) * 1000 + performance.now() - started,
      );
      this.o.onReport?.(this.report);
    }
  }

  async run() {
    const { report, o } = this;
    const prefix = normalisePrefix(report.prefix);
    const mode = report.mode;
    if (o.maxZoom !== undefined && (!Number.isInteger(o.maxZoom) || o.maxZoom < 0 || o.maxZoom > MAX_SAFE_ZOOM)) {
      throw new Error(`maxZoom must be an integer 0..${MAX_SAFE_ZOOM}, got ${o.maxZoom}`);
    }
    if (!(o.maxChangedFraction >= 0)) {
      throw new Error(`maxChangedFraction must be at least 0, got ${o.maxChangedFraction}`);
    }
    if (o.adopt && o.forceFull) {
      throw new Error('--adopt and --force-full do not go together');
    }
    if (!o.dryRun) {
      const missing = [
        !this.deps.tiles && 'the tiles zone',
        !this.deps.purge && 'the purge',
        !o.publicUrl && 'the public URL',
        !o.styles?.length && 'the styles',
      ].filter(Boolean);
      if (missing.length > 0) {
        throw new Error(`A sync that writes needs ${missing.join(', ')}`);
      }
    }

    const state = await readState(this.deps.state, o.statePrefix);
    report.previousBuild = state?.liveBuild ?? null;
    report.pendingBuilds = state?.pending.map((pending) => pending.build) ?? [];
    if (mode === 'adopt' && state) {
      throw new Error(`state.json exists already (live ${state.liveBuild}); --adopt only starts an empty state`);
    }
    if (!state && mode === 'diff') {
      throw new Error(
        'No state.json in the state zone: record a seeded build with --adopt, or upload one with --force-full',
      );
    }
    if (state && mode === 'diff' && state.liveBuild === o.build) {
      report.status = 'skipped';
      report.positionsTotal = state.positions;
      report.osmDate = state.osmDate;
      this.log(`${o.build} is live already: nothing to do`);
      return;
    }
    if (state && mode === 'diff' && state.prefix !== prefix) {
      throw new Error(
        `The live build is under ${state.prefix}/, not ${prefix}/: a sync to another prefix needs --force-full`,
      );
    }

    // The new build's manifest, cut to the zoom range.
    const { manifest, header } = await this.step('manifest', () => this.buildManifest());
    report.positionsTotal = header.positions;
    report.osmDate = o.osmDate ?? header.osmDate;

    // The plan.
    let ranges: RangeList | undefined;
    let totals: DiffTotals | undefined;
    if (mode !== 'adopt') {
      ({ ranges, totals } = await this.step('diff', () => this.plan(state, manifest, header)));
    }
    if (totals) {
      report.positionsChanged = totals.changed + totals.added + totals.removed;
      report.changedFraction = changedShare(totals, header.positions);
      this.log(
        `${totals.oldBuild} -> ${totals.newBuild}: ${totals.changed} changed, ${totals.added} added, ` +
          `${totals.removed} removed of ${header.positions} positions (${(report.changedFraction * 100).toFixed(4)}%)`,
      );
    }
    if (mode === 'diff' && report.changedFraction! > o.maxChangedFraction) {
      report.status = 'guard_rail';
      report.filesUploaded = 0;
      report.error =
        `${report.positionsChanged} of ${header.positions} positions changed (${report.changedFraction}), ` +
        `over the guard rail of ${o.maxChangedFraction}: nothing uploaded`;
      return;
    }
    if (o.dryRun) {
      report.status = 'success';
      this.log('Dry run: nothing written');
      return;
    }
    const tiles = this.deps.tiles!;
    this.archive = await ArchiveRanges.open(o.archive);

    if (mode === 'adopt') {
      await this.step('check', () => this.checkSample(tiles, manifest, header));
    } else {
      if (state && state.liveBuild !== o.build) {
        // Before the first PUT: a run that dies part-way leaves a state saying which positions may hold this build.
        await this.step('state', () =>
          beginBuild(this.deps.state, state, { buildKey: o.build, manifestFile: manifest, statePrefix: o.statePrefix }),
        );
      }
      const written = await this.step('upload', () =>
        this.upload(tiles, mode === 'force-full' ? fullPlan(manifest) : ranges!),
      );
      await this.step('replication', () => this.gate(tiles, written));
    }

    await this.step('static', () => this.uploadStatic(tiles));
    await this.step('purge', async () => {
      await this.deps.purge!();
      report.purged = true;
    });
    await this.step('state', () =>
      publishBuild(this.deps.state, {
        buildKey: o.build,
        manifestFile: manifest,
        prefix,
        osmDate: report.osmDate,
        statePrefix: o.statePrefix,
      }),
    );
    report.status = 'success';
    this.log(`${o.build} is live`);
  }

  private async buildManifest(): Promise<{ manifest: string; header: ManifestHeader }> {
    const { o } = this;
    const full = join(o.workDir, `${o.build}.tman.zst`);
    this.files.push(full);
    const started = performance.now();
    let last = started;
    await buildManifest(o.archive, {
      out: full,
      buildKey: o.build,
      threads: o.threads,
      onProgress: (progress) => {
        const now = performance.now();
        if (now - last >= o.progressIntervalMs) {
          last = now;
          const share = progress.archiveEntries / Math.max(1, progress.archiveEntriesTotal);
          this.log(`manifest: ${(share * 100).toFixed(1)}% of entries after ${seconds(now - started)} s`);
        }
      },
    });
    if (o.maxZoom === undefined) {
      return { manifest: full, header: await readManifestHeader(full) };
    }
    const clipped = join(o.workDir, `${o.build}.z${o.maxZoom}.tman.zst`);
    this.files.push(clipped);
    const header = await clipManifest(full, clipped, zoomEnd(o.maxZoom), o.maxZoom);
    await rm(full, { force: true });
    return { manifest: clipped, header };
  }

  /** Downloads the live and pending manifests and plans the sync, keeping the ranges unless over the guard rail. */
  private async plan(state: SyncState | null, manifest: string, header: ManifestHeader) {
    const { o, report } = this;
    let live: string | undefined;
    const pending: string[] = [];
    if (state) {
      live = join(o.workDir, `live-${state.liveBuild}.tman.zst`);
      this.files.push(live);
      await downloadManifest(this.deps.state, state, live, o.statePrefix);
      for (const build of state.pending) {
        pending.push(await this.pendingManifest(build, manifest, header));
      }
    }
    if (report.mode === 'force-full') {
      // Every position is written (verified first); the diff against the live build is only reported.
      if (!live) {
        return { ranges: undefined, totals: undefined };
      }
      const diff = await diffManifests(live, manifest);
      for await (const range of diff) {
        void range;
      }
      return { ranges: undefined, totals: diff.totals };
    }
    const plan = await planSync(live!, manifest, pending);
    const ranges = new RangeList();
    for await (const range of plan) {
      // Past the guard rail the run stops before writing, so the ranges are only counted.
      if (changedShare(plan.totals.diff, header.positions) <= o.maxChangedFraction) {
        ranges.push(range);
      } else {
        ranges.complete = false;
      }
    }
    report.positionsPending = plan.totals.verify;
    if (plan.totals.verify > 0) {
      this.log(
        `pending builds ${plan.totals.pendingBuilds.join(', ')}: ${plan.totals.verify} positions are checked before ` +
          `writing (${plan.totals.verifyOnly} only to restore the live bytes)`,
      );
    }
    return { ranges, totals: plan.totals.diff };
  }

  /** A pending build's manifest: the new one when it is this build again, else downloaded. */
  private async pendingManifest(build: PendingBuild, manifest: string, header: ManifestHeader): Promise<string> {
    if (build.build === this.o.build && build.positions === header.positions) {
      return manifest;
    }
    const file = join(this.o.workDir, `pending-${build.build}.tman.zst`);
    this.files.push(file);
    await downloadManifest(this.deps.state, build, file, this.o.statePrefix);
    return file;
  }

  /** Uploads the plan's changed positions and deletes its removed ones; returns what the gate must see. */
  private async upload(
    tiles: BunnyStorage,
    ranges: AsyncIterable<SyncRange> | Iterable<SyncRange>,
  ): Promise<ReplicationItem[]> {
    const { o, report } = this;
    const prefix = report.prefix;
    const written: ReplicationItem[] = [];
    const removed: SyncRange[] = [];
    // Removed ranges are set aside as the plan streams past (under D1b every build has every position, so there
    // are none in practice).
    async function* uploads() {
      for await (const range of ranges) {
        if (range.kind === 'removed') {
          removed.push(range);
        } else {
          yield range;
        }
      }
    }
    const stats: UploadStats = newUploadStats();
    const before = { ...tiles.counters };
    const started = performance.now();
    const progress = setInterval(() => {
      const done = stats.uploaded + stats.skippedExisting + stats.failed;
      this.log(
        `upload: ${stats.uploaded} uploaded, ${stats.skippedExisting} already stored, ${stats.failed} failed ` +
          `(${Math.round(done / Math.max(0.001, (performance.now() - started) / 1000))}/s), ` +
          `${tiles.inFlight} in flight, ${tiles.counters.retries - before.retries} retries, ` +
          `${tiles.counters.rateLimited - before.rateLimited} 429s`,
      );
    }, o.progressIntervalMs);
    // A file that already held the bytes still goes through the gate: an earlier run may have just written it. A
    // --force-full run gates only what it writes, as it may skip the whole zone.
    const gateSkipped = report.mode !== 'force-full';
    let verified = 0;
    try {
      await uploadTiles(tiles, planItems(this.archive!, uploads()), {
        prefix,
        concurrency: o.concurrency,
        maxFailures: o.maxFailures,
        signal: o.signal,
        verify: report.mode === 'force-full' || !o.plainPuts,
        stats,
        onSettled: (item, outcome) => {
          if (item.verify || report.mode === 'force-full' || !o.plainPuts) {
            verified++;
          }
          if (outcome === 'uploaded' || (outcome === 'skipped' && gateSkipped)) {
            written.push({ path: tilePath(prefix, item.tileId), sha256: item.sha256, tileId: item.tileId });
          }
        },
      });
    } finally {
      clearInterval(progress);
      report.filesUploaded = stats.uploaded;
      report.bytesUploaded = stats.bytes;
      report.retries = stats.retries;
      report.rateLimited = stats.rateLimited;
      report.filesSkipped = stats.skippedExisting;
      report.filesFailed = stats.failed;
      report.positionsVerified = verified;
      report.failures = stats.failures.slice(0, MAX_REPORTED_FAILURES).map(({ path, error }) => ({ path, error }));
    }
    this.log(
      `upload: ${stats.uploaded} uploaded (${stats.bytes} bytes), ${stats.skippedExisting} already stored, ` +
        `${stats.failed} failed, ${stats.retries} retries, ${stats.rateLimited} rate limited ` +
        `in ${seconds(stats.seconds * 1000)} s`,
    );
    if (stats.failed > 0 || stats.stopped) {
      if (!stats.error && stats.failed === 0) {
        this.stopIfAsked();
      }
      throw new Error(
        stats.error ??
          (stats.failed > 0 ? `${stats.failed} files failed, e.g. ${stats.failures[0]?.path}` : 'the upload stopped'),
      );
    }

    let deleted = 0;
    const positions = function* () {
      for (const range of removed) {
        for (let id = range.start; id < range.start + range.count; id++) {
          yield id;
        }
      }
    };
    await forEachLimit(
      positions(),
      o.concurrency,
      async (id) => {
        await tiles.delete(tilePath(prefix, id));
        deleted++;
      },
      o.signal,
    );
    report.filesDeleted = deleted;
    this.stopIfAsked();
    return written;
  }

  private async gate(tiles: BunnyStorage, items: ReplicationItem[]) {
    const { o, report } = this;
    const regions = o.regions ?? REPLICATION_REGIONS;
    this.log(`replication: waiting for ${items.length} files in ${regions.join(', ')}`);
    const result = await waitForReplication(tiles, items, regions, {
      timeoutMs: o.replicationTimeoutMs,
      intervalMs: o.replicationIntervalMs,
      concurrency: o.replicationConcurrency,
      mismatchGraceMs: o.replicationMismatchGraceMs,
      signal: o.signal,
      onPass: ({ pass, pending, elapsedMs, missingByRegion }) => {
        const lacking = nonZero(missingByRegion);
        this.log(
          `replication pass ${pass} at ${seconds(elapsedMs)} s: ${pending} files pending (${lacking || 'none'})`,
        );
      },
    });
    report.replicationComplete = result.complete;
    report.replicationWaitSeconds = Object.fromEntries(
      Object.entries(result.regionWaitMs).map(([region, ms]) => [region, ms === null ? null : seconds(ms)]),
    );
    report.replicationIncomplete = result.incomplete + result.missing.length + result.mismatched.length;
    this.stopIfAsked();
    if (!result.complete) {
      const lacking = nonZero(result.missingByRegion);
      throw new Error(
        `Replication incomplete after ${seconds(result.waitedMs)} s: ${result.incomplete} files not in every region` +
          ` (${lacking}), ${result.missing.length} missing, ${result.mismatched.length} with other bytes` +
          (result.errors ? `, ${result.errors} DESCRIBE errors` : '') +
          (result.missing[0] || result.mismatched[0] ? `, e.g. ${result.missing[0] ?? result.mismatched[0]}` : ''),
      );
    }
  }

  /** --adopt: the zone must already hold the build at random positions. */
  private async checkSample(tiles: BunnyStorage, manifest: string, header: ManifestHeader) {
    const { o, report } = this;
    const ids = await samplePositions(manifest, header.positions, o.adoptSamples);
    const wrong: string[] = [];
    await forEachLimit(
      ids,
      Math.min(256, o.concurrency),
      async (id) => {
        const run = await this.archive!.find(id);
        if (!run) {
          throw new Error(`Tile ${id} is not in ${o.archive}`);
        }
        const bytes = new Uint8Array(await this.archive!.archive.readTileData(run.offset, run.length));
        const path = tilePath(report.prefix, id);
        if (!(await tiles.holds(path, sha256Hex(bytes)))) {
          wrong.push(path);
        }
      },
      o.signal,
    );
    report.positionsVerified = ids.length;
    this.log(`adopt: ${ids.length - wrong.length} of ${ids.length} sampled positions hold the build`);
    if (wrong.length > 0) {
      const examples = wrong.slice(0, 3).join(', ');
      throw new Error(`${wrong.length} of ${ids.length} sampled positions don't hold ${o.build}, e.g. ${examples}`);
    }
  }

  /** The TileJSON and styles; only those whose bytes changed are written (identical re-PUTs break replication). */
  private async uploadStatic(tiles: BunnyStorage) {
    const { o, report } = this;
    const { header, metadata } = this.archive!.archive;
    const files = staticFiles({
      header: o.maxZoom === undefined ? header : { ...header, maxZoom: Math.min(header.maxZoom, o.maxZoom) },
      metadata,
      styles: o.styles!,
      publicUrl: o.publicUrl!,
      prefix: report.prefix,
    });
    let uploaded = 0;
    for (const file of files) {
      if (!(await tiles.holds(file.path, file.sha256))) {
        await tiles.put(file.path, file.body, file.sha256);
        uploaded++;
      }
    }
    report.staticUploaded = uploaded;
    this.log(`static: ${uploaded} of ${files.length} files changed and uploaded`);
  }
}
