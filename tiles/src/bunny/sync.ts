/**
 * The daily sync of the tiles zone to a new Protomaps build (bunny:sync, run by .github/workflows/tiles-sync.yml):
 * 1. read state.json: the live build, its manifest and any builds an earlier sync started without finishing;
 * 2. build the new build's manifest and plan against the live and pending manifests (planSync in diff.ts). A pending
 *    entry of this very build must hold the same tiles as the new manifest: beginBuild replaces its manifest, the only
 *    record of where that attempt wrote, so another archive under the same key is refused (revert it first, below);
 * 3. guard rail: stop before writing anything when more than maxChangedFraction of the positions changed;
 * 4. record the build as pending (beginBuild), then upload the changed positions only. Re-PUTting identical bytes
 *    clears a file's ReplicatedZones for good, so every position is DESCRIBEd first and only written when it doesn't
 *    hold the new bytes yet: a re-run after any failure skips what is stored, and a state that doesn't match the zone
 *    (a seed of another build adopted by mistake) costs no replication. Positions where a pending build differs from
 *    the live one are planned too, even where the new build matches the live one, to put the live bytes back;
 * 5. wait until every replica region holds every changed file (the replication gate); a timeout fails the run and
 *    leaves the build pending, so the next run checks those files again instead of writing them;
 * 6. upload the TileJSON and styles whose bytes changed, wait until every replica region holds all of them (not just
 *    the ones this run wrote: an earlier attempt may have written them and failed before its gate passed), purge the
 *    pull zone once, and make the build live in state.json (its manifest is stored first, and the archive's identity
 *    is recorded: archive-identity.ts).
 *
 * A run for the live build itself while state.json lists pending builds puts the live bytes back where they wrote
 * (step 4 without new bytes): that reverts a failed or cancelled sync.
 *
 * Other modes: --adopt records a build a seed (bunny:seed) already uploaded, given the seed's state dirs (which must
 * say it completed, for this archive, zone and prefix, over every tile ID the build has) and a sample of positions,
 * instead of writing tiles (a state dir an older bunny:seed left, with only a v1 archive fingerprint, counts only with
 * the archive's MD5, checked over the whole file, and for the very file that seed read, unchanged since it began: see
 * archiveProblem and legacySourceProblem); with --replace it switches a live zone to a build
 * seeded under another tile prefix (a full rewrite, plan section 6), the sample then also checked in every region.
 * Either way the TileJSON and styles go through the replication gate before the purge, in the regions given: before
 * the domain points at a freshly seeded zone, `--regions none` (storage only) spares waiting out its backlog.
 * --force-full writes every position of the build, each DESCRIBEd first, so it suits a repair or a small zoom range
 * (a planet first load is a seed and then --adopt). It keeps to the live build's prefix, as a diff does, and deletes
 * what a diff would: the positions the live build or a pending one has and this build doesn't (above --max-zoom too).
 * Under the live build's own key a run (a --force-full repair, or a revert) needs the very tiles of the live manifest:
 * no pending entry is made for a build that is live already, so nothing would record where it wrote others.
 *
 * The TileJSON and styles stay at the static prefix (`v1.json`, `v1/style/*`) whatever the tile prefix, so a switch
 * to another tile prefix only changes the TileJSON's tile template. After a run makes a build live, manifests in the
 * state zone that no state names are deleted, but for the newest few.
 *
 * Every run ends with a SyncReport, whatever stopped it; onReport sees it after every step. The manifests it makes or
 * downloads go to a directory of its own inside workDir, removed when it ends.
 */
import { lstat, mkdir, mkdtemp, readFile, rm, stat } from 'node:fs/promises';
import { isAbsolute, join } from 'node:path';
import { ArchiveIdentity, fileMd5Base64, identifyArchive } from './archive-identity';
import { ArchiveRanges, TileRun } from './archive-walk';
import { diffManifests, DiffRange, DiffTotals, planSync, SyncRange } from './diff';
import { buildManifest, clipManifest, manifestBatches, ManifestHeader, readManifestHeader } from './manifest';
import {
  classify,
  forEachLimit,
  GateItems,
  gateItems,
  normalizeRegions,
  REPLICATION_REGIONS,
  ReplicationResult,
  TileGateList,
  waitForReplication,
} from './replication';
import {
  beginBuild,
  downloadManifest,
  ObjectStore,
  PendingBuild,
  pruneManifests,
  publishBuild,
  readState,
  SyncState,
} from './state';
import { STATIC_PREFIX, StaticFile, staticFiles, StyleSource } from './static-files';
import { BunnyStorage, sha256Hex } from './storage';
import { DEFAULT_PREFIX, MAX_SAFE_ZOOM, prefixesOverlap, tilePath, zoomEnd } from './tile-id';
import { newUploadStats, SeedParams, SeedStatus, UploadItem, UploadStats, uploadTiles } from './upload';

export type SyncStatus = 'success' | 'skipped' | 'guard_rail' | 'failed';
export type SyncMode = 'diff' | 'force-full' | 'adopt' | 'replace';
export type SyncStep =
  'manifest' | 'diff' | 'identity' | 'check' | 'upload' | 'replication' | 'static' | 'purge' | 'state';

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
  /**
   * Seconds the replication gates (the tiles', then the TileJSON and styles') waited until each region held all their
   * files, added up; null for a region where one of them never did.
   */
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
  /** The first of those, with the regions each lacked: a file that never replicates blocks every later sync. */
  replicationStragglers: Array<{ path: string; regions: string[] }>;
  /** Of the TileJSON and style files, those whose bytes changed and were uploaded. */
  staticUploaded: number | null;
  purged: boolean;
  /** Manifests deleted from the state zone once the build was live. */
  manifestsDeleted: number | null;
  /** The first failed files. */
  failures: Array<{ path: string; error: string }>;
}

export interface SyncOptions {
  archive: string;
  build: string;
  /** Defaults to the archive metadata's OSM replication time. */
  osmDate?: string | null;
  /** Tile prefix in the tiles zone (default: the live one, or v1 without a state). */
  prefix?: string;
  /** Where the TileJSON and styles live: `${staticPrefix}.json`, `${staticPrefix}/style/*` (default v1). */
  staticPrefix?: string;
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
  /** With adopt: switch a live zone to a build seeded under another prefix. */
  replace?: boolean;
  /** With adopt: the state dirs of the seed (bunny:seed --state-dir) that uploaded the build, one per range. */
  seedStates?: string[];
  /**
   * With adopt: the archive's MD5 (base64, as builds.json gives it), checked over the whole file. Needed for a seed
   * state dir an older bunny:seed left, which records only a v1 fingerprint (header, root directory and size); the
   * archive must then also be the file that seed read, unchanged since it began (see legacySourceProblem).
   */
  archiveMd5?: string;
  /** Random positions --adopt checks (default 10,000). */
  adoptSamples?: number;
  /** Manifests kept in the state zone besides those state.json names, newest first (default 7). */
  keepManifests?: number;
  /**
   * PUT positions no pending build touched without a DESCRIBE first: faster, and safe only while the zone surely holds
   * the live build there.
   */
  plainPuts?: boolean;
  /** Where the TileJSON points tiles at, e.g. https://tiles.immich.cloud (needed unless dryRun). */
  publicUrl?: string;
  /** The map-styles build (needed unless dryRun). */
  styles?: StyleSource[];
  /** Manifests go to a directory of the run's own in here (each is ~1.3 GB for the planet). */
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
  staticPrefix: STATIC_PREFIX,
  maxChangedFraction: 0.05,
  replicationTimeoutMs: 120 * 60_000,
  replicationIntervalMs: 30_000,
  replicationConcurrency: 512,
  concurrency: 512,
  adoptSamples: 10_000,
  keepManifests: 7,
  maxFailures: 1_000,
  progressIntervalMs: 10_000,
};

const MAX_REPORTED_FAILURES = 20;
/** Least time the TileJSON and styles get to replicate, whatever the tile gate took. */
const STATIC_GATE_FLOOR_MS = 10 * 60_000;

export function emptyReport(
  options: Pick<SyncOptions, 'build' | 'prefix' | 'maxZoom' | 'dryRun' | 'forceFull' | 'adopt' | 'replace'>,
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
    mode: options.adopt ? (options.replace ? 'replace' : 'adopt') : options.forceFull ? 'force-full' : 'diff',
    dryRun: options.dryRun === true,
    prefix: (options.prefix ?? DEFAULT_PREFIX).replace(/^\/+|\/+$/g, ''),
    maxZoom: options.maxZoom ?? null,
    pendingBuilds: [],
    positionsVerified: null,
    positionsPending: null,
    filesSkipped: null,
    filesFailed: null,
    filesDeleted: null,
    replicationIncomplete: null,
    replicationStragglers: [],
    staticUploaded: null,
    purged: false,
    manifestsDeleted: null,
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

/** Ranges of a sync plan in tile-ID order, held compactly (17 bytes each, outside the JS heap). */
class RangeList implements Iterable<SyncRange> {
  /** False when ranges were left out (past the guard rail): such a list must never be uploaded. */
  complete = true;
  length = 0;
  private starts = new Float64Array(1024);
  private counts = new Float64Array(1024);
  private flags = new Uint8Array(1024);
  private static KINDS = ['changed', 'added', 'removed'] as const;

  push(range: SyncRange) {
    if (this.length === this.starts.length) {
      const grow = <T extends Float64Array | Uint8Array>(from: T, to: T) => (to.set(from), to);
      this.starts = grow(this.starts, new Float64Array(this.length * 2));
      this.counts = grow(this.counts, new Float64Array(this.length * 2));
      this.flags = grow(this.flags, new Uint8Array(this.length * 2));
    }
    this.starts[this.length] = range.start;
    this.counts[this.length] = range.count;
    this.flags[this.length] = RangeList.KINDS.indexOf(range.kind) | (range.verify ? 4 : 0);
    this.length++;
  }

  *[Symbol.iterator](): Iterator<SyncRange> {
    if (!this.complete) {
      throw new Error('The plan was cut short at the guard rail');
    }
    for (let i = 0; i < this.length; i++) {
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

/**
 * --force-full: every position of a manifest as one range per entry, each DESCRIBEd first, then the `removed` ranges
 * of the plan against the live and pending builds (positions they have and the build doesn't), to delete.
 */
async function* fullPlan(manifest: string, removed: Iterable<SyncRange> = []): AsyncGenerator<SyncRange> {
  for await (const batch of manifestBatches(manifest)) {
    for (let i = 0; i < batch.count; i++) {
      yield { start: batch.tileIds[i], count: batch.runLengths[i], kind: 'changed', verify: true };
    }
  }
  yield* removed;
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
  /** The archive's v2 identity, and whether its whole-file MD5 matched archiveMd5. */
  private identity?: ArchiveIdentity;
  private md5Verified = false;
  /** This run's own directory in workDir, made on first use: the run writes and removes nothing outside it. */
  private runDir?: Promise<string>;
  /** How long the tile gate waited, taken from the static files' share of the replication timeout. */
  private gateWaitedMs = 0;
  /** Per region, how long the gates so far waited until it held their files, added up; null once one never did. */
  private regionWaitMs: Record<string, number | null> = {};

  constructor(
    options: SyncOptions,
    private deps: SyncDeps,
    private report: SyncReport,
  ) {
    const defined = Object.fromEntries(Object.entries(options).filter(([, value]) => value !== undefined));
    this.o = { ...SYNC_DEFAULTS, ...(defined as SyncOptions) };
    // Region codes as the zone names them, each once: the adopt sample counts per region what the gate waits on.
    if (this.o.regions) {
      this.o.regions = normalizeRegions(this.o.regions);
    }
    this.log = options.log ?? (() => {});
  }

  async close() {
    await this.archive?.close();
    const dir = await this.runDir?.catch(() => undefined);
    if (dir) {
      await rm(dir, { recursive: true, force: true });
    }
  }

  /** A path for `name` in this run's directory: a file of the same name already in workDir is never touched. */
  private async scratch(name: string): Promise<string> {
    this.runDir ??= mkdir(this.o.workDir, { recursive: true }).then(() => mkdtemp(join(this.o.workDir, 'sync-')));
    return join(await this.runDir, name);
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
    if (o.replace && !o.adopt) {
      throw new Error('--replace goes with --adopt');
    }
    if (o.archiveMd5 !== undefined && !o.adopt) {
      throw new Error('--archive-md5 goes with --adopt');
    }
    const adopting = mode === 'adopt' || mode === 'replace';
    const staticPrefix = normalisePrefix(o.staticPrefix);
    if (!o.dryRun) {
      const missing = [
        !this.deps.tiles && 'the tiles zone',
        !this.deps.purge && 'the purge',
        !o.publicUrl && 'the public URL',
        !o.styles?.length && 'the styles',
        adopting && !o.seedStates?.length && "the seed's state dirs (--seed-state)",
      ].filter(Boolean);
      if (missing.length > 0) {
        throw new Error(`A sync that writes needs ${missing.join(', ')}`);
      }
    }

    const state = await readState(this.deps.state, o.statePrefix);
    report.previousBuild = state?.liveBuild ?? null;
    report.pendingBuilds = state?.pending.map((pending) => pending.build) ?? [];
    // A daily sync follows the live build's prefix, whatever a full rewrite switched it to.
    const prefix = normalisePrefix(o.prefix ?? state?.prefix ?? DEFAULT_PREFIX);
    report.prefix = prefix;
    if (mode === 'adopt' && state) {
      throw new Error(
        `state.json exists already (live ${state.liveBuild}); --adopt only starts an empty state, and --adopt ` +
          '--replace switches a live zone to a build seeded under another prefix',
      );
    }
    if (mode === 'replace') {
      if (!state) {
        throw new Error('No state.json to replace: --adopt without --replace starts one');
      }
      if (prefixesOverlap(prefix, state.prefix)) {
        throw new Error(
          `--adopt --replace needs the build seeded under a prefix apart from the live ${state.prefix}/, got ${prefix}/`,
        );
      }
    }
    if (!state && mode === 'diff') {
      throw new Error(
        'No state.json in the state zone: record a seeded build with --adopt, or upload one with --force-full',
      );
    }
    if (state && mode === 'diff' && state.liveBuild === o.build) {
      if (state.pending.length === 0) {
        report.status = 'skipped';
        report.positionsTotal = state.positions;
        report.osmDate = state.osmDate;
        this.log(`${o.build} is live already: nothing to do`);
        return;
      }
      this.log(
        `${o.build} is live, with ${report.pendingBuilds.join(', ')} pending: putting the live bytes back where ` +
          'they may have written',
      );
    }
    // A --force-full run records its build as pending against the live prefix too, so it writes only there.
    if (state && (mode === 'diff' || mode === 'force-full') && state.prefix !== prefix) {
      throw new Error(
        `The live build is under ${state.prefix}/, not ${prefix}/: switching prefixes is a seed under the new one ` +
          'and --adopt --replace',
      );
    }

    // Opened first: the manifest, the identity and every tile and file uploaded are read from this file (its source),
    // so a file put at the path during the run can't be recorded under another's manifest or identity.
    this.archive = await ArchiveRanges.open(o.archive);
    // The new build's manifest, cut to the zoom range.
    const { manifest, header } = await this.step('manifest', () => this.buildManifest());
    report.positionsTotal = header.positions;
    report.osmDate = o.osmDate ?? header.osmDate;

    // The plan (for --force-full only its removed ranges: every position of the build is written).
    let ranges: RangeList | undefined;
    let totals: DiffTotals | undefined;
    if (!adopting) {
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
    // Before anything is written: the state records it, and --adopt compares it with the seed's.
    await this.step('identity', () => this.identify());

    if (adopting) {
      await this.step('check', async () => {
        await this.checkSeeds(manifest, prefix);
        await this.checkSample(tiles, manifest, header);
      });
    } else {
      if (state && state.liveBuild !== o.build) {
        // Before the first PUT: a run that dies part-way leaves a state saying which positions may hold this build.
        // A run under the live key writes only the live build's own bytes (plan() makes sure), so needs no such record.
        await this.step('state', () =>
          beginBuild(this.deps.state, state, { buildKey: o.build, manifestFile: manifest, statePrefix: o.statePrefix }),
        );
      }
      // A file that already held the bytes still goes through the gate: an earlier run may have just written it. A
      // --force-full run, which may find the whole zone stored, leaves such files out only where no earlier run can
      // have left writes its gate never saw through: a state with nothing pending, for a build not live yet (a failed
      // repair of the live build leaves no record). Otherwise it gates every position (40 bytes each), as befits the
      // repairs and small zoom ranges it is for.
      const gateSkipped = mode !== 'force-full' || !state || state.pending.length > 0 || state.liveBuild === o.build;
      const written = await this.step('upload', () =>
        this.upload(tiles, mode === 'force-full' ? fullPlan(manifest, ranges) : ranges!, gateSkipped),
      );
      await this.step('replication', () => this.gate(tiles, written));
    }

    // The purge makes PoPs fetch them again, from the nearest replica: it must hold them first. All of them, not only
    // those this run wrote: a retry finds the bytes an attempt whose gate failed stored, and writes nothing. An
    // adoption too: the seed's check covers the tiles only.
    // The last read of the archive is behind: what was uploaded (or sampled) came from the file the identity is of, and
    // it must not have been written to meanwhile, or nothing of this build is served or recorded.
    await this.archive.checkUnchanged();
    const allStatic = await this.step('static', () => this.uploadStatic(tiles, staticPrefix));
    await this.step('replication', () => this.gateStatic(tiles, allStatic));
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
        archive: this.identity!,
        statePrefix: o.statePrefix,
      }),
    );
    report.status = 'success';
    this.log(`${o.build} is live`);
    await this.pruneManifests();
  }

  /** Housekeeping once the build is live: a failure or a stop here leaves the run a success. */
  private async pruneManifests() {
    const { o, report } = this;
    const started = performance.now();
    try {
      const deleted = await pruneManifests(this.deps.state, { keep: o.keepManifests, statePrefix: o.statePrefix });
      report.manifestsDeleted = deleted.length;
      if (deleted.length > 0) {
        this.log(`state: deleted ${deleted.length} manifests no state names (${deleted.join(', ')})`);
      }
    } catch (error) {
      this.log(`state: could not delete old manifests: ${error instanceof Error ? error.message : String(error)}`);
    } finally {
      report.stepSeconds.state = seconds((report.stepSeconds.state ?? 0) * 1000 + performance.now() - started);
      o.onReport?.(report);
    }
  }

  private async buildManifest(): Promise<{ manifest: string; header: ManifestHeader }> {
    const { o } = this;
    const full = await this.scratch(`${o.build}.tman.zst`);
    const started = performance.now();
    let last = started;
    await buildManifest(o.archive, {
      out: full,
      buildKey: o.build,
      threads: o.threads,
      source: this.archive!.source,
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
    const clipped = await this.scratch(`${o.build}.z${o.maxZoom}.tman.zst`);
    const header = await clipManifest(full, clipped, zoomEnd(o.maxZoom), o.maxZoom);
    await rm(full, { force: true });
    return { manifest: clipped, header };
  }

  /**
   * Downloads the live and pending manifests and plans the sync, keeping the ranges unless over the guard rail. For
   * --force-full, which writes every position of the build anyway, it keeps only the removed ranges.
   */
  private async plan(state: SyncState | null, manifest: string, header: ManifestHeader) {
    const { o, report } = this;
    if (!state) {
      // Nothing to plan against: --force-full writes every position (a diff without a state was refused).
      return { ranges: undefined, totals: undefined };
    }
    const live = await this.scratch(`live-${state.liveBuild}.tman.zst`);
    await downloadManifest(this.deps.state, state, live, o.statePrefix);
    const pending: string[] = [];
    for (const build of state.pending) {
      pending.push(await this.pendingManifest(state, build, manifest));
    }
    const force = report.mode === 'force-full';
    const plan = await planSync(live, manifest, pending);
    const ranges = new RangeList();
    for await (const range of plan) {
      if (force) {
        // Every position of the build is written (each DESCRIBEd first); what the live and pending builds have and
        // it doesn't is deleted, as a diff would.
        if (range.kind === 'removed') {
          ranges.push(range);
        }
      } else if (changedShare(plan.totals.diff, header.positions) <= o.maxChangedFraction) {
        ranges.push(range);
      } else {
        // Past the guard rail the run stops before writing, so the ranges are only counted.
        ranges.complete = false;
      }
    }
    report.positionsPending = plan.totals.verify;
    if (state.liveBuild === o.build) {
      this.refuseOtherLiveTiles(plan.totals.diff);
    }
    if (plan.totals.verify > 0) {
      this.log(
        `pending builds ${plan.totals.pendingBuilds.join(', ')}: ${plan.totals.verify} positions are checked before ` +
          `writing (${plan.totals.verifyOnly} only to restore the live bytes)`,
      );
    }
    return { ranges, totals: plan.totals.diff };
  }

  /**
   * A run under the live build's key (a --force-full repair, or the revert of a pending build) only puts back the live
   * build's own bytes: the archive must hold the very tiles of the live manifest. Such a run records no pending build
   * (the build is live already) and publishes its manifest over the live one, so tiles of other bytes, or positions
   * added or left out (another --max-zoom), would be written with nothing to say where: a run that died part-way would
   * leave them for later diffs to miss. Thrown before anything is written.
   */
  private refuseOtherLiveTiles(diff: DiffTotals) {
    const { o } = this;
    const differing = diff.changed + diff.added + diff.removed;
    if (differing === 0) {
      return;
    }
    const zoom = diff.byZoom.findIndex((counts) => counts.changed + counts.added + counts.removed > 0);
    throw new Error(
      `${o.build} is the live build, and ${o.archive} has other tiles than its manifest at ${differing} positions ` +
        `from zoom ${zoom} (${diff.changed} changed, ${diff.added} added, ${diff.removed} removed` +
        `${o.maxZoom === undefined ? '' : `, with --max-zoom ${o.maxZoom}`}): a sync under the live key only puts ` +
        "back the live build's own bytes, as it records no pending build to say where it wrote. Sync other tiles " +
        'under a build key of their own',
    );
  }

  /**
   * A pending build's manifest, downloaded. When it is this build again it must hold the same tiles as the new
   * manifest (tile IDs and hashes; its header's times may differ): beginBuild replaces it with the new one, and it is
   * all that says where the earlier attempt may have written bytes this archive doesn't have. So another archive
   * under the key (a build published again, another --max-zoom) is refused before anything is written.
   */
  private async pendingManifest(state: SyncState, build: PendingBuild, manifest: string): Promise<string> {
    const { o } = this;
    const file = await this.scratch(`pending-${build.build}.tman.zst`);
    await downloadManifest(this.deps.state, build, file, o.statePrefix);
    if (build.build === o.build) {
      let first: DiffRange | undefined;
      for await (const range of await diffManifests(file, manifest)) {
        first = range;
        break;
      }
      if (first) {
        const what = { changed: 'holds other bytes in', added: 'is only in', removed: 'is not in' }[first.kind];
        throw new Error(
          `${o.build} is pending from an attempt with other tiles than ${o.archive} (tile ${first.start} ${what} ` +
            'this archive): syncing it would replace the only record of where that attempt wrote. Sync the live ' +
            `build ${state.liveBuild} first, which puts its bytes back, then this archive`,
        );
      }
    }
    return file;
  }

  /**
   * Uploads the plan's changed positions and deletes its removed ones; returns what the gate must see: what it wrote,
   * and with `gateSkipped` also what it found stored already.
   */
  private async upload(
    tiles: BunnyStorage,
    ranges: AsyncIterable<SyncRange> | Iterable<SyncRange>,
    gateSkipped: boolean,
  ): Promise<TileGateList> {
    const { o, report } = this;
    const prefix = report.prefix;
    const written = new TileGateList(prefix);
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
            written.push(item.tileId, item.sha256);
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

  private waitFor(tiles: BunnyStorage, items: GateItems, timeoutMs: number, what: string) {
    const { o } = this;
    const regions = o.regions ?? REPLICATION_REGIONS;
    this.log(`replication: waiting for ${items.length} ${what} in ${regions.join(', ') || 'storage only'}`);
    return waitForReplication(tiles, items, regions, {
      timeoutMs,
      intervalMs: o.replicationIntervalMs,
      concurrency: o.replicationConcurrency,
      mismatchGraceMs: o.replicationMismatchGraceMs,
      signal: o.signal,
      onPass: ({ pass, pending, elapsedMs, missingByRegion }) => {
        const lacking = nonZero(missingByRegion);
        this.log(
          `replication pass ${pass} at ${seconds(elapsedMs)} s: ${pending} ${what} pending (${lacking || 'none'})`,
        );
      },
    });
  }

  /**
   * Records a gate's outcome in the report, its regional waits added to the earlier gate's (a region one of them
   * never completed in stays null); throws when it did not complete.
   */
  private gateOutcome(result: ReplicationResult, what: string) {
    const { report } = this;
    report.replicationComplete = result.complete;
    for (const [region, ms] of Object.entries(result.regionWaitMs)) {
      const before = this.regionWaitMs[region];
      this.regionWaitMs[region] = ms === null || before === null ? null : (before ?? 0) + ms;
    }
    report.replicationWaitSeconds = Object.fromEntries(
      Object.entries(this.regionWaitMs).map(([region, ms]) => [region, ms === null ? null : seconds(ms)]),
    );
    report.replicationIncomplete = result.incomplete + result.missing.length + result.mismatched.length;
    report.replicationStragglers = result.stragglers;
    for (const straggler of result.stragglers) {
      this.log(`replication: ${straggler.path} is not in ${straggler.regions.join(', ')}`);
    }
    this.stopIfAsked();
    if (!result.complete) {
      const lacking = nonZero(result.missingByRegion);
      const straggler = result.stragglers[0];
      const example = result.missing[0] ?? result.mismatched[0] ?? straggler?.path;
      throw new Error(
        `Replication of ${what} incomplete after ${seconds(result.waitedMs)} s: ${result.incomplete} files not in ` +
          `every region (${lacking}), ${result.missing.length} missing, ${result.mismatched.length} with other bytes` +
          (result.errors ? `, ${result.errors} DESCRIBE errors` : '') +
          (example ? `, e.g. ${example}` : '') +
          (straggler && example === straggler.path ? ` (not in ${straggler.regions.join(', ')})` : ''),
      );
    }
  }

  private async gate(tiles: BunnyStorage, items: TileGateList) {
    const started = performance.now();
    try {
      this.gateOutcome(await this.waitFor(tiles, items, this.o.replicationTimeoutMs, 'files'), 'the tiles');
    } finally {
      this.gateWaitedMs = performance.now() - started;
    }
  }

  /** The TileJSON and styles, within what the tile gate left of the replication timeout (10 min at least). */
  private async gateStatic(tiles: BunnyStorage, files: StaticFile[]) {
    const { o } = this;
    const timeoutMs = Math.max(
      Math.min(STATIC_GATE_FLOOR_MS, o.replicationTimeoutMs),
      o.replicationTimeoutMs - this.gateWaitedMs,
    );
    const items = gateItems(files.map((file) => ({ path: file.path, sha256: file.sha256 })));
    const result = await this.waitFor(tiles, items, timeoutMs, 'static files');
    try {
      this.gateOutcome(result, 'the TileJSON and styles');
    } catch (error) {
      if (this.report.mode !== 'adopt' || error instanceof SyncStop) {
        throw error;
      }
      throw new Error(
        `${(error as Error).message}; a seed's replication backlog can hold them up for hours: before the domain ` +
          'points at this zone, --regions none adopts once storage holds them',
        { cause: error },
      );
    }
  }

  /**
   * --adopt: the seed's state dirs must say it completed, for this archive, zone and prefix, and their tile-ID ranges
   * must cover every tile ID the build's manifest has (an archive may start above zoom 0 or have gaps, so the count of
   * positions says nothing of which IDs): a sample alone misses a few failed files or a chunk never done, and under
   * D1b nothing revisits those positions. A seed's range counts as a whole once its status says complete; how its
   * checkpoint got there (checkpoint.log) is bunny:seed's business.
   */
  private async checkSeeds(manifest: string, prefix: string) {
    const { o } = this;
    const zone = this.deps.tiles!.zone;
    const ranges: Array<{ from: number; to: number }> = [];
    for (const dir of o.seedStates ?? []) {
      let params: SeedParams;
      let status: SeedStatus;
      try {
        params = JSON.parse(await readFile(join(dir, 'seed.json'), 'utf8')) as SeedParams;
        status = JSON.parse(await readFile(join(dir, 'status.json'), 'utf8')) as SeedStatus;
      } catch (error) {
        const reason = error instanceof Error ? error.message : String(error);
        throw new Error(`${dir} is not a seed's state dir: ${reason}`, { cause: error });
      }
      const problems = [
        status.state !== 'complete' && `its last run ended ${status.state}${status.error ? ` (${status.error})` : ''}`,
        params.zone !== zone && `it seeded zone ${params.zone}, not ${zone}`,
        params.prefix !== prefix && `it seeded ${params.prefix}/, not ${prefix}/`,
        this.archiveProblem(params) || (await this.legacySourceProblem(dir, params, status)),
        !(Number.isSafeInteger(params.from) && Number.isSafeInteger(params.to) && params.from < params.to) &&
          `its seed.json has no tile-ID range (from ${params.from}, to ${params.to})`,
        (status.range?.from !== params.from || status.range?.to !== params.to) && 'its status.json is of another range',
      ].filter(Boolean);
      if (problems.length > 0) {
        throw new Error(`Seed state ${dir}: ${problems.join('; ')}`);
      }
      ranges.push({ from: params.from, to: params.to });
    }
    // The seeds' ranges, merged where they touch or overlap, then the manifest's runs walked against them.
    const covered: Array<{ from: number; to: number }> = [];
    for (const range of ranges.sort((a, b) => a.from - b.from)) {
      const last = covered.at(-1);
      if (last && range.from <= last.to) {
        last.to = Math.max(last.to, range.to);
      } else {
        covered.push({ ...range });
      }
    }
    const shown = covered.map(({ from, to }) => `[${from}, ${to})`);
    const coverage =
      shown.length > 4 ? `${shown.slice(0, 4).join(', ')} and ${shown.length - 4} more` : shown.join(', ');
    let next = 0;
    for await (const batch of manifestBatches(manifest)) {
      for (let i = 0; i < batch.count; i++) {
        const end = batch.tileIds[i] + batch.runLengths[i];
        for (let id = batch.tileIds[i]; id < end; id = covered[next].to) {
          while (next < covered.length && covered[next].to <= id) {
            next++;
          }
          if (next === covered.length || covered[next].from > id) {
            throw new Error(
              `The completed seeds given cover tile IDs ${coverage || 'none'}, not tile ${id} of the build: seed ` +
                'the tiles they miss',
            );
          }
        }
      }
    }
    this.log(`adopt: ${ranges.length} seed state dir(s) say every tile of the build (in ${coverage}) completed`);
  }

  /**
   * Hashes the archive (and, given archiveMd5, checks its MD5 at the same time): the file the manifest was built from
   * and the uploads read, unchanged, or the run stops.
   */
  private async identify() {
    const { o } = this;
    const { source } = this.archive!;
    // The MD5 pass reads the whole file on its own: stop it if the identity fails.
    const controller = new AbortController();
    const [identified, md5] = await Promise.all([
      identifyArchive(o.archive, { threads: o.threads }),
      o.archiveMd5 === undefined ? undefined : fileMd5Base64(o.archive, controller.signal, source),
    ]).catch((error: unknown) => {
      controller.abort();
      throw error;
    });
    if (identified.source !== source) {
      throw new Error(
        `${o.archive} was replaced or written to since this run opened it: its manifest and uploads are of the ` +
          'file it opened, not of the one there now',
      );
    }
    const { identity } = identified;
    if (md5 !== undefined && md5 !== o.archiveMd5) {
      throw new Error(`${o.archive} has MD5 ${md5}, not the ${o.archiveMd5} given with --archive-md5`);
    }
    this.identity = identity;
    this.md5Verified = md5 !== undefined;
    this.log(`archive: ${identity.identity}${this.md5Verified ? ', MD5 as given' : ''}`);
  }

  /**
   * Why a seed state's archive is not this one, or false. A v2 identity must match. A v1 fingerprint (an older
   * bunny:seed's, of the header, root directory and size only) must match this archive's, and counts only once the
   * whole file's MD5 matched the one given with archiveMd5 (from builds.json): the fingerprint alone can't tell
   * archives with other tile bytes apart.
   */
  private archiveProblem(params: SeedParams): string | false {
    const { o } = this;
    const recorded = params.archive as Partial<ArchiveIdentity> & { fingerprint?: unknown };
    const other = `it seeded another archive than ${o.archive}`;
    if (typeof recorded?.identity === 'string') {
      return (recorded.identity !== this.identity!.identity || recorded.size !== this.identity!.size) && other;
    }
    if (typeof recorded?.fingerprint !== 'string') {
      return 'its seed.json records no archive identity';
    }
    if (recorded.fingerprint !== this.archive!.legacyFingerprint || recorded.size !== this.archive!.size) {
      return other;
    }
    return (
      !this.md5Verified &&
      'it records only a v1 archive fingerprint (header, root directory and size), which other tile bytes can share: ' +
        "adopt it with --archive-md5 <the build's md5sum from builds.json>, checked over the whole archive"
    );
  }

  /**
   * Why the archive a v1 seed state's seed read may not be this one, or false (and false for a v2 identity, which
   * covers the whole file). The MD5 vouches for the archive given here only: a seed that read a damaged copy with the
   * same header, root directory and size (a download still in progress, say), later replaced by the real build at
   * that path, would pass the fingerprint and the MD5. So the file status.json names (the path the seed's last run
   * read) must be this archive (same device and inode), and neither it nor a symlink at that path may have changed
   * (ctime: any write, replacement or metadata change moves it) since the seed began: since seed.json was written,
   * at its first start, and since its last run started. A seed resumed from another copy under another path can't be
   * told from its state.
   */
  private async legacySourceProblem(dir: string, params: SeedParams, status: SeedStatus): Promise<string | false> {
    if (typeof (params.archive as Partial<ArchiveIdentity> | undefined)?.identity === 'string') {
      return false;
    }
    const { o } = this;
    const seeded = (status as Partial<SeedStatus>).archive;
    if (typeof seeded !== 'string' || !isAbsolute(seeded)) {
      return (
        `its status.json names the archive the seed read as ${JSON.stringify(seeded)}, not an absolute path, so ` +
        `nothing ties ${o.archive} to the bytes it uploaded`
      );
    }
    const files = await Promise.all([
      stat(o.archive, { bigint: true }),
      stat(seeded, { bigint: true }),
      lstat(seeded, { bigint: true }),
      stat(join(dir, 'seed.json'), { bigint: true }),
    ]).catch((error: unknown) => (error instanceof Error ? error.message : String(error)));
    if (typeof files === 'string') {
      return `the archive it read, ${seeded}, can't be checked (${files})`;
    }
    const [given, read, link, seedJson] = files;
    if (given.dev !== read.dev || given.ino !== read.ino) {
      return `it read ${seeded}, not ${o.archive}`;
    }
    const started = Date.parse(String((status as Partial<SeedStatus>).startedAt));
    const began = Number.isFinite(started)
      ? [seedJson.mtimeNs, BigInt(started) * 1_000_000n].reduce((a, b) => (b < a ? b : a))
      : seedJson.mtimeNs;
    const changed = read.ctimeNs > link.ctimeNs ? read.ctimeNs : link.ctimeNs;
    if (changed > began) {
      const at = (ns: bigint) => new Date(Number(ns / 1_000_000n)).toISOString();
      return (
        `the archive at ${seeded} changed (${at(changed)}) after the seed began (${at(began)}), so it need not hold ` +
        'the bytes the seed read: run bunny:seed again with this archive, a new --state-dir and --verify-existing ' +
        '(it writes only the files that differ) and adopt that state dir'
      );
    }
    return false;
  }

  /**
   * --adopt: the zone must already hold the build at random positions; with --replace each must also be in every
   * region, as the switch sends every client to the new prefix at once.
   */
  private async checkSample(tiles: BunnyStorage, manifest: string, header: ManifestHeader) {
    const { o, report } = this;
    const regions = o.regions ?? REPLICATION_REGIONS;
    const everywhere = report.mode === 'replace';
    const ids = await samplePositions(manifest, header.positions, o.adoptSamples);
    const wrong: string[] = [];
    const lacking: Record<string, number> = Object.fromEntries(regions.map((region) => [region, 0]));
    const unreplicated: string[] = [];
    await forEachLimit(
      ids,
      Math.min(256, o.concurrency),
      async (id) => {
        const run = await this.archive!.find(id);
        if (!run) {
          throw new Error(`Tile ${id} is not in ${o.archive}`);
        }
        const sha256 = sha256Hex(new Uint8Array(await this.archive!.archive.readTileData(run.offset, run.length)));
        const path = tilePath(report.prefix, id);
        if (!everywhere) {
          if (!(await tiles.holds(path, sha256))) {
            wrong.push(path);
          }
          return;
        }
        const { state, missingRegions } = classify(await tiles.describe(path), sha256, regions);
        if (state === 'missing' || (state === 'mismatched' && !(await tiles.holds(path, sha256)))) {
          wrong.push(path);
        } else if (state !== 'replicated') {
          // A DESCRIBE that still lags an overwrite says nothing of the replicas either.
          unreplicated.push(path);
          (state === 'mismatched' ? regions : missingRegions).forEach((region) => lacking[region]++);
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
    if (unreplicated.length > 0) {
      throw new Error(
        `${unreplicated.length} of ${ids.length} sampled positions under ${report.prefix}/ are not in every region ` +
          `yet (${nonZero(lacking)}), e.g. ${unreplicated[0]}: switch once the seed has replicated`,
      );
    }
  }

  /**
   * The TileJSON and styles; only those whose bytes changed are written (identical re-PUTs break replication).
   * Returns all of them, for the gate; the report counts the ones written. A stop asked for ends it before the next
   * file, or before the PUT of one whose DESCRIBE it came during; a PUT already sent finishes.
   */
  private async uploadStatic(tiles: BunnyStorage, staticPrefix: string): Promise<StaticFile[]> {
    const { o, report } = this;
    const { header, metadata } = this.archive!.archive;
    const files = staticFiles({
      header: o.maxZoom === undefined ? header : { ...header, maxZoom: Math.min(header.maxZoom, o.maxZoom) },
      metadata,
      styles: o.styles!,
      publicUrl: o.publicUrl!,
      prefix: report.prefix,
      staticPrefix,
    });
    const uploaded: StaticFile[] = [];
    try {
      for (const file of files) {
        this.stopIfAsked();
        if (!(await tiles.holds(file.path, file.sha256))) {
          this.stopIfAsked();
          await tiles.put(file.path, file.body, file.sha256);
          uploaded.push(file);
        }
      }
    } finally {
      report.staticUploaded = uploaded.length;
    }
    this.log(`static: ${uploaded.length} of ${files.length} files changed and uploaded`);
    return files;
  }
}
