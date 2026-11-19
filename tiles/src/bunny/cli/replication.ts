/**
 * Checks that tile files are stored with the expected bytes and replicated: DESCRIBEs a random sample of positions,
 * or every position of a range, and reports per-region coverage (overall and per zoom), missing files and checksum
 * mismatches. The expected SHA-256 comes from the archive (--archive), or with --live from the live build's manifest
 * in the state zone, whose first 8 bytes of each tile's SHA-256 DESCRIBE's Checksum must start with: no archive
 * needed, so the sync watchdog runs it daily on a sample in every region (plan section 9).
 *
 *   npm run bunny:replication -- (--archive <path> | --live [--state-prefix <p>]) [--prefix v1]
 *     [--min-zoom Z --max-zoom Z | --from-id N --to-id M]
 *     [--sample N [--per-zoom] [--seed S]] [--regions UK,SE,...] [--concurrency 256]
 *     [--max-connections-per-address 8] [--addresses ip,ip,...]
 *     [--wait [--timeout-min 120] [--interval-s 30] [--mismatch-grace-s 300]] [--json <path>] [--list 100]
 *   npm run bunny:replication -- --listing [--prefix v1] [--min-zoom 0 --max-zoom 15] [--regions ...] [--json <path>]
 *
 * --live takes the prefix and zooms of the live build and needs --sample; while state.json lists pending builds the
 * zone may hold their bytes where they differ, so it then checks nothing and reports why (exit 0), before it reads the
 * manifest or the tiles zone.
 * --listing walks every `<prefix>/<z>/<x>/` folder of the zooms with directory listings (Bunny serves 5 at once per
 * zone; this takes 4): one listing returns the replica regions of up to 32,768 files in about 1.6 s, so it checks that
 * every position of a planet first load is stored and in every region in hours, where DESCRIBEs would take weeks. It
 * checks presence and replication, not bytes: Bunny checked each file's checksum at its PUT, and samples check bytes.
 * It walks whole zooms, so it takes --min-zoom/--max-zoom and no --from-id/--to-id.
 *
 * Without --sample every position of the range is checked, streamed (zooms default to the archive's). --per-zoom takes
 * N positions from each zoom instead of N overall. --wait polls until every region holds every file (the replication
 * gate) instead of checking once; it keeps the file list in memory, so use it for samples or small ranges. With --wait,
 * a file DESCRIBE shows missing or with other bytes is asked about again on every pass (DESCRIBE can lag an overwrite)
 * and fails the wait once it has shown so for --mismatch-grace-s; --timeout-min bounds the whole wait, the first pass
 * included. --regions codes are matched in upper case, each once.
 * Bunny allows 250 connections per zone per storage server, shared by every tool working on the zone: a seed takes up
 * to 240 per address, so this check takes 8 per address by default (about 40 in flight over 5 addresses). Raise
 * --max-connections-per-address only while nothing else uploads to the zone; --concurrency is lowered to the
 * addresses times that.
 * Needs BUNNY_STORAGE_ZONE and BUNNY_STORAGE_PASSWORD (and BUNNY_STORAGE_HOST unless it is storage.bunnycdn.com).
 * --addresses (or BUNNY_STORAGE_ADDRESSES) pins the storage IPs instead of resolving the host.
 * The report goes to stdout as JSON (and to --json), a summary to stderr.
 * --live and --listing need BUNNY_STATE_ZONE and BUNNY_STATE_PASSWORD too (--listing only when --prefix is not given).
 * Exit codes: 0 everything stored and on every region, 2 stored but not yet on every region, 1 missing or
 * mismatched files, DESCRIBE errors (with --wait, files whose last DESCRIBE failed), a refused password or no file to
 * check (a range or sample with no position in the archive or build), 3 bad arguments.
 */
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { parseArgs } from 'node:util';
import { ArchiveRanges, TileRun } from '../archive-walk';
import { hashHex, manifestBatches, readManifestHeader } from '../manifest';
import {
  checkReplication,
  CoverageReport,
  forEachLimit,
  normalizeRegions,
  REPLICATION_REGIONS,
  ReplicationItem,
  ReplicationResult,
  waitForReplication,
} from '../replication';
import { BunnyStateStore, downloadManifest, readState, SyncState } from '../state';
import { BunnyStorage, parseAddresses, resolveStorageAddresses, sha256Hex, storageOptionsFromEnv } from '../storage';
import { DEFAULT_PREFIX, MAX_SAFE_ZOOM, tileIdZoom, tilePath, zoomEnd, zoomRange, zoomStart } from '../tile-id';

class UsageError extends Error {}

const SHA_CACHE_MAX_TILE_BYTES = 4096;
const SHA_CACHE_ENTRIES = 65_536;
/** Leaves a running seed (240 per address by default) within Bunny's 250 per zone per storage server. */
const DEFAULT_MAX_CONNECTIONS_PER_ADDRESS = 8;

function integer(name: string, value: string | undefined, fallback?: number): number {
  if (value === undefined) {
    if (fallback === undefined) {
      throw new UsageError(`--${name} is required`);
    }
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new UsageError(`--${name} must be a non-negative integer, got ${value}`);
  }
  return parsed;
}

function mulberry32(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** `count` distinct ids of [from, to), sorted. */
function pick(from: number, to: number, count: number, next: () => number): number[] {
  const size = to - from;
  if (count >= size) {
    return Array.from({ length: size }, (_, i) => from + i);
  }
  const ids = new Set<number>();
  while (ids.size < count) {
    ids.add(from + Math.floor(next() * size));
  }
  return [...ids].sort((a, b) => a - b);
}

class ExpectedHashes {
  private cache = new Map<number, string>();
  constructor(private ranges: ArchiveRanges) {}

  async of(run: TileRun): Promise<string> {
    const cached = this.cache.get(run.offset);
    if (cached) {
      return cached;
    }
    const sha = sha256Hex(new Uint8Array(await this.ranges.archive.readTileData(run.offset, run.length)));
    if (run.length <= SHA_CACHE_MAX_TILE_BYTES) {
      if (this.cache.size >= SHA_CACHE_ENTRIES) {
        this.cache.delete(this.cache.keys().next().value!);
      }
      this.cache.set(run.offset, sha);
    }
    return sha;
  }
}

async function* rangeItems(
  ranges: ArchiveRanges,
  hashes: ExpectedHashes,
  prefix: string,
  from: number,
  to: number,
): AsyncGenerator<ReplicationItem> {
  for await (const run of ranges.runs(from, to)) {
    const sha256 = await hashes.of(run);
    const end = Math.min(run.tileId + run.runLength, to);
    for (let tileId = Math.max(run.tileId, from); tileId < end; tileId++) {
      yield { path: tilePath(prefix, tileId), sha256, tileId };
    }
  }
}

interface SampleOptions {
  from: number;
  to: number;
  count: number;
  perZoom: boolean;
  next: () => number;
}

function sampleIds({ from, to, count, perZoom, next }: SampleOptions): number[] {
  const ids: number[] = [];
  if (perZoom) {
    for (let z = tileIdZoom(from); z <= tileIdZoom(to - 1); z++) {
      ids.push(...pick(Math.max(from, zoomStart(z)), Math.min(to, zoomEnd(z)), count, next));
    }
  } else {
    ids.push(...pick(from, to, count, next));
  }
  return ids;
}

async function sampleItems(
  ranges: ArchiveRanges,
  hashes: ExpectedHashes,
  prefix: string,
  sample: SampleOptions,
): Promise<{ items: ReplicationItem[]; notInArchive: number }> {
  const ids = sampleIds(sample);
  const items: ReplicationItem[] = [];
  let notInArchive = 0;
  for (const tileId of ids) {
    const run = await ranges.find(tileId);
    if (!run) {
      notInArchive++;
      continue;
    }
    items.push({ path: tilePath(prefix, tileId), sha256: await hashes.of(run), tileId });
  }
  return { items, notInArchive };
}

/** Sampled positions with the manifest's hash of each (the first 8 bytes of its SHA-256), in one pass over it. */
async function manifestSampleItems(
  manifest: string,
  prefix: string,
  sample: SampleOptions,
): Promise<{ items: ReplicationItem[]; notInArchive: number }> {
  const ids = sampleIds(sample).sort((a, b) => a - b);
  const items: ReplicationItem[] = [];
  let next = 0;
  for await (const batch of manifestBatches(manifest)) {
    for (let i = 0; i < batch.count && next < ids.length; i++) {
      const start = batch.tileIds[i];
      const end = start + batch.runLengths[i];
      while (next < ids.length && ids[next] < start) {
        next++;
      }
      const sha256 = hashHex(batch.hashHi[i], batch.hashLo[i]);
      for (; next < ids.length && ids[next] < end; next++) {
        items.push({ path: tilePath(prefix, ids[next]), sha256, tileId: ids[next] });
      }
    }
  }
  return { items, notInArchive: ids.length - items.length };
}

/** The live build's manifest, downloaded to a temporary directory the caller removes. */
async function liveManifest(
  store: BunnyStateStore,
  state: SyncState,
  statePrefix: string | undefined,
): Promise<{ state: SyncState; manifest: string; dir: string }> {
  const dir = await mkdtemp(join(tmpdir(), 'bunny-replication-'));
  const manifest = join(dir, `${state.liveBuild}.tman.zst`);
  try {
    await downloadManifest(store, state, manifest, statePrefix);
  } catch (error) {
    await rm(dir, { recursive: true, force: true });
    throw error;
  }
  return { state, manifest, dir };
}

interface ColumnTally {
  columns: number;
  /** Positions of the zoom's columns walked. */
  expected: number;
  present: number;
  missing: number;
  /** Files of the expected positions each region holds. */
  regions: Record<string, number>;
}

/**
 * --listing: every column folder of each zoom, listed. A position is missing when its file (by the canonical name
 * tilePath() gives it) is not in the listing; a present file counts for each region its ReplicatedZones names.
 */
async function checkListings(
  storage: BunnyStorage,
  prefix: string,
  minZoom: number,
  maxZoom: number,
  regions: string[],
  listLimit: number,
) {
  const started = performance.now();
  const byZoom: Record<string, ColumnTally> = {};
  const missingPaths: string[] = [];
  let lastProgress = started;
  for (let z = minZoom; z <= maxZoom; z++) {
    const size = 2 ** z;
    const tally: ColumnTally = (byZoom[z] = {
      columns: 0,
      expected: 0,
      present: 0,
      missing: 0,
      regions: Object.fromEntries(regions.map((region) => [region, 0])),
    });
    const columns = (function* () {
      for (let x = 0; x < size; x++) {
        yield x;
      }
    })();
    // The storage client lets 4 listings run at once (Bunny serves 5 per zone).
    await forEachLimit(columns, 4, async (x) => {
      const seen = new Uint8Array(size);
      for (const entry of await storage.list(`${prefix}/${z}/${x}`)) {
        // Only the name tilePath() writes and clients ask for: `01.mvt` is another object than `1.mvt`.
        const match = /^(0|[1-9]\d*)\.mvt$/.exec(entry.name);
        const y = match ? Number(match[1]) : -1;
        if (entry.isDirectory || y < 0 || y >= size || seen[y]) {
          continue;
        }
        seen[y] = 1;
        tally.present++;
        const held = new Set(entry.replicatedZones);
        regions.forEach((region) => held.has(region) && tally.regions[region]++);
      }
      for (let y = 0; y < size; y++) {
        if (!seen[y]) {
          tally.missing++;
          if (missingPaths.length < listLimit) {
            missingPaths.push(`${prefix}/${z}/${x}/${y}.mvt`);
          }
        }
      }
      tally.columns++;
      tally.expected += size;
      const now = performance.now();
      if (now - lastProgress >= 10_000) {
        lastProgress = now;
        process.stderr.write(`z${z}: ${n(tally.columns)} of ${n(size)} columns listed, ${n(tally.missing)} missing\n`);
      }
    });
  }
  const sum = (field: 'expected' | 'present' | 'missing') =>
    Object.values(byZoom).reduce((total, tally) => total + tally[field], 0);
  const present = sum('present');
  const held = Object.fromEntries(
    regions.map((region) => [region, Object.values(byZoom).reduce((total, tally) => total + tally.regions[region], 0)]),
  );
  return {
    expected: sum('expected'),
    present,
    missing: sum('missing'),
    held,
    coverage: Object.fromEntries(regions.map((region) => [region, present ? held[region] / present : 0])),
    byZoom,
    missingPaths,
    seconds: (performance.now() - started) / 1000,
  };
}

const percent = (part: number, whole: number) => (whole ? `${((100 * part) / whole).toFixed(1)}%` : '-');
const n = (value: number) => value.toLocaleString('en-US');

function summarise(report: CoverageReport, regions: string[]): string {
  const lines = [
    `Checked ${n(report.checked)} files in ${report.seconds.toFixed(1)} s: ${n(report.replicated)} on every region, ` +
      `${n(report.partial)} not yet everywhere, ${n(report.missing)} missing, ${n(report.mismatched)} mismatched, ` +
      `${n(report.errors)} errors`,
    `  all zooms: ${regions.map((region) => `${region} ${percent(report.regions[region], report.checked)}`).join(' ')}`,
  ];
  for (const [zoom, tally] of Object.entries(report.byZoom).sort(([a], [b]) => Number(a) - Number(b))) {
    const problems = tally.missing + tally.mismatched + tally.errors;
    lines.push(
      `  z${zoom.padEnd(2)} ${n(tally.checked).padStart(9)} files, ${percent(tally.replicated, tally.checked)} everywhere` +
        `${problems ? `, ${tally.missing} missing, ${tally.mismatched} mismatched, ${tally.errors} errors` : ''}: ` +
        regions.map((region) => `${region} ${percent(tally.regions[region], tally.checked)}`).join(' '),
    );
  }
  for (const path of report.missingPaths.slice(0, 10)) {
    lines.push(`  missing ${path}`);
  }
  for (const path of report.mismatchedPaths.slice(0, 10)) {
    lines.push(`  mismatched ${path}`);
  }
  return lines.join('\n');
}

async function main(): Promise<number> {
  const { values } = parseArgs({
    options: {
      archive: { type: 'string' },
      live: { type: 'boolean', default: false },
      listing: { type: 'boolean', default: false },
      'state-prefix': { type: 'string' },
      prefix: { type: 'string' },
      'min-zoom': { type: 'string' },
      'max-zoom': { type: 'string' },
      'from-id': { type: 'string' },
      'to-id': { type: 'string' },
      sample: { type: 'string' },
      'per-zoom': { type: 'boolean', default: false },
      seed: { type: 'string' },
      regions: { type: 'string' },
      concurrency: { type: 'string' },
      wait: { type: 'boolean', default: false },
      'timeout-min': { type: 'string' },
      'interval-s': { type: 'string' },
      'mismatch-grace-s': { type: 'string' },
      'max-connections-per-address': { type: 'string' },
      json: { type: 'string' },
      list: { type: 'string' },
      addresses: { type: 'string' },
    },
    strict: true,
  });
  const sources = [values.archive !== undefined, values.live, values.listing].filter(Boolean).length;
  if (sources !== 1) {
    throw new UsageError('Give one of --archive, --live or --listing');
  }
  if (values.live && values.sample === undefined) {
    throw new UsageError('--live checks a sample: give --sample');
  }
  if (values.listing && (values.sample !== undefined || values.wait)) {
    throw new UsageError('--listing walks every column; it takes no --sample or --wait');
  }
  const byId = values['from-id'] !== undefined || values['to-id'] !== undefined;
  if (byId && (values['min-zoom'] !== undefined || values['max-zoom'] !== undefined)) {
    throw new UsageError('Give either --min-zoom/--max-zoom or --from-id/--to-id, not both');
  }
  if (byId && values.listing) {
    throw new UsageError('--listing walks whole zooms: bound it with --min-zoom/--max-zoom, not --from-id/--to-id');
  }
  // Checked before anything is read, so a skip for pending builds cannot hide a bad range.
  const idRange = byId ? { from: integer('from-id', values['from-id']), to: integer('to-id', values['to-id']) } : null;
  if (idRange && idRange.to <= idRange.from) {
    throw new UsageError(`Empty range [${idRange.from}, ${idRange.to})`);
  }
  // --listing's zooms, checked before anything is read: a reversed range lists no column and would pass.
  let listing: { minZoom: number; maxZoom: number } | undefined;
  if (values.listing) {
    const minZoom = integer('min-zoom', values['min-zoom'], 0);
    const maxZoom = integer('max-zoom', values['max-zoom'], 15);
    if (maxZoom < minZoom) {
      const defaulted = values['max-zoom'] === undefined ? ' (the default)' : '';
      throw new UsageError(`--max-zoom ${maxZoom}${defaulted} is below --min-zoom ${minZoom}`);
    }
    if (maxZoom > MAX_SAFE_ZOOM) {
      throw new UsageError(`--max-zoom must be at most ${MAX_SAFE_ZOOM}, got ${maxZoom}`);
    }
    listing = { minZoom, maxZoom };
  }
  if (values.sample !== undefined && integer('sample', values.sample) === 0) {
    throw new UsageError('--sample must be at least 1: a sample of none checks nothing');
  }
  // Region codes are compared with DESCRIBE's upper-case ones, each counted once.
  const regions = values.regions === undefined ? REPLICATION_REGIONS : normalizeRegions(values.regions.split(','));
  if (regions.length === 0) {
    throw new UsageError(`--regions needs region codes, e.g. UK,SE; got ${JSON.stringify(values.regions)}`);
  }
  let prefix = (values.prefix ?? DEFAULT_PREFIX).replace(/^\/+|\/+$/g, '');
  const perAddress = Math.max(
    1,
    integer('max-connections-per-address', values['max-connections-per-address'], DEFAULT_MAX_CONNECTIONS_PER_ADDRESS),
  );
  const requestedConcurrency = integer('concurrency', values.concurrency, 256);
  const timeoutMs = integer('timeout-min', values['timeout-min'], 120) * 60_000;
  if (values.wait && timeoutMs === 0) {
    throw new UsageError('--timeout-min must be at least 1: it bounds the whole wait, the first pass included');
  }
  const intervalMs = integer('interval-s', values['interval-s'], 30) * 1000;
  const mismatchGraceMs = integer('mismatch-grace-s', values['mismatch-grace-s'], 300) * 1000;
  const listLimit = integer('list', values.list, 100);
  const seed = integer('seed', values.seed, Math.floor(Math.random() * 2 ** 31));
  const mode = values.sample !== undefined ? (values['per-zoom'] ? 'sample-per-zoom' : 'sample') : 'range';

  const storageOptions = storageOptionsFromEnv();
  if (values.addresses !== undefined) {
    try {
      storageOptions.addresses = parseAddresses(values.addresses);
    } catch (error) {
      throw new UsageError((error as Error).message);
    }
  }
  /** The tiles zone's client, its addresses resolved only once something is to be checked. */
  const tileStorage = async () => {
    storageOptions.addresses ??= await resolveStorageAddresses(storageOptions.host);
    const concurrency = Math.max(1, Math.min(requestedConcurrency, storageOptions.addresses.length * perAddress));
    const storage = new BunnyStorage({ ...storageOptions, concurrency, connectionsPerAddress: perAddress });
    return { storage, concurrency };
  };
  const output = async (json: object, code: number) => {
    const text = JSON.stringify(json);
    process.stdout.write(text + '\n');
    if (values.json) {
      await writeFile(values.json, text + '\n');
    }
    return code;
  };

  if (listing) {
    if (values.prefix === undefined) {
      const store = BunnyStateStore.fromEnv();
      const state = await readState(store, values['state-prefix']).finally(() => store.close());
      if (!state) {
        throw new UsageError('No state.json says which prefix is live: give --prefix');
      }
      prefix = state.prefix;
    }
    const { storage } = await tileStorage();
    try {
      const report = await checkListings(storage, prefix, listing.minZoom, listing.maxZoom, regions, listLimit);
      const lacking = regions.filter((region) => report.held[region] < report.present);
      process.stderr.write(
        `Listed ${n(report.expected)} positions under ${prefix}/ in ${report.seconds.toFixed(1)} s: ` +
          `${n(report.missing)} missing; ${regions.map((region) => `${region} ${percent(report.held[region], report.present)}`).join(' ')}\n`,
      );
      for (const path of report.missingPaths.slice(0, 10)) {
        process.stderr.write(`  missing ${path}\n`);
      }
      return await output(
        { mode: 'listing', prefix, regions, ...report },
        report.missing > 0 ? 1 : lacking.length > 0 ? 2 : 0,
      );
    } finally {
      await storage.close();
    }
  }

  let live: Awaited<ReturnType<typeof liveManifest>> | undefined;
  let ranges: ArchiveRanges | undefined;
  if (values.live) {
    const store = BunnyStateStore.fromEnv();
    try {
      const state = await readState(store, values['state-prefix']);
      if (!state) {
        throw new Error('No state.json in the state zone: no build is live to check');
      }
      if (values.prefix === undefined) {
        prefix = state.prefix;
      }
      if (state.pending.length > 0) {
        // The zone may hold a pending build's bytes wherever one differs from the live build. Said before the
        // manifest or the tiles zone is needed, so neither can keep this report from being written.
        const reason = `pending builds ${state.pending.map((pending) => pending.build).join(', ')}: a sync has to finish first`;
        process.stderr.write(`Nothing checked: ${reason}\n`);
        return await output(
          { prefix, range: null, mode, seed, build: state.liveBuild, notInArchive: 0, regions, skipped: reason },
          0,
        );
      }
      live = await liveManifest(store, state, values['state-prefix']);
    } finally {
      await store.close();
    }
  } else {
    ranges = await ArchiveRanges.open(values.archive!);
  }
  let storage: BunnyStorage | undefined;
  try {
    const opened = await tileStorage();
    storage = opened.storage;
    const { concurrency } = opened;
    const bounds = live
      ? await readManifestHeader(live.manifest).then((header) => ({
          minZoom: header.minZoom ?? 0,
          // Every position of [0, positions) is in the build (D1b); a clipped manifest says so by its positions.
          maxZoom: tileIdZoom(Math.max(0, header.positions - 1)),
        }))
      : ranges!.archive.header;
    const { from, to } =
      idRange ??
      zoomRange(
        integer('min-zoom', values['min-zoom'], bounds.minZoom),
        integer('max-zoom', values['max-zoom'], bounds.maxZoom),
      );
    if (to <= from) {
      throw new UsageError(`Empty range [${from}, ${to})`);
    }
    const scope = {
      prefix,
      range: { from, to },
      mode,
      seed: values.sample !== undefined ? seed : null,
      build: live?.state.liveBuild ?? null,
      notInArchive: 0,
      regions,
    };
    const sample = {
      from,
      to,
      count: integer('sample', values.sample, 0),
      perZoom: values['per-zoom'],
      next: mulberry32(seed),
    };
    const hashes = ranges && new ExpectedHashes(ranges);
    const sampled =
      values.sample === undefined
        ? undefined
        : live
          ? await manifestSampleItems(live.manifest, prefix, sample)
          : await sampleItems(ranges!, hashes!, prefix, sample);
    scope.notInArchive = sampled?.notInArchive ?? 0;
    // A check of no file verifies nothing: an error, not a pass.
    const nothingToCheck = () =>
      new Error(
        `No file to check: ${sampled ? `none of the ${n(sampled.notInArchive)} positions sampled` : 'no position'} ` +
          `of [${from}, ${to}) is in the ${live ? 'live build' : 'archive'}`,
      );
    if (sampled?.items.length === 0) {
      throw nothingToCheck();
    }

    let code: number;
    let result: object;
    if (values.wait) {
      const items: ReplicationItem[] = sampled?.items ?? [];
      if (!sampled) {
        for await (const item of rangeItems(ranges!, hashes!, prefix, from, to)) {
          items.push(item);
        }
      }
      if (items.length === 0) {
        throw nothingToCheck();
      }
      const waited: ReplicationResult = await waitForReplication(storage, items, regions, {
        timeoutMs,
        intervalMs,
        mismatchGraceMs,
        concurrency,
        onPass: ({ pass, pending, elapsedMs, missingByRegion }) =>
          process.stderr.write(
            `pass ${pass} at ${Math.round(elapsedMs / 1000)} s: ${n(pending)} of ${n(items.length)} files not yet ` +
              `everywhere; missing per region: ${regions.map((region) => `${region} ${n(missingByRegion[region])}`).join(' ')}\n`,
          ),
      });
      code = waited.missing.length + waited.mismatched.length + waited.errors > 0 ? 1 : waited.complete ? 0 : 2;
      result = { ...scope, files: items.length, ...waited };
      process.stderr.write(
        `${waited.complete ? 'Replicated' : 'Not replicated'} after ${Math.round(waited.waitedMs / 1000)} s: ` +
          `${waited.missing.length} missing, ${waited.mismatched.length} mismatched, ${n(waited.incomplete)} incomplete; ` +
          `region done at (s): ${regions.map((region) => `${region} ${waited.regionWaitMs[region] === null ? '-' : Math.round(waited.regionWaitMs[region]! / 1000)}`).join(' ')}\n`,
      );
    } else {
      const report = await checkReplication(
        storage,
        sampled?.items ?? rangeItems(ranges!, hashes!, prefix, from, to),
        regions,
        { concurrency, listLimit },
      );
      if (report.checked === 0) {
        throw nothingToCheck();
      }
      code = report.missing + report.mismatched + report.errors > 0 ? 1 : report.partial > 0 ? 2 : 0;
      // Files stored with the right bytes that each region lacks: what the sample check alerts on.
      const stored = report.replicated + report.partial;
      const lacking = Object.fromEntries(regions.map((region) => [region, stored - report.regions[region]]));
      result = { ...scope, ...report, lacking };
      process.stderr.write(summarise(report, regions) + '\n');
    }
    return await output(result, code);
  } finally {
    await storage?.close();
    await ranges?.close();
    if (live) {
      await rm(live.dir, { recursive: true, force: true });
    }
  }
}

main().then(
  (code) => {
    process.exitCode = code;
  },
  (error: unknown) => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = error instanceof UsageError ? 3 : 1;
  },
);
