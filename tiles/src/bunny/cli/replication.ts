/**
 * Checks that tile files are stored with the archive's bytes and replicated: DESCRIBEs a random sample of positions,
 * or every position of a range, and reports per-region coverage (overall and per zoom), missing files and checksum
 * mismatches, with the expected SHA-256 recomputed from the archive.
 *
 *   npm run bunny:replication -- --archive <path> [--prefix v1] [--min-zoom Z --max-zoom Z | --from-id N --to-id M]
 *     [--sample N [--per-zoom] [--seed S]] [--regions UK,SE,...] [--concurrency 256]
 *     [--max-connections-per-address 8] [--addresses ip,ip,...]
 *     [--wait [--timeout-min 120] [--interval-s 30] [--mismatch-grace-s 300]] [--json <path>] [--list 100]
 *
 * Without --sample every position of the range is checked, streamed (zooms default to the archive's). --per-zoom takes
 * N positions from each zoom instead of N overall. --wait polls until every region holds every file (the replication
 * gate) instead of checking once; it keeps the file list in memory, so use it for samples or small ranges. With --wait,
 * a file DESCRIBE shows missing or with other bytes is asked about again on every pass (DESCRIBE can lag an overwrite)
 * and fails the wait once it has shown so for --mismatch-grace-s.
 * Bunny allows 250 connections per zone per storage server, shared by every tool working on the zone: a seed takes up
 * to 240 per address, so this check takes 8 per address by default (about 40 in flight over 5 addresses). Raise
 * --max-connections-per-address only while nothing else uploads to the zone; --concurrency is lowered to the
 * addresses times that.
 * Needs BUNNY_STORAGE_ZONE and BUNNY_STORAGE_PASSWORD (and BUNNY_STORAGE_HOST unless it is storage.bunnycdn.com).
 * --addresses (or BUNNY_STORAGE_ADDRESSES) pins the storage IPs instead of resolving the host.
 * The report goes to stdout as JSON (and to --json), a summary to stderr.
 * Exit codes: 0 everything stored and on every region, 2 stored but not yet on every region, 1 missing or
 * mismatched files, DESCRIBE errors (on the last pass with --wait) or a refused password, 3 bad arguments.
 */
import { writeFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { ArchiveRanges, TileRun } from '../archive-walk';
import {
  checkReplication,
  CoverageReport,
  REPLICATION_REGIONS,
  ReplicationItem,
  ReplicationResult,
  waitForReplication,
} from '../replication';
import { BunnyStorage, parseAddresses, resolveStorageAddresses, sha256Hex, storageOptionsFromEnv } from '../storage';
import { DEFAULT_PREFIX, tileIdZoom, tilePath, zoomEnd, zoomRange, zoomStart } from '../tile-id';

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

async function sampleItems(
  ranges: ArchiveRanges,
  hashes: ExpectedHashes,
  prefix: string,
  { from, to, count, perZoom, next }: { from: number; to: number; count: number; perZoom: boolean; next: () => number },
): Promise<{ items: ReplicationItem[]; notInArchive: number }> {
  const ids: number[] = [];
  if (perZoom) {
    for (let z = tileIdZoom(from); z <= tileIdZoom(to - 1); z++) {
      ids.push(...pick(Math.max(from, zoomStart(z)), Math.min(to, zoomEnd(z)), count, next));
    }
  } else {
    ids.push(...pick(from, to, count, next));
  }
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
      prefix: { type: 'string', default: DEFAULT_PREFIX },
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
  if (!values.archive) {
    throw new UsageError('--archive is required');
  }
  const byId = values['from-id'] !== undefined || values['to-id'] !== undefined;
  if (byId && (values['min-zoom'] !== undefined || values['max-zoom'] !== undefined)) {
    throw new UsageError('Give either --min-zoom/--max-zoom or --from-id/--to-id, not both');
  }
  const regions = values.regions
    ? values.regions.split(',').map((region) => region.trim().toUpperCase())
    : REPLICATION_REGIONS;
  const prefix = values.prefix.replace(/^\/+|\/+$/g, '');
  const perAddress = Math.max(
    1,
    integer('max-connections-per-address', values['max-connections-per-address'], DEFAULT_MAX_CONNECTIONS_PER_ADDRESS),
  );
  const timeoutMs = integer('timeout-min', values['timeout-min'], 120) * 60_000;
  const intervalMs = integer('interval-s', values['interval-s'], 30) * 1000;
  const mismatchGraceMs = integer('mismatch-grace-s', values['mismatch-grace-s'], 300) * 1000;

  const storageOptions = storageOptionsFromEnv();
  if (values.addresses !== undefined) {
    try {
      storageOptions.addresses = parseAddresses(values.addresses);
    } catch (error) {
      throw new UsageError((error as Error).message);
    }
  }
  storageOptions.addresses ??= await resolveStorageAddresses(storageOptions.host);
  const concurrency = Math.max(
    1,
    Math.min(integer('concurrency', values.concurrency, 256), storageOptions.addresses.length * perAddress),
  );
  storageOptions.concurrency = concurrency;
  storageOptions.connectionsPerAddress = perAddress;
  const ranges = await ArchiveRanges.open(values.archive);
  const storage = new BunnyStorage(storageOptions);
  try {
    const { header } = ranges.archive;
    const { from, to } = byId
      ? { from: integer('from-id', values['from-id']), to: integer('to-id', values['to-id']) }
      : zoomRange(
          integer('min-zoom', values['min-zoom'], header.minZoom),
          integer('max-zoom', values['max-zoom'], header.maxZoom),
        );
    if (to <= from) {
      throw new UsageError(`Empty range [${from}, ${to})`);
    }
    const hashes = new ExpectedHashes(ranges);
    const seed = integer('seed', values.seed, Math.floor(Math.random() * 2 ** 31));
    const sampled =
      values.sample === undefined
        ? undefined
        : await sampleItems(ranges, hashes, prefix, {
            from,
            to,
            count: integer('sample', values.sample),
            perZoom: values['per-zoom'],
            next: mulberry32(seed),
          });
    const scope = {
      prefix,
      range: { from, to },
      mode: sampled ? (values['per-zoom'] ? 'sample-per-zoom' : 'sample') : 'range',
      seed: sampled ? seed : null,
      notInArchive: sampled?.notInArchive ?? 0,
      regions,
    };

    let code: number;
    let output: object;
    if (values.wait) {
      const items: ReplicationItem[] = sampled?.items ?? [];
      if (!sampled) {
        for await (const item of rangeItems(ranges, hashes, prefix, from, to)) {
          items.push(item);
        }
      }
      const result: ReplicationResult = await waitForReplication(storage, items, regions, {
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
      code = result.missing.length + result.mismatched.length + result.errors > 0 ? 1 : result.complete ? 0 : 2;
      output = { ...scope, files: items.length, ...result };
      process.stderr.write(
        `${result.complete ? 'Replicated' : 'Not replicated'} after ${Math.round(result.waitedMs / 1000)} s: ` +
          `${result.missing.length} missing, ${result.mismatched.length} mismatched, ${n(result.incomplete)} incomplete; ` +
          `region done at (s): ${regions.map((region) => `${region} ${result.regionWaitMs[region] === null ? '-' : Math.round(result.regionWaitMs[region]! / 1000)}`).join(' ')}\n`,
      );
    } else {
      const report = await checkReplication(
        storage,
        sampled?.items ?? rangeItems(ranges, hashes, prefix, from, to),
        regions,
        { concurrency, listLimit: integer('list', values.list, 100) },
      );
      code = report.missing + report.mismatched + report.errors > 0 ? 1 : report.partial > 0 ? 2 : 0;
      output = { ...scope, ...report };
      process.stderr.write(summarise(report, regions) + '\n');
    }
    const json = JSON.stringify(output);
    process.stdout.write(json + '\n');
    if (values.json) {
      await writeFile(values.json, json + '\n');
    }
    return code;
  } finally {
    await storage.close();
    await ranges.close();
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
