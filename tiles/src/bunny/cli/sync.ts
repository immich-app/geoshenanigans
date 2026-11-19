/**
 * Syncs the tiles zone to a new build in place (see sync.ts), or starts the state with a seeded build:
 *
 *   npm run bunny:sync -- --archive <path> --build <YYYYMMDD> [--osm-date <ISO>] [--prefix <live prefix, or v1>]
 *     [--max-changed-fraction 0.05] [--replication-timeout-min 120] [--max-zoom Z] [--state-prefix <p>]
 *     [--dry-run] [--force-full | --adopt --seed-state <dir> [--seed-state <dir> ...] [--replace] [--archive-md5 B]]
 *     [--report <path.json>] [--styles-dir <dir>] [--work-dir <dir>] [--static-prefix v1] [--keep-manifests 7]
 *     [--threads N] [--concurrency 512] [--replication-interval-s 30] [--replication-concurrency 512]
 *     [--regions UK,SE,...] [--adopt-samples 10000] [--max-failures 1000] [--request-timeout-s 60] [--plain-puts]
 *
 * Without state.json it refuses unless --adopt (record a build bunny:seed uploaded, writing no tiles: every
 * --seed-state dir must say its seed completed, for this archive, zone and prefix, together over every position, and
 * --adopt-samples random positions must hold the build) or --force-full (write every position, each DESCRIBEd first,
 * and delete those the live or a pending build has and this one doesn't; for repairs and small zoom ranges). Under
 * the live build's key a run (a repair, a revert) refuses an archive whose tiles differ from the live manifest.
 * --adopt --replace switches a live zone to a build seeded under another --prefix (a full rewrite): the same checks,
 * the sample also in every region, then the TileJSON points at the new prefix.
 * Every run that writes first hashes the whole archive (its identity, archive-identity.ts), which state.json records
 * and --adopt compares with each seed's. A seed state dir an older bunny:seed left records only a v1 fingerprint
 * (header, root directory and size): --adopt takes it only with --archive-md5, the build's md5sum from builds.json
 * (base64), which it checks against the MD5 of the whole archive, and with --archive the very file the seed read (the
 * path in its status.json), unchanged since the seed began.
 * A run for the live build while state.json lists pending builds puts the live bytes back where they wrote (a revert
 * of a failed or cancelled sync). --dry-run builds the manifest and plans, then stops: it needs only the state zone.
 * Every changed position is
 * DESCRIBEd before its PUT; --plain-puts skips that where no pending build may have written (faster, but an identical
 * re-PUT clears a file's replication, so only when the zone surely holds the live build).
 * The replication gate waits for every replica region unless --regions lists some; --regions none only checks that
 * every changed file is stored with its bytes, for when the account's replication queue is known to be backed up (a
 * seed) and serving the files from the main region until they replicate is acceptable.
 *
 * Environment: BUNNY_STATE_ZONE and BUNNY_STATE_PASSWORD; unless --dry-run also BUNNY_STORAGE_ZONE,
 * BUNNY_STORAGE_PASSWORD, BUNNY_API_KEY, BUNNY_PULL_ZONE_ID and BUNNY_PUBLIC_URL; BUNNY_STORAGE_HOST unless it is
 * storage.bunnycdn.com, BUNNY_STORAGE_ADDRESSES to pin the storage IPs, STYLES_DIR (the map-styles build) unless
 * --styles-dir is given, and BUNNY_API_URL only to point the purge at a local stand-in. Manifests go to --work-dir
 * (default a new directory under TMPDIR, removed afterwards).
 * Everything a run needs is checked before it starts: an option it doesn't know, a flag given a value, an option
 * without one, a public URL the TileJSON can't carry, a style it can't serve or a pull zone id the purge can't use
 * fails the run before its first request, not after the tiles are written.
 *
 * The report (--report, and stdout) is written however the run ends, including a first SIGINT/SIGTERM, which stops
 * taking new files and lets the requests in flight finish (a second exits at once).
 * Exit codes: 0 success or skipped, 3 guard rail (nothing uploaded), 1 any failure.
 */
import { rmSync } from 'node:fs';
import { mkdir, mkdtemp, rename, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { parseArgs } from 'node:util';
import { parseMd5Base64 } from '../archive-identity';
import { purgePullZone, purgeUrl } from '../purge';
import { normalizeRegions } from '../replication';
import { BunnyStateStore } from '../state';
import { checkStaticFiles, readStyles } from '../static-files';
import { BunnyStorage, resolveStorageAddresses, storageOptionsFromEnv } from '../storage';
import { emptyReport, runSync, SYNC_DEFAULTS, SyncReport, SyncStep } from '../sync';

const ADDRESS_REFRESH_MS = 5 * 60_000;
/** Steps that write to the tiles zone: a stop lets their requests finish rather than exiting at once. */
const WRITING_STEPS: SyncStep[] = ['upload', 'static'];

const INTEGER = /^\d+$/;
const DECIMAL = /^(\d+\.?\d*|\.\d+)$/;

/** A number in plain decimal notation: not empty, hex, an exponent or padded, which Number() would all take. */
function number(name: string, value: string | undefined, min = 0, integer = true): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  const parsed = Number(value);
  if (
    !(integer ? INTEGER : DECIMAL).test(value) ||
    !Number.isFinite(parsed) ||
    parsed < min ||
    (integer && !Number.isSafeInteger(parsed))
  ) {
    throw new Error(`--${name} must be ${integer ? 'an integer' : 'a number'} of at least ${min}, got ${value}`);
  }
  return parsed;
}

/** Writes the report file atomically, one write after another, so the last one written is the last one asked for. */
class ReportFile {
  private chain = Promise.resolve();

  constructor(private path: string | undefined) {}

  write(report: SyncReport): Promise<void> {
    const path = this.path;
    if (!path) {
      return this.chain;
    }
    const body = `${JSON.stringify(report, null, 2)}\n`;
    this.chain = this.chain
      .then(async () => {
        await writeFile(`${path}.partial`, body);
        await rename(`${path}.partial`, path);
      })
      .catch((error: unknown) => {
        process.stderr.write(`Could not write the report: ${error instanceof Error ? error.message : String(error)}\n`);
      });
    return this.chain;
  }
}

const OPTIONS = {
  archive: { type: 'string' },
  build: { type: 'string' },
  'osm-date': { type: 'string' },
  prefix: { type: 'string' },
  'max-changed-fraction': { type: 'string' },
  'replication-timeout-min': { type: 'string' },
  'max-zoom': { type: 'string' },
  'state-prefix': { type: 'string' },
  'dry-run': { type: 'boolean', default: false },
  'force-full': { type: 'boolean', default: false },
  adopt: { type: 'boolean', default: false },
  replace: { type: 'boolean', default: false },
  'seed-state': { type: 'string', multiple: true },
  'archive-md5': { type: 'string' },
  'static-prefix': { type: 'string' },
  'keep-manifests': { type: 'string' },
  report: { type: 'string' },
  'styles-dir': { type: 'string' },
  'work-dir': { type: 'string' },
  threads: { type: 'string' },
  concurrency: { type: 'string' },
  'replication-interval-s': { type: 'string' },
  'replication-concurrency': { type: 'string' },
  regions: { type: 'string' },
  'adopt-samples': { type: 'string' },
  'max-failures': { type: 'string' },
  'request-timeout-s': { type: 'string' },
  'plain-puts': { type: 'boolean', default: false },
} as const;

const log = (line: string) => process.stderr.write(`[${new Date().toISOString()}] ${line}\n`);

/**
 * The options every decision of the run reads. Strict: an unknown option, a positional argument, a flag given a value
 * (`--dry-run=true`), an option missing its value or given an empty one is refused, never read as a default.
 */
function parseOptions() {
  const { values } = parseArgs({ options: OPTIONS, strict: true });
  for (const [name, value] of Object.entries(values)) {
    if ([value].flat().includes('')) {
      throw new Error(`--${name} needs a value`);
    }
  }
  return values;
}

async function main(): Promise<number> {
  // A permissive parse only to start the report, so a command line parseOptions refuses still leaves one that says
  // why. Nothing the run does reads it.
  const { values: given } = parseArgs({ options: OPTIONS, strict: false });
  const text = (name: keyof typeof OPTIONS) => (typeof given[name] === 'string' ? (given[name] as string) : undefined);
  const flag = (name: keyof typeof OPTIONS) => given[name] === true;
  const reportFile = new ReportFile(text('report'));
  let report = emptyReport({
    build: text('build') ?? '',
    prefix: text('prefix'),
    dryRun: flag('dry-run'),
    forceFull: flag('force-full'),
    adopt: flag('adopt'),
    replace: flag('replace'),
  });
  // A report from the start: a run killed outright still leaves one that says it failed.
  report.error = 'the sync did not finish';
  await reportFile.write(report);

  let storage: BunnyStorage | undefined;
  let state: BunnyStateStore | undefined;
  let ownWorkDir: string | undefined;
  let refresh: NodeJS.Timeout | undefined;
  try {
    const values = parseOptions();
    const { archive, build } = values;
    if (!archive || !build || !/^\d{8}$/.test(build)) {
      throw new Error('Usage: bunny:sync --archive <path> --build <YYYYMMDD> [options]; see src/bunny/cli/sync.ts');
    }
    const archiveMd5 = values['archive-md5'];
    if (archiveMd5 !== undefined) {
      parseMd5Base64(archiveMd5);
    }
    const osmDate = values['osm-date'];
    if (osmDate !== undefined && Number.isNaN(Date.parse(osmDate))) {
      throw new Error(`--osm-date must be an ISO date, got ${osmDate}`);
    }
    const dryRun = values['dry-run'];
    const requestTimeoutS = number('request-timeout-s', values['request-timeout-s'], 1) ?? 60;
    const concurrency = number('concurrency', values.concurrency, 1) ?? SYNC_DEFAULTS.concurrency;
    const replicationConcurrency =
      number('replication-concurrency', values['replication-concurrency'], 1) ?? SYNC_DEFAULTS.replicationConcurrency;
    const replicationTimeoutMin = number('replication-timeout-min', values['replication-timeout-min'], 0, false);
    if (replicationTimeoutMin === 0) {
      // The gate starts no DESCRIBE after its timeout, so 0 would fail every run that writes, after writing.
      throw new Error(
        '--replication-timeout-min must be more than 0: it bounds the whole gate, its first pass included',
      );
    }
    const numbers = {
      maxChangedFraction: number('max-changed-fraction', values['max-changed-fraction'], 0, false),
      replicationTimeoutMs: (replicationTimeoutMin ?? 120) * 60_000,
      replicationIntervalMs:
        (number('replication-interval-s', values['replication-interval-s'], 1, false) ?? 30) * 1000,
      maxZoom: number('max-zoom', values['max-zoom']),
      keepManifests: number('keep-manifests', values['keep-manifests']),
      adoptSamples: number('adopt-samples', values['adopt-samples'], 1),
      threads: number('threads', values.threads, 1),
      maxFailures: number('max-failures', values['max-failures'], 1),
    };
    const regionList = values.regions;
    // Trimmed, upper-cased and each once, as the zone names its replicas.
    const regions =
      regionList === undefined ? undefined : regionList === 'none' ? [] : normalizeRegions(regionList.split(','));
    if (regions?.length === 0 && regionList !== 'none') {
      throw new Error('--regions needs region codes, or "none" to check storage only');
    }

    let styles;
    let publicUrl: string | undefined;
    let purge: (() => Promise<unknown>) | undefined;
    if (!dryRun) {
      const stylesDir = values['styles-dir'] ?? process.env.STYLES_DIR;
      if (!stylesDir) {
        throw new Error('--styles-dir or STYLES_DIR (the map-styles build) is needed to write the TileJSON and styles');
      }
      styles = await readStyles(stylesDir);
      publicUrl = process.env.BUNNY_PUBLIC_URL;
      if (!publicUrl) {
        throw new Error('BUNNY_PUBLIC_URL must be set');
      }
      // The TileJSON and styles are written after the tiles, and the purge after the gate: what either would refuse
      // must stop the run here, not leave a pending build and changed tiles behind.
      checkStaticFiles({ publicUrl, styles, prefix: values.prefix, staticPrefix: values['static-prefix'] });
      const apiKey = process.env.BUNNY_API_KEY;
      const pullZoneId = process.env.BUNNY_PULL_ZONE_ID;
      if (!apiKey || !pullZoneId) {
        throw new Error('BUNNY_API_KEY and BUNNY_PULL_ZONE_ID must be set');
      }
      const apiUrl = process.env.BUNNY_API_URL || undefined;
      purgeUrl(pullZoneId, apiUrl);
      purge = async () => {
        const result = await purgePullZone(apiKey, pullZoneId, { baseUrl: apiUrl });
        log(`purge: HTTP ${result.status} after ${result.attempts} attempt(s) in ${result.ms} ms`);
      };
      const options = storageOptionsFromEnv();
      storage = new BunnyStorage({
        ...options,
        concurrency: Math.max(concurrency, replicationConcurrency),
        timeoutMs: requestTimeoutS * 1000,
      });
      if (!options.addresses) {
        // The replication gate can run for hours: follow the storage host's addresses as they change.
        const tiles = storage;
        refresh = setInterval(() => {
          resolveStorageAddresses(options.host)
            .then((addresses) => tiles.setAddresses(addresses))
            .catch(() => {});
        }, ADDRESS_REFRESH_MS);
      }
    }
    state = BunnyStateStore.fromEnv();

    let workDir = values['work-dir'];
    if (workDir) {
      await mkdir(workDir, { recursive: true });
    } else {
      workDir = ownWorkDir = await mkdtemp(join(tmpdir(), 'bunny-sync-'));
    }

    const controller = new AbortController();
    let current: SyncStep | undefined;
    let signals = 0;
    const onSignal = (signal: NodeJS.Signals) => {
      signals++;
      const stopped = { ...report, status: 'failed' as const, error: `stopped by ${signal}` };
      if (signals > 1 || !current || !WRITING_STEPS.includes(current)) {
        process.stderr.write(`${signal}: exiting\n`);
        void reportFile.write(stopped).finally(() => {
          if (ownWorkDir) {
            rmSync(ownWorkDir, { recursive: true, force: true });
          }
          process.exit(1);
        });
        return;
      }
      process.stderr.write(`${signal}: finishing the requests in flight (again to exit at once)\n`);
      void reportFile.write(stopped);
      controller.abort(signal);
    };
    process.on('SIGINT', onSignal);
    process.on('SIGTERM', onSignal);

    report = await runSync(
      {
        archive,
        build,
        osmDate,
        prefix: values.prefix,
        ...numbers,
        replicationConcurrency,
        regions,
        statePrefix: values['state-prefix'],
        dryRun,
        forceFull: values['force-full'],
        adopt: values.adopt,
        replace: values.replace,
        seedStates: values['seed-state'] ?? [],
        archiveMd5,
        staticPrefix: values['static-prefix'],
        plainPuts: values['plain-puts'],
        publicUrl,
        styles,
        workDir,
        concurrency,
        signal: controller.signal,
        log,
        onStep: (step) => (current = step),
        onReport: (latest) => {
          report = latest;
          void reportFile.write({ ...latest, error: latest.error ?? 'the sync did not finish' });
        },
      },
      { state, tiles: storage, purge },
    );
  } catch (error) {
    report.status = 'failed';
    report.error = error instanceof Error ? error.message : String(error);
  } finally {
    clearInterval(refresh);
    await storage?.close();
    await state?.close();
    if (ownWorkDir) {
      await rm(ownWorkDir, { recursive: true, force: true });
    }
  }
  await reportFile.write(report);
  process.stdout.write(`${JSON.stringify(report)}\n`);
  if (report.error) {
    process.stderr.write(`Sync ${report.status}: ${report.error}\n`);
  }
  return report.status === 'success' || report.status === 'skipped' ? 0 : report.status === 'guard_rail' ? 3 : 1;
}

main().then(
  (code) => {
    process.exitCode = code;
  },
  (error: unknown) => {
    process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
    process.exitCode = 1;
  },
);
