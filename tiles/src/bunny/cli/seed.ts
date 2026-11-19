/**
 * Uploads every tile position of a PMTiles archive, or a zoom or tile-id range of it, to the tiles storage zone as one
 * file each (`<prefix>/{z}/{x}/{y}.mvt`). Resumable: the same command again continues where it stopped and repairs
 * failed files, DESCRIBEing everything in chunks it had started before any PUT.
 *
 *   npm run bunny:seed -- --archive <path> [--prefix v1] [--min-zoom 0 --max-zoom 15 | --from-id N --to-id M]
 *     [--first-load | --verify-existing] [--concurrency 1200] [--workers 16] [--chunk-size 65536] [--state-dir <dir>]
 *     [--max-failures 1000] [--request-timeout-s 60] [--progress-interval-s 10] [--retry-base-ms 500]
 *     [--max-connections-per-address 240] [--addresses ip,ip,...] [--settle-ms 2000]
 *     [--state-prefix <p> | --no-state-check]
 *
 * --to-id is exclusive, and an id range must lie within the archive's tiles (its first tile id to one past its last).
 * Zooms default to the archive's; a zoom range is cut to the archive's tiles. The state dir defaults to
 * ./bunny-seed-<zone>-<prefix>-<from>-<to>. --concurrency is lowered to the storage addresses times
 * --max-connections-per-address (Bunny allows 250 per zone per storage server, shared by every tool working on the
 * zone at once: the default 240 leaves 10, which bunny:replication's default of 8 fits in; lower it to run anything
 * heavier alongside). --addresses (or BUNNY_STORAGE_ADDRESSES) pins the storage IPs instead of resolving the host,
 * e.g. to leave out a failing server. --retry-base-ms and --request-timeout-s must be at least 1.
 * --settle-ms is the least wait before DESCRIBEing a file whose PUT failed ambiguously (five times that after a
 * timeout); it is for tests: below the default, a PUT the server finishes late can be sent again with the same bytes,
 * which clears the file's replication.
 * Re-PUTting identical bytes clears a file's replication, so by default every position is DESCRIBEd before its PUT
 * and only files missing or different are written (--verify-existing asks for that explicitly). --first-load is for a
 * prefix and range nothing was uploaded to before this state dir's seed: the first load of a zone, or a full rewrite's
 * new prefix. Chunks the state dir has not started then go out without a DESCRIBE per file (half the requests). A few
 * checks back that up but can't prove it: a fresh state dir first DESCRIBEs about 16k positions spread over the range
 * (and the first of every chunk) and refuses if any is stored, a chunk not started runs verified if one of 64 of its
 * positions spread over it is stored already, and a prefix a sync made a build live under is refused; a stored stretch
 * between those samples is PUT again. Either way, a resume waits until PUTs the last run may have left in flight are
 * past the window in which the server may still commit them (request timeout + 5 x settle, up to about 70 s after a
 * kill) before it DESCRIBEs its unfinished chunks, and a seed refuses to start while another one on this machine, with
 * another state dir, covers an overlapping range of the same zone and prefix (claims live in $XDG_STATE_HOME/bunny-seed,
 * default ~/.local/state/bunny-seed; seeds on other machines are not seen).
 * Unless the addresses are pinned, the storage host is looked up again every 5 minutes and workers move to the new set.
 * The archive is identified by a hash of the whole file (archive-identity.ts; about 15 s for a planet build from NVMe,
 * at every start): the checkpoint refuses a resume with another archive, and the seed reads only the file that was
 * hashed, unchanged (it stops if the path is replaced or the file written to).
 * Once a sync has made a build live (state.json in the state zone), a seed under its prefix refuses any archive but the
 * live build's: positions a daily sync updated would get older bytes back, which no later diff repairs. So it reads
 * state.json first (BUNNY_STATE_ZONE and BUNNY_STATE_PASSWORD, --state-prefix as for bunny:sync); --no-state-check is
 * for a zone no sync uses. Seeding another prefix (a full rewrite) is fine.
 * Needs BUNNY_STORAGE_ZONE and BUNNY_STORAGE_PASSWORD, and BUNNY_STORAGE_HOST unless it is storage.bunnycdn.com.
 * Progress goes to stderr every 10 s and to <state dir>/status.json; the final status is printed to stdout as JSON.
 * Exit codes: 0 complete, 1 failed (rerun to repair), 2 bad arguments, 130 stopped by SIGINT/SIGTERM.
 */
import { parseArgs } from 'node:util';
import { ArchiveIdentity, identifyArchive } from '../archive-identity';
import { ArchiveRanges } from '../archive-walk';
import { BunnyStateStore, readState } from '../state';
import { parseAddresses, RetryOptions, storageOptionsFromEnv } from '../storage';
import { DEFAULT_PREFIX, prefixesOverlap, zoomRange } from '../tile-id';
import { DEFAULT_MAX_CONNECTIONS_PER_ADDRESS, formatDuration, runSeed } from '../upload';

class UsageError extends Error {}

function integer(name: string, value: string | undefined, fallback?: number, min = 0): number {
  if (value === undefined) {
    if (fallback === undefined) {
      throw new UsageError(`--${name} is required`);
    }
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < min) {
    throw new UsageError(`--${name} must be an integer of at least ${min}, got ${value}`);
  }
  return parsed;
}

/**
 * Refuses to seed over the live build's prefix with any other archive, and to take a seed over it for a first load
 * (see the header).
 */
async function checkLiveState(
  prefix: string,
  archive: ArchiveIdentity,
  statePrefix: string | undefined,
  firstLoad: boolean,
) {
  if (!process.env.BUNNY_STATE_ZONE || !process.env.BUNNY_STATE_PASSWORD) {
    throw new UsageError(
      'BUNNY_STATE_ZONE and BUNNY_STATE_PASSWORD are needed to check that the seed does not write over a live build ' +
        '(--no-state-check for a zone no sync uses)',
    );
  }
  const store = BunnyStateStore.fromEnv();
  const state = await readState(store, statePrefix).finally(() => store.close());
  if (!state || !prefixesOverlap(state.prefix, prefix)) {
    return;
  }
  if (firstLoad) {
    throw new Error(
      `${state.liveBuild} is live under ${state.prefix}/, so ${prefix}/ is no first load: leave out --first-load, ` +
        'and every position is DESCRIBEd before its PUT',
    );
  }
  if (!state.archive) {
    throw new Error(
      `${state.liveBuild} is live under ${state.prefix}/ and state.json records no v2 identity of its archive, so ` +
        'nothing says this archive is the one it came from. Seed another prefix, or let a sync record the live ' +
        'archive first',
    );
  }
  if (state.archive.identity !== archive.identity || state.archive.size !== archive.size) {
    throw new Error(
      `${state.liveBuild} is live under ${state.prefix}/ and this archive is not the one it came from: the seed would ` +
        "put other bytes back where daily syncs wrote. Repair with the live build's archive, or seed another prefix",
    );
  }
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
      concurrency: { type: 'string' },
      workers: { type: 'string' },
      'chunk-size': { type: 'string' },
      'state-dir': { type: 'string' },
      'first-load': { type: 'boolean', default: false },
      'verify-existing': { type: 'boolean', default: false },
      'max-failures': { type: 'string' },
      'request-timeout-s': { type: 'string' },
      'progress-interval-s': { type: 'string' },
      'retry-base-ms': { type: 'string' },
      'settle-ms': { type: 'string' },
      'max-connections-per-address': { type: 'string' },
      addresses: { type: 'string' },
      'state-prefix': { type: 'string' },
      'no-state-check': { type: 'boolean', default: false },
    },
    strict: true,
  });
  if (!values.archive) {
    throw new UsageError('--archive is required');
  }
  const byId = values['from-id'] !== undefined || values['to-id'] !== undefined;
  const byZoom = values['min-zoom'] !== undefined || values['max-zoom'] !== undefined;
  if (byId && byZoom) {
    throw new UsageError('Give either --min-zoom/--max-zoom or --from-id/--to-id, not both');
  }
  if (values['first-load'] && values['verify-existing']) {
    throw new UsageError('Give either --first-load or --verify-existing (the default), not both');
  }

  // Numbers first, so a typo fails before the archive is opened.
  const retry: Partial<RetryOptions> = {};
  if (values['retry-base-ms'] !== undefined) {
    // 0 would retry 429s in a tight loop.
    retry.baseDelayMs = integer('retry-base-ms', values['retry-base-ms'], undefined, 1);
  }
  if (values['settle-ms'] !== undefined) {
    retry.settleMs = integer('settle-ms', values['settle-ms']);
  }
  // 0 would turn off every request timeout (undici reads 0 as none), so a stop could wait on a request for good.
  const timeoutSeconds = integer('request-timeout-s', values['request-timeout-s'], 60, 1);
  const chunkSize = integer('chunk-size', values['chunk-size'], 65_536, 1);
  const workers = integer('workers', values.workers, 16, 1);
  const concurrency = integer('concurrency', values.concurrency, 1_200, 1);
  const maxConnectionsPerAddress = integer(
    'max-connections-per-address',
    values['max-connections-per-address'],
    DEFAULT_MAX_CONNECTIONS_PER_ADDRESS,
    1,
  );
  const maxFailures = integer('max-failures', values['max-failures'], 1_000, 1);
  const progressIntervalMs = integer('progress-interval-s', values['progress-interval-s'], 10, 1) * 1000;
  const prefix = values.prefix.replace(/^\/+|\/+$/g, '');
  const storage = storageOptionsFromEnv();
  if (values.addresses !== undefined) {
    try {
      storage.addresses = parseAddresses(values.addresses);
    } catch (error) {
      throw new UsageError((error as Error).message);
    }
  }

  const ranges = await ArchiveRanges.open(values.archive);
  const { header } = ranges.archive;
  const bounds = await ranges.bounds().finally(() => ranges.close());
  if (!bounds) {
    throw new UsageError(`${values.archive} has no tiles`);
  }
  const asked = byId
    ? { from: integer('from-id', values['from-id']), to: integer('to-id', values['to-id']) }
    : zoomRange(
        integer('min-zoom', values['min-zoom'], header.minZoom),
        integer('max-zoom', values['max-zoom'], header.maxZoom),
      );
  if (byId && (asked.from < bounds.first || asked.to > bounds.end)) {
    // Chunks before the archive's first tile or past its last would be checkpointed done with nothing in them.
    throw new UsageError(
      `[${asked.from}, ${asked.to}) reaches past the tiles of ${values.archive}, which has tiles in ` +
        `[${bounds.first}, ${bounds.end}) only`,
    );
  }
  const from = Math.max(asked.from, bounds.first);
  const to = Math.min(asked.to, bounds.end);
  if (from !== asked.from || to !== asked.to) {
    process.stderr.write(`The zooms' ids [${asked.from}, ${asked.to}) cut to the archive's tiles: [${from}, ${to})\n`);
  }
  if (to <= from) {
    throw new UsageError(
      `Empty range [${from}, ${to}): ${values.archive} has tiles in [${bounds.first}, ${bounds.end})`,
    );
  }
  const started = performance.now();
  process.stderr.write(`Hashing ${values.archive} (its identity for the checkpoint and the live state)\n`);
  const identified = await identifyArchive(values.archive);
  const archive = identified.identity;
  process.stderr.write(`Archive ${archive.identity} in ${formatDuration((performance.now() - started) / 1000)}\n`);
  if (!values['no-state-check']) {
    await checkLiveState(prefix, archive, values['state-prefix'], values['first-load']);
  }
  const safeName = (text: string) => text.replace(/[^\w.-]+/g, '_');

  const controller = new AbortController();
  let signals = 0;
  const onSignal = (signal: NodeJS.Signals) => {
    signals++;
    if (signals > 1) {
      // The checkpoint is already safe: unfinished chunks are verified on resume.
      process.stderr.write(`${signal} again: exiting now\n`);
      process.exit(130);
    }
    process.stderr.write(`${signal}: finishing the PUTs in flight (again to exit at once)\n`);
    controller.abort();
  };
  process.on('SIGINT', onSignal);
  process.on('SIGTERM', onSignal);

  const { status, failures } = await runSeed({
    archivePath: values.archive,
    prefix,
    from,
    to,
    chunkSize,
    workers,
    concurrency,
    stateDir: values['state-dir'] ?? `bunny-seed-${safeName(storage.zone)}-${safeName(prefix) || 'root'}-${from}-${to}`,
    firstLoad: values['first-load'],
    storage: { ...storage, timeoutMs: timeoutSeconds * 1000, retry },
    maxConnectionsPerAddress,
    maxFailures,
    progressIntervalMs,
    signal: controller.signal,
    archiveIdentity: identified,
  });

  const seconds = (Date.parse(status.updatedAt) - Date.parse(status.startedAt)) / 1000;
  process.stderr.write(
    `Seed ${status.state}: ${status.files.uploaded.toLocaleString('en-US')} uploaded, ` +
      `${status.files.skippedExisting.toLocaleString('en-US')} already stored, ${status.files.failed} failed, ` +
      `${status.chunks.done}/${status.chunks.total} chunks done, in ${formatDuration(seconds)} ` +
      `(${status.rate.overall.toLocaleString('en-US')}/s); 429s ${status.rateLimited}, retries ${status.retries.retries}; ` +
      `peak RSS ${Math.round(status.memory.peakRssBytes / 2 ** 20)} MiB\n`,
  );
  if (status.error) {
    process.stderr.write(`Error: ${status.error}\n`);
  }
  for (const failure of failures.slice(0, 20)) {
    process.stderr.write(`  failed ${failure.path}: ${failure.error}\n`);
  }
  if (status.files.failed > 20) {
    process.stderr.write(`  ... ${status.files.failed - 20} more in ${status.failuresFile}\n`);
  }
  process.stdout.write(JSON.stringify(status) + '\n');
  return status.state === 'complete' ? 0 : status.state === 'interrupted' ? 130 : 1;
}

main().then(
  (code) => {
    process.exitCode = code;
  },
  (error: unknown) => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = error instanceof UsageError ? 2 : 1;
  },
);
