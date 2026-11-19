/**
 * Uploads every tile position of a PMTiles archive, or a zoom or tile-id range of it, to the tiles storage zone as one
 * file each (`<prefix>/{z}/{x}/{y}.mvt`). Resumable: the same command again continues where it stopped and repairs
 * failed files, DESCRIBEing everything in chunks it had started before any PUT.
 *
 *   npm run bunny:seed -- --archive <path> [--prefix v1] [--min-zoom 0 --max-zoom 15 | --from-id N --to-id M]
 *     [--concurrency 1200] [--workers 16] [--chunk-size 65536] [--state-dir <dir>] [--verify-existing]
 *     [--max-failures 1000] [--request-timeout-s 60] [--progress-interval-s 10] [--retry-base-ms 500]
 *     [--max-connections-per-address 240] [--addresses ip,ip,...] [--settle-ms 2000]
 *
 * --to-id is exclusive. Zooms default to the archive's. The state dir defaults to
 * ./bunny-seed-<zone>-<prefix>-<from>-<to>. --concurrency is lowered to the storage addresses times
 * --max-connections-per-address (Bunny allows 250 per zone per storage server, shared by every tool working on the
 * zone at once: the default 240 leaves 10, which bunny:replication's default of 8 fits in; lower it to run anything
 * heavier alongside). --addresses (or BUNNY_STORAGE_ADDRESSES) pins the storage IPs instead of resolving the host,
 * e.g. to leave out a failing server. --retry-base-ms and --request-timeout-s must be at least 1.
 * --settle-ms is the least wait before DESCRIBEing a file whose PUT failed ambiguously (five times that after a
 * timeout); it is for tests: below the default, a PUT the server finishes late can be sent again with the same bytes,
 * which clears the file's replication.
 * Re-PUTting identical bytes clears a file's replication, so: a resume waits until PUTs the last run may have left
 * in flight are past the window in which the server may still commit them (request timeout + 5 x settle, up to about
 * 70 s after a kill) before it DESCRIBEs its unfinished chunks; a chunk that was not started is checked at 64 spread
 * positions and runs verified if any is stored already (another seed); and a seed refuses to start while another one
 * on this machine, with another state dir, covers an overlapping range of the same zone and prefix (claims live in
 * $XDG_STATE_HOME/bunny-seed, default ~/.local/state/bunny-seed; seeds on other machines are not seen).
 * Unless the addresses are pinned, the storage host is looked up again every 5 minutes and workers move to the new set.
 * Needs BUNNY_STORAGE_ZONE and BUNNY_STORAGE_PASSWORD, and BUNNY_STORAGE_HOST unless it is storage.bunnycdn.com.
 * Progress goes to stderr every 10 s and to <state dir>/status.json; the final status is printed to stdout as JSON.
 * Exit codes: 0 complete, 1 failed (rerun to repair), 2 bad arguments, 130 stopped by SIGINT/SIGTERM.
 */
import { parseArgs } from 'node:util';
import { ArchiveRanges } from '../archive-walk';
import { parseAddresses, RetryOptions, storageOptionsFromEnv } from '../storage';
import { DEFAULT_PREFIX, zoomRange } from '../tile-id';
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
      'verify-existing': { type: 'boolean', default: false },
      'max-failures': { type: 'string' },
      'request-timeout-s': { type: 'string' },
      'progress-interval-s': { type: 'string' },
      'retry-base-ms': { type: 'string' },
      'settle-ms': { type: 'string' },
      'max-connections-per-address': { type: 'string' },
      addresses: { type: 'string' },
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

  const header = await ArchiveRanges.open(values.archive).then(async (ranges) => {
    await ranges.close();
    return ranges.archive.header;
  });
  const { from, to } = byId
    ? { from: integer('from-id', values['from-id']), to: integer('to-id', values['to-id']) }
    : zoomRange(
        integer('min-zoom', values['min-zoom'], header.minZoom),
        integer('max-zoom', values['max-zoom'], header.maxZoom),
      );
  if (to <= from) {
    throw new UsageError(`Empty range [${from}, ${to})`);
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
    verifyExisting: values['verify-existing'],
    storage: { ...storage, timeoutMs: timeoutSeconds * 1000, retry },
    maxConnectionsPerAddress,
    maxFailures,
    progressIntervalMs,
    signal: controller.signal,
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
