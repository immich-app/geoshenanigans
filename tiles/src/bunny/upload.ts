/**
 * Uploading tile positions: the uploader core (any source of tiles, one thread), the seed checkpoint, and the seed
 * engine that walks an archive's tile-id range in chunks and uploads them from worker threads.
 */
import { createHash } from 'node:crypto';
import { FileHandle, mkdir, open, readFile, rename, rm, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { join, resolve } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { Worker } from 'node:worker_threads';
import { ArchiveRanges } from './archive-walk';
import { forEachLimit } from './replication';
import { claimRange, defaultClaimsDir, isAlive } from './seed-claims';
import {
  addCounters,
  BunnyStorage,
  BunnyStorageOptions,
  DEFAULT_RETRY,
  DEFAULT_TIMEOUT_MS,
  newCounters,
  resolveStorageAddresses,
  StorageCounters,
  StorageError,
  storageOrigin,
} from './storage';
import { DEFAULT_PREFIX, tileIdZoom, tilePath } from './tile-id';

export interface UploadItem {
  tileId: number;
  bytes: Uint8Array;
  /** Hex SHA-256 of `bytes`. */
  sha256: string;
  /** DESCRIBE first and skip the PUT if the stored checksum already matches. */
  verify?: boolean;
}

export interface UploadFailure {
  tileId: number;
  path: string;
  error: string;
}

export interface UploadStats {
  uploaded: number;
  skippedExisting: number;
  retries: number;
  rateLimited: number;
  failed: number;
  bytes: number;
  seconds: number;
  /** The first MAX_REPORTED_FAILURES failures. */
  failures: UploadFailure[];
  /**
   * Not every item was settled: stopped by the signal (including items the storage client's signal stopped mid-retry,
   * which are neither uploaded nor failed), a fatal error or too many failures.
   */
  stopped: boolean;
  /** Why the upload stopped early, if it did on its own. */
  error?: string;
}

export type UploadOutcome = 'uploaded' | 'skipped' | 'failed';

export interface UploadOptions<T extends UploadItem> {
  prefix?: string;
  /** Files in flight at most (default 64). */
  concurrency?: number;
  /** DESCRIBE every file before its PUT; items can ask for it one by one, but cannot opt out of this. */
  verify?: boolean;
  /** Stop taking new items; the ones in flight finish. */
  signal?: AbortSignal;
  /** Stop after this many failed files (default 1000). */
  maxFailures?: number;
  onSettled?: (item: T, outcome: UploadOutcome, failure?: UploadFailure) => void;
  /** Updated as files finish, for progress reports while the upload runs. */
  stats?: UploadStats;
}

export const MAX_REPORTED_FAILURES = 1000;

export const newUploadStats = (): UploadStats => ({
  uploaded: 0,
  skippedExisting: 0,
  retries: 0,
  rateLimited: 0,
  failed: 0,
  bytes: 0,
  seconds: 0,
  failures: [],
  stopped: false,
});

export const sha256Hex = (bytes: Uint8Array) => createHash('sha256').update(bytes).digest('hex').toUpperCase();

/**
 * Uploads every item to `${prefix}/${z}/${x}/${y}.mvt` with at most `concurrency` in flight. A failed file is
 * recorded and the rest carry on; a fatal storage error (credentials) or too many failures stop the upload.
 */
export async function uploadTiles<T extends UploadItem>(
  storage: BunnyStorage,
  source: AsyncIterable<T> | Iterable<T>,
  options: UploadOptions<T> = {},
): Promise<UploadStats> {
  const prefix = options.prefix ?? DEFAULT_PREFIX;
  const maxFailures = options.maxFailures ?? 1000;
  const stats = options.stats ?? newUploadStats();
  const before = { ...storage.counters };
  const startedAt = performance.now();
  const iterator =
    Symbol.asyncIterator in source ? source[Symbol.asyncIterator]() : (source as Iterable<T>)[Symbol.iterator]();
  let exhausted = false;
  let halted = false;
  let unsettled = 0;
  const stopping = () => halted || options.signal?.aborted === true;

  const uploadOne = async (item: T) => {
    const path = tilePath(prefix, item.tileId);
    try {
      if (options.verify || item.verify) {
        const stored = await storage.describe(path);
        if (stored?.checksum === item.sha256.toUpperCase()) {
          stats.skippedExisting++;
          options.onSettled?.(item, 'skipped');
          return;
        }
      }
      await storage.put(path, item.bytes, item.sha256);
      stats.uploaded++;
      stats.bytes += item.bytes.length;
      options.onSettled?.(item, 'uploaded');
    } catch (error) {
      if (error instanceof StorageError && error.stopped) {
        // Maybe stored, maybe not: the caller verifies it next time.
        unsettled++;
        return;
      }
      const message = error instanceof Error ? error.message : String(error);
      const failure = { tileId: item.tileId, path, error: message };
      stats.failed++;
      if (stats.failures.length < MAX_REPORTED_FAILURES) {
        stats.failures.push(failure);
      }
      options.onSettled?.(item, 'failed', failure);
      if (error instanceof StorageError && error.fatal) {
        stats.error ??= message;
        halted = true;
      } else if (stats.failed >= maxFailures) {
        stats.error ??= `${stats.failed} files failed`;
        halted = true;
      }
    }
  };

  const runner = async () => {
    while (!stopping()) {
      const next = await iterator.next();
      if (next.done) {
        exhausted = true;
        return;
      }
      await uploadOne(next.value);
    }
  };
  const results = await Promise.allSettled(Array.from({ length: Math.max(1, options.concurrency ?? 64) }, runner));
  stats.seconds = (performance.now() - startedAt) / 1000;
  stats.retries += storage.counters.retries - before.retries;
  stats.rateLimited += storage.counters.rateLimited - before.rateLimited;
  stats.stopped = !exhausted || unsettled > 0;
  const failure = results.find((result): result is PromiseRejectedResult => result.status === 'rejected');
  if (failure) {
    throw failure.reason;
  }
  return stats;
}

// --- Checkpoint -------------------------------------------------------------------------------------------------

export interface SeedParams {
  /** Storage zone and canonical storage origin: a checkpoint says nothing about what another zone holds. */
  zone: string;
  host: string;
  prefix: string;
  from: number;
  to: number;
  chunkSize: number;
  archive: { size: number; fingerprint: string };
}

const CHECKPOINT_LINE = /^(start|done) (\d+)$/;

/**
 * The seed's resume state in its state dir: seed.json (what is being seeded) and checkpoint.log, an append-only log
 * of `start <chunk>` (written and fsynced before the chunk's first PUT) and `done <chunk>` (after its last).
 * A chunk with a start but no done may hold some of its files, so a resume DESCRIBEs them before any PUT.
 * seed.lock holds the pid of the process using the dir: two seeds on one dir would PUT the same chunks twice.
 */
export class Checkpoint {
  private writes: Promise<void> = Promise.resolve();
  /** Set by a failed append, which may have left part of a line: a later line would join it into one bad line. */
  private failed?: Error;

  private constructor(
    private handle: FileHandle,
    private lockPath: string,
    readonly done: Set<number>,
    readonly started: Set<number>,
  ) {}

  /** True when nothing of this seed has been started yet. */
  get fresh(): boolean {
    return this.started.size === 0 && this.done.size === 0;
  }

  static async open(dir: string, params: SeedParams): Promise<Checkpoint> {
    await mkdir(dir, { recursive: true });
    const lockPath = await lock(dir);
    try {
      return await Checkpoint.read(dir, params, lockPath);
    } catch (error) {
      await rm(lockPath, { force: true });
      throw error;
    }
  }

  private static async read(dir: string, params: SeedParams, lockPath: string): Promise<Checkpoint> {
    const paramsPath = join(dir, 'seed.json');
    const logPath = join(dir, 'checkpoint.log');
    const existing = await readIfExists(paramsPath);
    const text = (await readIfExists(logPath)) ?? '';
    if (existing === undefined) {
      if (text.trim() !== '') {
        throw new Error(
          `${logPath} has entries but seed.json is missing, so nothing says which seed they belong to; ` +
            'use another --state-dir',
        );
      }
      await writeAtomic(paramsPath, JSON.stringify(params, null, 2) + '\n', true);
    } else {
      const previous = JSON.parse(existing) as SeedParams;
      const mismatch = (['zone', 'host', 'prefix', 'from', 'to', 'chunkSize'] as const).filter(
        (key) => previous[key] !== params[key],
      );
      if (previous.archive?.fingerprint !== params.archive.fingerprint) {
        mismatch.push('archive' as never);
      }
      if (mismatch.length > 0) {
        throw new Error(
          `${dir} holds the checkpoint of a different seed (${mismatch.join(', ')} differ); use another --state-dir`,
        );
      }
    }

    // A torn last line (killed mid-write) never reached its fsync, so its chunk was never dispatched or never done.
    const complete = text.slice(0, text.lastIndexOf('\n') + 1);
    const done = new Set<number>();
    const started = new Set<number>();
    for (const line of complete.split('\n').filter(Boolean)) {
      const match = CHECKPOINT_LINE.exec(line);
      if (!match) {
        throw new Error(`${logPath}: unreadable line ${JSON.stringify(line)}`);
      }
      (match[1] === 'start' ? started : done).add(Number(match[2]));
    }
    const handle = await open(logPath, 'a+');
    if (complete.length < text.length) {
      await handle.truncate(Buffer.byteLength(complete));
      await handle.sync();
    }
    return new Checkpoint(handle, lockPath, done, started);
  }

  /** Records that chunks are about to be uploaded; resolves once that is on disk. */
  markStarted(chunkIds: number[]): Promise<void> {
    chunkIds.forEach((id) => this.started.add(id));
    return this.append(chunkIds.map((id) => `start ${id}\n`).join(''));
  }

  markDone(chunkId: number): Promise<void> {
    this.done.add(chunkId);
    return this.append(`done ${chunkId}\n`);
  }

  async close(): Promise<void> {
    await this.writes;
    await this.handle.close();
    await rm(this.lockPath, { force: true });
  }

  private append(text: string): Promise<void> {
    const write = this.writes.then(async () => {
      if (this.failed) {
        throw new Error(`checkpoint.log takes no more lines: an earlier write failed (${this.failed.message})`);
      }
      try {
        // One write(2) may take part of the bytes (a nearly full disk); the line counts only once all of it is out.
        const bytes = Buffer.from(text);
        for (let done = 0; done < bytes.length;) {
          const { bytesWritten } = await this.handle.write(bytes, done, bytes.length - done);
          if (bytesWritten <= 0) {
            throw new Error('write took no bytes');
          }
          done += bytesWritten;
        }
        await this.handle.sync();
      } catch (error) {
        // The next open drops the torn last line.
        this.failed = error instanceof Error ? error : new Error(String(error));
        throw this.failed;
      }
    });
    this.writes = write.catch(() => {});
    return write;
  }
}

/** Takes the state dir's lock, or fails if a live process holds it (a stale lock from a killed run is taken over). */
async function lock(dir: string): Promise<string> {
  const path = join(dir, 'seed.lock');
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      await writeFile(path, `${process.pid}\n`, { flag: 'wx' });
      return path;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'EEXIST') {
        throw error;
      }
    }
    const holder = Number((await readIfExists(path))?.trim());
    if (Number.isInteger(holder) && holder > 0 && holder !== process.pid && isAlive(holder)) {
      throw new Error(`${dir} is in use by process ${holder}; stop it first or use another --state-dir`);
    }
    await rm(path, { force: true });
  }
  throw new Error(`Could not take the lock in ${dir}`);
}

async function readIfExists(path: string): Promise<string | undefined> {
  try {
    return await readFile(path, 'utf8');
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return undefined;
    }
    throw error;
  }
}

let temporaryFiles = 0;

async function writeAtomic(path: string, text: string, durable = false) {
  const temporary = `${path}.${process.pid}.${++temporaryFiles}.tmp`;
  if (durable) {
    const handle = await open(temporary, 'w');
    await handle.writeFile(text);
    await handle.sync();
    await handle.close();
  } else {
    await writeFile(temporary, text);
  }
  await rename(temporary, path);
}

// --- Seed engine ------------------------------------------------------------------------------------------------

/** A chunk of consecutive tile ids handed to a worker, with the runs covering it clipped to [start, end). */
export interface ChunkDescriptor {
  chunkId: number;
  start: number;
  end: number;
  verify: boolean;
  /** Per run: tileId, runLength, offset, length. */
  runs: Float64Array;
}

/** Chunks of the global grid `chunkId * chunkSize` that overlap [from, to), skipping those `skip` names. */
export async function* planChunks(
  ranges: ArchiveRanges,
  from: number,
  to: number,
  chunkSize: number,
  skip: (chunkId: number) => boolean = () => false,
): AsyncGenerator<Omit<ChunkDescriptor, 'verify'>> {
  const runs = ranges.runs(from, to);
  let current = (await runs.next()).value;
  for (let chunkId = Math.floor(from / chunkSize); chunkId * chunkSize < to; chunkId++) {
    const start = Math.max(from, chunkId * chunkSize);
    const end = Math.min(to, (chunkId + 1) * chunkSize);
    const skipped = skip(chunkId);
    const parts: number[] = [];
    while (current && current.tileId < end) {
      const first = Math.max(current.tileId, start);
      const last = Math.min(current.tileId + current.runLength, end);
      if (!skipped && last > first) {
        parts.push(first, last - first, current.offset, current.length);
      }
      if (current.tileId + current.runLength > end) {
        break;
      }
      current = (await runs.next()).value;
    }
    if (!skipped) {
      yield { chunkId, start, end, runs: Float64Array.from(parts) };
    }
  }
}

export interface SeedOptions {
  archivePath: string;
  prefix: string;
  from: number;
  to: number;
  chunkSize: number;
  workers: number;
  /** PUTs in flight at most, across all workers. */
  concurrency: number;
  stateDir: string;
  /** DESCRIBE before every PUT, not just in chunks a previous run left unfinished. */
  verifyExisting: boolean;
  storage: Omit<BunnyStorageOptions, 'counters' | 'concurrency' | 'connectionsPerAddress' | 'signal'>;
  /** Connections to one storage address across all workers (default 240); caps the total in flight too. */
  maxConnectionsPerAddress?: number;
  maxFailures?: number;
  progressIntervalMs?: number;
  signal?: AbortSignal;
  log?: (line: string) => void;
  /** Where seeds on this machine record the ranges they are seeding (default: defaultClaimsDir()). */
  claimsDir?: string;
  /** How often the storage host is looked up again, unless storage.addresses pins them (default 5 min). */
  addressRefreshMs?: number;
  /** Looks up the storage host's addresses (default: resolveStorageAddresses). */
  resolveAddresses?: (host?: string) => Promise<string[]>;
}

export type SeedState = 'running' | 'stopping' | 'complete' | 'failed' | 'interrupted';

export interface SeedStatus {
  state: SeedState;
  startedAt: string;
  updatedAt: string;
  archive: string;
  prefix: string;
  range: { from: number; to: number };
  chunkSize: number;
  workers: number;
  /** Requests in flight at most: --concurrency, lowered to what the addresses' connection cap allows. */
  concurrency: number;
  /** `overlapping`: chunks found partly stored already with the same bytes (another seed), so verified. */
  chunks: { total: number; done: number; previouslyDone: number; active: number; failed: number; overlapping: number };
  files: {
    total: number;
    done: number;
    previouslyDone: number;
    uploaded: number;
    skippedExisting: number;
    failed: number;
  };
  bytes: number;
  rate: { oneMinute: number; overall: number };
  inFlight: number;
  retries: StorageCounters;
  rateLimited: number;
  etaSeconds: number | null;
  current: { tileId: number; zoom: number } | null;
  memory: { rssBytes: number; peakRssBytes: number };
  /** The storage addresses requests go to. */
  addresses: string[];
  /**
   * When every PUT this run sent, up to its next status write, is past the window in which the server may still
   * commit it (request timeout + 5 x settle). A resume waits for it before DESCRIBEing unfinished chunks, so it reads
   * what such a late commit stored instead of "missing" (and PUTting the same bytes again).
   */
  settleBy: string;
  failuresFile: string;
  error: string | null;
}

export interface SeedResult {
  status: SeedStatus;
  failures: UploadFailure[];
}

export interface WorkerConfig {
  archivePath: string;
  tileDataOffset: number;
  prefix: string;
  concurrency: number;
  storage: BunnyStorageOptions;
  maxFailures: number;
}

export type WorkerMessage =
  | { type: 'progress'; stats: UploadStats; counters: StorageCounters; inFlight: number }
  /** Every position of the chunk was handed to the uploader; some may still be in flight. */
  | { type: 'chunk-emitted'; chunkId: number }
  /** Sampled positions of a chunk that was not started were stored already (`reason`), so it runs verified. */
  | { type: 'chunk-overlap'; chunkId: number; reason: string }
  | { type: 'chunk-done'; chunkId: number; failures: UploadFailure[] }
  | { type: 'chunk-stopped'; chunkId: number }
  | { type: 'fatal'; error: string }
  | { type: 'final'; stats: UploadStats; counters: StorageCounters };

export type MainMessage =
  | { type: 'chunk'; chunk: ChunkDescriptor }
  | { type: 'addresses'; addresses: string[] }
  | { type: 'end' }
  | { type: 'stop' };

/**
 * Chunks a worker holds that still have positions to hand out: the one it is on and one queued, so the next starts
 * while the last positions of this one are in flight.
 */
const PREFETCH = 2;
/** Chunks a worker holds at most, counting those waiting only on a few slow files, so stragglers do not idle it. */
const MAX_OUTSTANDING = 8;
/** Bunny allows 250 connections per zone per storage server. */
export const DEFAULT_MAX_CONNECTIONS_PER_ADDRESS = 240;
const PREFLIGHT_HEAD = 24;
const PREFLIGHT_MAX_SAMPLES = 16_384;
const PREFLIGHT_MAX_CHUNK_STARTS = 65_536;
const PREFLIGHT_CONCURRENCY = 128;
/** Positions DESCRIBEd before a chunk that was not started goes out: 0.1% of a 65,536-position chunk. */
export const CHUNK_PROBE_SAMPLES = 64;
const DEFAULT_ADDRESS_REFRESH_MS = 5 * 60_000;
/** Longest a resume waits for the previous run's PUTs to settle, whatever its status.json says. */
const MAX_SETTLE_WAIT_MS = 15 * 60_000;
const OVERLAP_LOG_LINES = 20;

interface WorkerSlot {
  worker: Worker;
  outstanding: Set<number>;
  emitting: Set<number>;
  stats: UploadStats;
  counters: StorageCounters;
  inFlight: number;
  exited: Promise<void>;
  dead: boolean;
}

/**
 * Uploads every position of [from, to) of an archive as one file each, resumable from `stateDir`. The main thread
 * walks the directories and hands out chunk descriptors; workers read the tile bytes themselves (pread) and PUT them
 * over their own connection pools, so tile bytes never cross threads and memory stays bounded by the chunks in flight.
 */
export async function runSeed(options: SeedOptions): Promise<SeedResult> {
  const log = options.log ?? ((line: string) => process.stderr.write(line + '\n'));
  const { prefix, from, to, chunkSize, stateDir } = options;
  if (!(from >= 0 && to > from && chunkSize > 0)) {
    throw new Error(`Bad seed range [${from}, ${to}) / chunk size ${chunkSize}`);
  }
  const ranges = await ArchiveRanges.open(options.archivePath);
  let checkpoint: Checkpoint | undefined;
  let unclaim: (() => Promise<void>) | undefined;
  try {
    const host = storageOrigin(options.storage.host);
    checkpoint = await Checkpoint.open(stateDir, {
      zone: options.storage.zone,
      host,
      prefix,
      from,
      to,
      chunkSize,
      archive: { size: ranges.size, fingerprint: ranges.fingerprint },
    });
    unclaim = await claimRange(options.claimsDir ?? defaultClaimsDir(), {
      stateDir: resolve(stateDir),
      zone: options.storage.zone,
      host,
      prefix,
      from,
      to,
    });
    const addresses = options.storage.addresses?.length
      ? options.storage.addresses
      : await (options.resolveAddresses ?? resolveStorageAddresses)(options.storage.host);
    const storageOptions = { ...options.storage, addresses };
    const unfinished = [...checkpoint.started].filter((id) => !checkpoint!.done.has(id)).length;
    if (unfinished > 0) {
      await settlePrevious(options, unfinished, log);
    }
    const perAddress = Math.max(1, options.maxConnectionsPerAddress ?? DEFAULT_MAX_CONNECTIONS_PER_ADDRESS);
    if (checkpoint.fresh && !options.verifyExisting) {
      const ids = preflightIds(from, to, chunkSize);
      log(`Fresh state dir: checking that none of ${ids.length} positions spread over the range is stored already`);
      const concurrency = Math.max(
        1,
        Math.min(PREFLIGHT_CONCURRENCY, options.concurrency, addresses.length * perAddress),
      );
      const connectionsPerAddress = Math.min(perAddress, Math.ceil(concurrency / addresses.length));
      await preflight(ranges, { ...storageOptions, concurrency, connectionsPerAddress }, prefix, ids);
    }
    return await new SeedRun(options, ranges, checkpoint, storageOptions, log).run();
  } finally {
    await unclaim?.();
    await checkpoint?.close();
    await ranges.close();
  }
}

/** Request timeout + 5 x settle: how long after sending a PUT put() itself allows the server to commit it. */
function commitWindowMs(options: SeedOptions): number {
  const timeoutMs = options.storage.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  return timeoutMs + 5 * (options.storage.retry?.settleMs ?? DEFAULT_RETRY.settleMs);
}

/**
 * A run that was stopped or killed may have left PUTs the server commits after it is gone (put() would have waited
 * for them before its DESCRIBE). Verifying their chunks before then reads "missing" and PUTs the same bytes a second
 * time, so a resume first waits for the `settleBy` the previous run recorded (the full window from now if unknown).
 */
async function settlePrevious(options: SeedOptions, unfinished: number, log: (line: string) => void) {
  const previous = await readFile(join(options.stateDir, 'status.json'), 'utf8')
    .then((text) => JSON.parse(text) as Partial<SeedStatus>)
    .catch(() => undefined);
  const recorded = Date.parse(previous?.settleBy ?? '');
  const now = Date.now();
  const until = Number.isFinite(recorded)
    ? Math.min(recorded, now + MAX_SETTLE_WAIT_MS)
    : now + (options.progressIntervalMs ?? 10_000) + commitWindowMs(options);
  if (until > now && !options.signal?.aborted) {
    log(
      `Waiting ${Math.ceil((until - now) / 1000)} s before verifying the ${unfinished} chunks the last run left ` +
        'unfinished: PUTs it had in flight may still be committed',
    );
    await sleep(until - now, undefined, { signal: options.signal }).catch(() => {});
  }
}

/**
 * The positions a fresh seed checks before it starts: the first few of the range, the first position of every chunk
 * (an earlier run with this chunk size uploaded each chunk it started from there), and about 16k spread evenly, for
 * earlier runs over other chunk sizes or id ranges: any stretch of the range they stored at least that spacing long
 * holds one of them.
 */
export function preflightIds(from: number, to: number, chunkSize: number): number[] {
  const ids = new Set<number>();
  for (let id = from; id < Math.min(to, from + PREFLIGHT_HEAD); id++) {
    ids.add(id);
  }
  if ((to - from) / chunkSize <= PREFLIGHT_MAX_CHUNK_STARTS) {
    for (let id = Math.ceil(from / chunkSize) * chunkSize; id < to; id += chunkSize) {
      ids.add(id);
    }
  }
  const stride = Math.max(1, Math.ceil((to - from) / PREFLIGHT_MAX_SAMPLES));
  for (let id = from; id < to; id += stride) {
    ids.add(id);
  }
  ids.add(to - 1);
  return [...ids].sort((a, b) => a - b);
}

/**
 * The positions DESCRIBEd before a chunk that was not started goes out: its first and last and an even spread between,
 * `samples` in all. Another seed that stored part of the chunk shows up there unless its part is shorter than
 * (end - start) / samples and touches neither end.
 */
export function chunkProbeIds(start: number, end: number, samples = CHUNK_PROBE_SAMPLES): number[] {
  const length = end - start;
  if (length <= samples) {
    return Array.from({ length }, (_, i) => start + i);
  }
  return Array.from({ length: samples }, (_, i) => start + Math.round((i * (length - 1)) / (samples - 1)));
}

/**
 * A fresh state dir over files that are already stored would PUT identical bytes and clear their replication, so
 * positions spread over the range are checked first. It also proves the credentials before any worker starts.
 */
async function preflight(ranges: ArchiveRanges, storageOptions: BunnyStorageOptions, prefix: string, ids: number[]) {
  const storage = new BunnyStorage(storageOptions);
  const found = new AbortController();
  const hashes = new Map<number, string>();
  let existing: string | undefined;
  try {
    await forEachLimit(
      ids,
      storageOptions.concurrency ?? PREFLIGHT_CONCURRENCY,
      async (id) => {
        const run = await ranges.find(id);
        if (!run) {
          return;
        }
        let sha = hashes.get(run.offset);
        if (sha === undefined) {
          sha = sha256Hex(new Uint8Array(await ranges.archive.readTileData(run.offset, run.length)));
          hashes.set(run.offset, sha);
        }
        const path = tilePath(prefix, id);
        if ((await storage.describe(path))?.checksum === sha) {
          existing ??= path;
          found.abort();
        }
      },
      found.signal,
    );
  } finally {
    await storage.close();
  }
  if (existing) {
    throw new Error(
      `${existing} is already stored under ${prefix}/ with the same bytes, so an earlier run uploaded part of this ` +
        'range. PUTting identical bytes again clears their replication: resume with the --state-dir that uploaded ' +
        'them, or pass --verify-existing to DESCRIBE before every PUT.',
    );
  }
}

/** Worker entry: the .ts source needs the tsx loader in the worker too when the parent was not started with it. */
function workerEntry(): { path: string; execArgv: string[] } {
  const typescript = import.meta.url.endsWith('.ts');
  const path = fileURLToPath(new URL(typescript ? './seed-worker.ts' : './seed-worker.js', import.meta.url).href);
  if (!typescript || process.execArgv.some((arg) => arg.includes('tsx'))) {
    return { path, execArgv: process.execArgv };
  }
  const tsx = pathToFileURL(createRequire(import.meta.url).resolve('tsx')).href;
  return { path, execArgv: [...process.execArgv, `--import=${tsx}`] };
}

class SeedRun {
  private slots: WorkerSlot[] = [];
  private failures: UploadFailure[] = [];
  private failuresWritten: Promise<unknown> = Promise.resolve();
  private failedChunks = 0;
  private doneThisRun = 0;
  private previouslyDone = 0;
  private previouslyDoneFiles = 0;
  private totalChunks: number;
  private stopping = false;
  private ended = false;
  private interrupted = false;
  private error: string | null = null;
  private startedAt = new Date();
  private samples: [number, number][] = [];
  private chunkStarts = new Map<number, number>();
  private wakeFeeder?: () => void;
  private failuresFile: string;
  private totalFailed = 0;
  private concurrency: number;
  private overlapping = 0;
  /** The workers' shares of requests in flight add up to this, whatever the addresses. */
  private maxInFlight = 1;
  private addresses: string[];
  private lookingUp = false;
  private lookupFailing = false;

  constructor(
    private options: SeedOptions,
    private ranges: ArchiveRanges,
    private checkpoint: Checkpoint,
    private storageOptions: BunnyStorageOptions,
    private log: (line: string) => void,
  ) {
    const { from, to, chunkSize } = options;
    const first = Math.floor(from / chunkSize);
    const last = Math.floor((to - 1) / chunkSize);
    this.totalChunks = last - first + 1;
    for (let id = first; id <= last; id++) {
      if (checkpoint.done.has(id)) {
        this.previouslyDone++;
        this.previouslyDoneFiles += Math.min(to, (id + 1) * chunkSize) - Math.max(from, id * chunkSize);
      }
    }
    this.failuresFile = join(options.stateDir, 'failures.jsonl');
    this.concurrency = options.concurrency;
    this.addresses = [...(storageOptions.addresses ?? [])];
  }

  private get perAddress(): number {
    return Math.max(1, this.options.maxConnectionsPerAddress ?? DEFAULT_MAX_CONNECTIONS_PER_ADDRESS);
  }

  async run(): Promise<SeedResult> {
    const { options } = this;
    const remaining = this.totalChunks - this.previouslyDone;
    const addresses = Math.max(1, this.addresses.length);
    const { perAddress } = this;
    const total = Math.max(1, Math.min(options.concurrency, addresses * perAddress));
    // Every worker needs a connection to each address, and at least one request of the total.
    const workers = Math.max(1, Math.min(options.workers, remaining, total, perAddress));
    this.concurrency = total;
    this.maxInFlight = total;
    const onAbort = () => this.stop('interrupted');
    options.signal?.addEventListener('abort', onAbort);
    if (options.signal?.aborted) {
      this.stop('interrupted');
    }
    if (total < options.concurrency) {
      this.log(
        `Concurrency lowered from ${options.concurrency} to ${total}: ${addresses} storage addresses x ` +
          `${perAddress} connections each`,
      );
    }
    this.log(
      `Seeding ${options.archivePath} [${options.from}, ${options.to}) to ${options.storage.zone}/${options.prefix}/: ` +
        `${this.totalChunks} chunks (${this.previouslyDone} done before), ${workers} workers, ${total} in flight, ` +
        `${addresses} storage addresses`,
    );

    // On disk before any chunk starts, so a resume after a kill knows how long to wait (settleBy).
    await this.report('running', false);
    const progressTimer = setInterval(() => void this.report('running'), options.progressIntervalMs ?? 10_000);
    const sampleTimer = setInterval(() => this.sample(), 1_000);
    // Bunny's storage host has several addresses (300 s TTL) and a multi-day seed outlives them.
    const lookupTimer = this.options.storage.addresses?.length
      ? undefined
      : setInterval(() => void this.lookUpAddresses(), options.addressRefreshMs ?? DEFAULT_ADDRESS_REFRESH_MS);
    try {
      if (remaining > 0 && !this.stopping) {
        const { path, execArgv } = workerEntry();
        for (let i = 0; i < workers; i++) {
          // Shares that add up to exactly the total in flight and to at most perAddress connections per address.
          const concurrency = share(total, workers, i);
          const connections = Math.max(1, Math.min(Math.ceil(concurrency / addresses), share(perAddress, workers, i)));
          this.slots.push(this.spawn(path, execArgv, concurrency, connections));
        }
        await this.feed().catch((error: Error) => {
          // Without its start line on disk no further chunk may go out; the workers finish what they have.
          this.error ??= `could not hand out chunks: ${error.message}`;
          this.stop('failed');
        });
        await Promise.all(this.slots.map((slot) => slot.exited));
      }
    } finally {
      clearInterval(progressTimer);
      clearInterval(sampleTimer);
      clearInterval(lookupTimer);
      options.signal?.removeEventListener('abort', onAbort);
      await this.failuresWritten;
    }

    const complete = !this.stopping && this.failedChunks === 0 && this.error === null;
    const state: SeedState = this.interrupted ? 'interrupted' : complete ? 'complete' : 'failed';
    if (state === 'failed' && this.error === null) {
      this.error = `${this.totalFailed} files failed in ${this.failedChunks} chunks; run again to repair them`;
    }
    const status = await this.report(state);
    return { status, failures: this.failures };
  }

  private spawn(path: string, execArgv: string[], concurrency: number, connectionsPerAddress: number): WorkerSlot {
    const config: WorkerConfig = {
      archivePath: this.options.archivePath,
      tileDataOffset: this.ranges.tileDataOffset,
      prefix: this.options.prefix,
      concurrency,
      storage: { ...this.storageOptions, concurrency, connectionsPerAddress },
      maxFailures: this.options.maxFailures ?? 1000,
    };
    const worker = new Worker(path, { workerData: config, execArgv });
    const slot: WorkerSlot = {
      worker,
      outstanding: new Set(),
      emitting: new Set(),
      stats: newUploadStats(),
      counters: newCounters(),
      inFlight: 0,
      exited: new Promise((resolve) => worker.once('exit', () => resolve())),
      dead: false,
    };
    worker.on('message', (message: WorkerMessage) => this.onMessage(slot, message));
    worker.on('error', (error: Error) => {
      this.error ??= `worker crashed: ${error.message}`;
      this.stop('failed');
    });
    void slot.exited.then(() => {
      slot.dead = true;
      slot.inFlight = 0;
      if (!this.stopping && (!this.ended || slot.outstanding.size > 0)) {
        this.error ??= 'a worker exited before its chunks were finished';
        this.stop('failed');
      }
      slot.outstanding.clear();
      slot.emitting.clear();
      this.wakeFeeder?.();
    });
    return slot;
  }

  private onMessage(slot: WorkerSlot, message: WorkerMessage) {
    switch (message.type) {
      case 'progress':
      case 'final':
        slot.stats = message.stats;
        slot.counters = message.counters;
        slot.inFlight = message.type === 'progress' ? message.inFlight : 0;
        return;
      case 'chunk-emitted':
        slot.emitting.delete(message.chunkId);
        this.wakeFeeder?.();
        return;
      case 'chunk-overlap':
        if (++this.overlapping <= OVERLAP_LOG_LINES) {
          this.log(
            `Chunk ${message.chunkId}: ${message.reason}; verifying it (DESCRIBE before every PUT)` +
              (this.overlapping === OVERLAP_LOG_LINES ? '; further such chunks are only counted in status.json' : ''),
          );
        }
        return;
      case 'chunk-done':
        slot.outstanding.delete(message.chunkId);
        slot.emitting.delete(message.chunkId);
        this.chunkStarts.delete(message.chunkId);
        if (message.failures.length === 0) {
          this.doneThisRun++;
          void this.checkpoint.markDone(message.chunkId).catch((error: Error) => {
            this.error ??= `checkpoint write failed: ${error.message}`;
            this.stop('failed');
          });
        } else {
          this.failedChunks++;
          this.recordFailures(message.chunkId, message.failures);
        }
        this.wakeFeeder?.();
        return;
      case 'chunk-stopped':
        slot.outstanding.delete(message.chunkId);
        slot.emitting.delete(message.chunkId);
        this.chunkStarts.delete(message.chunkId);
        this.wakeFeeder?.();
        return;
      case 'fatal':
        this.error ??= message.error;
        this.stop('failed');
        return;
    }
  }

  private recordFailures(chunkId: number, failures: UploadFailure[]) {
    this.totalFailed += failures.length;
    for (const failure of failures) {
      if (this.failures.length < MAX_REPORTED_FAILURES) {
        this.failures.push(failure);
      }
    }
    const lines = failures.map((failure) => JSON.stringify({ chunkId, ...failure, at: new Date().toISOString() }));
    this.failuresWritten = this.failuresWritten
      .then(() => writeFile(this.failuresFile, lines.join('\n') + '\n', { flag: 'a' }))
      .catch((error: Error) => {
        // The state dir is no longer writable (the checkpoint is next): stop the normal way, with the reason.
        this.error ??= `could not write ${this.failuresFile}: ${error.message}`;
        this.stop('failed');
      });
    if (this.totalFailed >= (this.options.maxFailures ?? 1000)) {
      this.error ??= `${this.totalFailed} files failed; stopping`;
      this.stop('failed');
    }
  }

  private stop(reason: 'interrupted' | 'failed') {
    if (reason === 'interrupted') {
      this.interrupted = true;
    }
    if (this.stopping) {
      return;
    }
    this.stopping = true;
    this.log(reason === 'interrupted' ? 'Stopping: finishing the PUTs in flight' : `Stopping: ${this.error}`);
    for (const slot of this.slots) {
      slot.worker.postMessage({ type: 'stop' } satisfies MainMessage);
    }
    this.wakeFeeder?.();
  }

  /** Looks the storage host up again and moves the workers to the new address set if it changed. */
  private async lookUpAddresses() {
    if (this.lookingUp || this.stopping) {
      return;
    }
    this.lookingUp = true;
    try {
      const found = [
        ...new Set(await (this.options.resolveAddresses ?? resolveStorageAddresses)(this.options.storage.host)),
      ].sort();
      this.lookupFailing = false;
      const added = found.filter((address) => !this.addresses.includes(address));
      const removed = this.addresses.filter((address) => !found.includes(address));
      if (found.length === 0 || (added.length === 0 && removed.length === 0) || this.stopping) {
        return;
      }
      this.addresses = found;
      this.concurrency = Math.max(1, Math.min(this.maxInFlight, found.length * this.perAddress));
      this.log(
        `Storage addresses changed: added ${added.join(', ') || 'none'}, removed ${removed.join(', ') || 'none'}; ` +
          `now ${found.length}`,
      );
      for (const slot of this.slots) {
        if (!slot.dead) {
          slot.worker.postMessage({ type: 'addresses', addresses: found } satisfies MainMessage);
        }
      }
    } catch (error) {
      if (!this.lookupFailing) {
        this.log(`Could not look up the storage host again (${(error as Error).message}); keeping its addresses`);
      }
      this.lookupFailing = true;
    } finally {
      this.lookingUp = false;
    }
  }

  /** Hands chunks to workers in tile-id order, each recorded as started (on disk) before it is sent. */
  private async feed() {
    const { from, to, chunkSize, verifyExisting } = this.options;
    const chunks = planChunks(this.ranges, from, to, chunkSize, (id) => this.checkpoint.done.has(id));
    for (;;) {
      let slot = this.idleSlot();
      while (!slot && !this.stopping) {
        await new Promise<void>((resolve) => (this.wakeFeeder = resolve));
        slot = this.idleSlot();
      }
      if (this.stopping || !slot) {
        break;
      }
      const next = await chunks.next();
      if (next.done || this.stopping) {
        break;
      }
      const chunk: ChunkDescriptor = {
        ...next.value,
        verify: verifyExisting || this.checkpoint.started.has(next.value.chunkId),
      };
      await this.checkpoint.markStarted([chunk.chunkId]);
      slot.outstanding.add(chunk.chunkId);
      slot.emitting.add(chunk.chunkId);
      this.chunkStarts.set(chunk.chunkId, chunk.start);
      slot.worker.postMessage({ type: 'chunk', chunk } satisfies MainMessage, [chunk.runs.buffer as ArrayBuffer]);
    }
    await chunks.return(undefined);
    if (!this.stopping) {
      this.ended = true;
      for (const slot of this.slots) {
        slot.worker.postMessage({ type: 'end' } satisfies MainMessage);
      }
    }
  }

  private idleSlot(): WorkerSlot | undefined {
    let best: WorkerSlot | undefined;
    for (const slot of this.slots) {
      if (slot.dead || slot.emitting.size >= PREFETCH || slot.outstanding.size >= MAX_OUTSTANDING) {
        continue;
      }
      if (
        !best ||
        slot.emitting.size < best.emitting.size ||
        (slot.emitting.size === best.emitting.size && slot.outstanding.size < best.outstanding.size)
      ) {
        best = slot;
      }
    }
    return best;
  }

  private totals() {
    const stats = newUploadStats();
    const counters = newCounters();
    let inFlight = 0;
    for (const slot of this.slots) {
      stats.uploaded += slot.stats.uploaded;
      stats.skippedExisting += slot.stats.skippedExisting;
      stats.failed += slot.stats.failed;
      stats.bytes += slot.stats.bytes;
      addCounters(counters, slot.counters);
      inFlight += slot.inFlight;
    }
    return { stats, counters, inFlight };
  }

  private sample() {
    const { stats } = this.totals();
    const now = performance.now();
    this.samples.push([now, stats.uploaded + stats.skippedExisting]);
    while (this.samples.length > 2 && now - this.samples[1][0] > 60_000) {
      this.samples.shift();
    }
  }

  private async report(state: SeedState, print = true): Promise<SeedStatus> {
    const { options } = this;
    const { stats, counters, inFlight } = this.totals();
    const done = stats.uploaded + stats.skippedExisting;
    const elapsed = (Date.now() - this.startedAt.getTime()) / 1000;
    const oldest = this.samples[0];
    const now = performance.now();
    const oneMinute = oldest && now > oldest[0] ? ((done - oldest[1]) * 1000) / (now - oldest[0]) : 0;
    const overall = elapsed > 0 ? done / elapsed : 0;
    const total = options.to - options.from;
    const left = total - this.previouslyDoneFiles - done;
    const current = this.chunkStarts.size > 0 ? Math.min(...this.chunkStarts.values()) : null;
    const memory = process.memoryUsage();
    const status: SeedStatus = {
      state: state === 'running' && this.stopping ? 'stopping' : state,
      startedAt: this.startedAt.toISOString(),
      updatedAt: new Date().toISOString(),
      archive: options.archivePath,
      prefix: options.prefix,
      range: { from: options.from, to: options.to },
      chunkSize: options.chunkSize,
      workers: this.slots.length,
      concurrency: this.concurrency,
      chunks: {
        total: this.totalChunks,
        done: this.previouslyDone + this.doneThisRun,
        previouslyDone: this.previouslyDone,
        active: this.chunkStarts.size,
        failed: this.failedChunks,
        overlapping: this.overlapping,
      },
      files: {
        total,
        done: this.previouslyDoneFiles + done,
        previouslyDone: this.previouslyDoneFiles,
        uploaded: stats.uploaded,
        skippedExisting: stats.skippedExisting,
        failed: stats.failed,
      },
      bytes: stats.bytes,
      rate: { oneMinute: Math.round(oneMinute), overall: Math.round(overall) },
      inFlight,
      retries: counters,
      rateLimited: counters.rateLimited,
      etaSeconds: oneMinute > 0 && left > 0 ? Math.round(left / oneMinute) : left <= 0 ? 0 : null,
      current: current === null ? null : { tileId: current, zoom: tileIdZoom(current) },
      memory: { rssBytes: memory.rss, peakRssBytes: process.resourceUsage().maxRSS * 1024 },
      addresses: this.addresses,
      // While running, PUTs go out until the next status write (allowed one interval late); once ended, none after.
      settleBy: new Date(
        Date.now() + (state === 'running' ? 2 * (options.progressIntervalMs ?? 10_000) : 0) + commitWindowMs(options),
      ).toISOString(),
      failuresFile: this.failuresFile,
      error: this.error,
    };
    if (state === 'running' && print) {
      this.log(formatProgress(status));
    }
    await writeAtomic(join(options.stateDir, 'status.json'), JSON.stringify(status, null, 2) + '\n').catch(
      (error: Error) => this.log(`Could not write status.json: ${error.message}`),
    );
    return status;
  }
}

/** Part `i` of `n` split into `parts` near-equal whole shares. */
const share = (n: number, parts: number, i: number) => Math.floor(n / parts) + (i < n % parts ? 1 : 0);

const number = (value: number) => Math.round(value).toLocaleString('en-US');

export function formatProgress(status: SeedStatus): string {
  const { files, retries } = status;
  const percent = files.total > 0 ? ((100 * files.done) / files.total).toFixed(1) : '100';
  const eta = status.etaSeconds === null ? '?' : formatDuration(status.etaSeconds);
  const where = status.current ? ` at z${status.current.zoom} id ${number(status.current.tileId)}` : '';
  return (
    `${new Date().toISOString().slice(11, 19)} ${number(files.done)}/${number(files.total)} files (${percent}%), ` +
    `${number(status.rate.oneMinute)}/s 1m, ${number(status.rate.overall)}/s overall, ${number(status.inFlight)} in flight, ` +
    `avg ${number(retries.requests ? retries.requestMs / retries.requests : 0)} ms, ` +
    `429 ${number(retries.rateLimited)}, 5xx ${number(retries.serverErrors)}, network ${number(retries.networkErrors)}, ` +
    `timeouts ${number(retries.timeouts)}, failed ${number(files.failed)}, ` +
    `rss ${(status.memory.rssBytes / 2 ** 30).toFixed(2)} GiB, eta ${eta}${where}`
  );
}

export function formatDuration(seconds: number): string {
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  return h > 0 ? `${h}h${String(m).padStart(2, '0')}m` : m > 0 ? `${m}m${String(s).padStart(2, '0')}s` : `${s}s`;
}
