/**
 * Bunny Edge Storage client: PUT with a SHA-256 Checksum, DESCRIBE, GET, listing and DELETE over keep-alive
 * connections pinned to each address of the storage host (Bunny allows 250 connections per zone per storage server).
 *
 * Re-PUTting a file with identical bytes clears its ReplicatedZones for good, so put() never sends a file twice
 * unless it knows the first copy was not stored: a 429 means it was not; after any other failure (network error,
 * timeout, 5xx) it asks DESCRIBE first and only PUTs again if the file is missing or holds other bytes.
 */
import { lookup } from 'node:dns/promises';
import { isIP } from 'node:net';
import { setTimeout as sleep } from 'node:timers/promises';
import { Pool } from 'undici';

export const DEFAULT_STORAGE_HOST = 'storage.bunnycdn.com';

export interface StorageObject {
  /** Uppercase hex SHA-256 of the stored bytes. */
  checksum: string;
  length: number;
  /** Replica regions holding the file (the main region is not listed). */
  replicatedZones: string[];
  lastChanged: string;
}

export interface StorageListEntry extends StorageObject {
  name: string;
  isDirectory: boolean;
}

export interface RetryOptions {
  /** Attempts per request for retryable failures other than 429 (5xx, network errors, timeouts). */
  maxAttempts: number;
  baseDelayMs: number;
  maxDelayMs: number;
  /** Total 429 back-off one request may spend before it fails. 429s are otherwise retried without limit. */
  maxRateLimitWaitMs: number;
  /**
   * Least wait between an ambiguous PUT failure and its DESCRIBE, so a request the server is still finishing lands
   * first (five times this after a client timeout).
   */
  settleMs: number;
}

export const DEFAULT_RETRY: RetryOptions = {
  maxAttempts: 8,
  baseDelayMs: 500,
  maxDelayMs: 60_000,
  maxRateLimitWaitMs: 30 * 60_000,
  settleMs: 2_000,
};

export interface BunnyStorageOptions {
  zone: string;
  password: string;
  /** A host name (HTTPS on 443), or an origin URL such as `http://127.0.0.1:8080` for a local stand-in. */
  host?: string;
  /** Requests in flight at most (default 64). */
  concurrency?: number;
  /** Addresses to pin connections to instead of resolving the host (default: every IPv4 address of the host). */
  addresses?: string[];
  /** Per request: time to response headers and between body reads (default 60 s). */
  timeoutMs?: number;
  retry?: Partial<RetryOptions>;
  /** Counters to add to; pass one object to several clients to sum them. */
  counters?: StorageCounters;
}

export interface StorageCounters {
  requests: number;
  puts: number;
  describes: number;
  /** Requests sent again after a failure, of any kind. */
  retries: number;
  rateLimited: number;
  serverErrors: number;
  networkErrors: number;
  timeouts: number;
  /** Ambiguous PUT failures that DESCRIBE showed were stored after all. */
  ambiguousStored: number;
  /** Ambiguous PUT failures where DESCRIBE showed the file missing or different, so it was PUT again. */
  ambiguousResent: number;
  /** Time spent in requests that got an answer, summed (divide by requests for the average). */
  requestMs: number;
}

export const newCounters = (): StorageCounters => ({
  requests: 0,
  puts: 0,
  describes: 0,
  retries: 0,
  rateLimited: 0,
  serverErrors: 0,
  networkErrors: 0,
  timeouts: 0,
  ambiguousStored: 0,
  ambiguousResent: 0,
  requestMs: 0,
});

export function addCounters(into: StorageCounters, from: StorageCounters): StorageCounters {
  for (const key of Object.keys(into) as (keyof StorageCounters)[]) {
    into[key] += from[key];
  }
  return into;
}

export class StorageError extends Error {
  constructor(
    message: string,
    readonly status?: number,
    /** Wrong password or zone: every other request will fail the same way. */
    readonly fatal = false,
  ) {
    super(message);
    this.name = 'StorageError';
  }
}

/** Reads BUNNY_STORAGE_ZONE, BUNNY_STORAGE_PASSWORD and BUNNY_STORAGE_HOST. */
export function storageOptionsFromEnv(env = process.env): BunnyStorageOptions {
  const zone = env.BUNNY_STORAGE_ZONE;
  const password = env.BUNNY_STORAGE_PASSWORD;
  if (!zone || !password) {
    throw new Error('BUNNY_STORAGE_ZONE and BUNNY_STORAGE_PASSWORD must be set');
  }
  return { zone, password, host: env.BUNNY_STORAGE_HOST || DEFAULT_STORAGE_HOST };
}

interface Endpoint {
  protocol: 'http:' | 'https:';
  hostname: string;
  port: number;
  /** The Host header and TLS server name. */
  hostHeader: string;
}

function parseHost(host: string): Endpoint {
  const url = new URL(/^https?:\/\//.test(host) ? host : `https://${host}`);
  const protocol = url.protocol === 'http:' ? 'http:' : 'https:';
  const port = url.port ? Number(url.port) : protocol === 'http:' ? 80 : 443;
  return { protocol, hostname: url.hostname.replace(/^\[|\]$/g, ''), port, hostHeader: url.host };
}

/** The addresses connections to a storage host are spread across: its IPv4 addresses, else whatever it resolves to. */
export async function resolveStorageAddresses(host = DEFAULT_STORAGE_HOST): Promise<string[]> {
  const { hostname } = parseHost(host);
  if (isIP(hostname)) {
    return [hostname];
  }
  let found = await lookup(hostname, { all: true, family: 4 }).catch(() => []);
  if (found.length === 0) {
    found = await lookup(hostname, { all: true });
  }
  return [...new Set(found.map((entry) => entry.address))].sort();
}

type Outcome = { status: number; body: Buffer } | { error: Error; kind: 'network' | 'timeout' };

const TIMEOUT_CODES = new Set([
  'UND_ERR_HEADERS_TIMEOUT',
  'UND_ERR_BODY_TIMEOUT',
  'UND_ERR_CONNECT_TIMEOUT',
  'ETIMEDOUT',
]);

class Semaphore {
  private waiting: (() => void)[] = [];
  active = 0;
  constructor(private limit: number) {}

  async acquire() {
    if (this.active >= this.limit) {
      await new Promise<void>((resolve) => this.waiting.push(resolve));
    }
    this.active++;
  }

  release() {
    this.active--;
    this.waiting.shift()?.();
  }
}

interface PinnedPool {
  pool: Pool;
  active: number;
}

export class BunnyStorage {
  readonly counters: StorageCounters;
  readonly zone: string;
  private endpoint: Endpoint;
  private password: string;
  private retry: RetryOptions;
  private timeoutMs: number;
  private concurrency: number;
  private slots: Semaphore;
  // Bunny serves at most 5 concurrent listings per zone.
  private listSlots = new Semaphore(4);
  private pools?: Promise<PinnedPool[]>;
  private closed = false;

  constructor(private options: BunnyStorageOptions) {
    this.zone = options.zone;
    this.password = options.password;
    this.endpoint = parseHost(options.host ?? DEFAULT_STORAGE_HOST);
    this.retry = { ...DEFAULT_RETRY, ...options.retry };
    this.timeoutMs = options.timeoutMs ?? 60_000;
    this.concurrency = Math.max(1, options.concurrency ?? 64);
    this.slots = new Semaphore(this.concurrency);
    this.counters = options.counters ?? newCounters();
  }

  get inFlight(): number {
    return this.slots.active;
  }

  /** Stores `body` at `path`, safe to call for a file that may already be there (see the module comment). */
  async put(path: string, body: Uint8Array, sha256Hex: string): Promise<void> {
    const checksum = sha256Hex.toUpperCase();
    const headers = { checksum, 'content-type': 'application/octet-stream' };
    let failures = 0;
    let rateLimitedFor = 0;
    let rateLimitedTimes = 0;
    for (;;) {
      this.counters.puts++;
      const outcome = await this.send('PUT', path, { body, headers });
      if ('status' in outcome && outcome.status >= 200 && outcome.status < 300) {
        return;
      }
      if ('status' in outcome && outcome.status === 429) {
        rateLimitedFor += await this.rateLimitPause(++rateLimitedTimes, rateLimitedFor, `PUT ${path}`);
        this.counters.retries++;
        continue;
      }
      if ('status' in outcome && outcome.status < 500 && outcome.status !== 408) {
        throw this.httpError('PUT', path, outcome);
      }
      // Ambiguous: the server may have stored the body before the failure reached us. After a timeout it may still
      // be working on it, so give it longer before asking.
      failures++;
      const reason = describeOutcome(outcome);
      const settle = 'kind' in outcome && outcome.kind === 'timeout' ? this.retry.settleMs * 5 : this.retry.settleMs;
      await sleep(Math.max(settle, this.backoff(failures)));
      const stored = await this.describe(path);
      if (stored?.checksum.toUpperCase() === checksum) {
        this.counters.ambiguousStored++;
        return;
      }
      if (failures >= this.retry.maxAttempts) {
        throw new StorageError(`PUT ${path} failed ${failures} times, last: ${reason}`);
      }
      this.counters.ambiguousResent++;
      this.counters.retries++;
    }
  }

  /** The stored file's checksum, size and replica regions, or null if there is none. */
  async describe(path: string): Promise<StorageObject | null> {
    this.counters.describes++;
    const response = await this.idempotent('DESCRIBE', path, [200, 404]);
    if (response.status === 404) {
      return null;
    }
    return toStorageObject(JSON.parse(response.body.toString('utf8')));
  }

  async get(path: string): Promise<Uint8Array | null> {
    const response = await this.idempotent('GET', path, [200, 404]);
    return response.status === 404 ? null : new Uint8Array(response.body);
  }

  /** Deletes a file, or a folder and everything below it if `path` ends with "/". A missing path is not an error. */
  async delete(path: string): Promise<void> {
    await this.idempotent('DELETE', path, [200, 201, 204, 404]);
  }

  /** The files and folders directly inside `dir`. */
  async list(dir: string): Promise<StorageListEntry[]> {
    await this.listSlots.acquire();
    try {
      const folder = dir.replace(/^\/+|\/+$/g, '');
      const response = await this.idempotent('GET', folder ? `${folder}/` : '', [200, 404]);
      if (response.status === 404) {
        return [];
      }
      const rows = JSON.parse(response.body.toString('utf8')) as BunnyObject[];
      return rows.map((row) => ({
        name: row.ObjectName,
        isDirectory: row.IsDirectory === true,
        ...toStorageObject(row),
      }));
    } finally {
      this.listSlots.release();
    }
  }

  async close(): Promise<void> {
    this.closed = true;
    const pools = await this.pools?.catch(() => []);
    await Promise.all((pools ?? []).map(({ pool }) => pool.close()));
  }

  private async idempotent(method: string, path: string, accept: number[]): Promise<{ status: number; body: Buffer }> {
    let failures = 0;
    let rateLimitedFor = 0;
    let rateLimitedTimes = 0;
    for (;;) {
      const outcome = await this.send(method, path, {});
      if ('status' in outcome && accept.includes(outcome.status)) {
        return outcome;
      }
      if ('status' in outcome && outcome.status === 429) {
        rateLimitedFor += await this.rateLimitPause(++rateLimitedTimes, rateLimitedFor, `${method} ${path}`);
        this.counters.retries++;
        continue;
      }
      if ('status' in outcome && outcome.status < 500 && outcome.status !== 408) {
        throw this.httpError(method, path, outcome);
      }
      failures++;
      if (failures >= this.retry.maxAttempts) {
        throw new StorageError(`${method} ${path} failed ${failures} times, last: ${describeOutcome(outcome)}`);
      }
      await sleep(this.backoff(failures));
      this.counters.retries++;
    }
  }

  private async rateLimitPause(times: number, waitedMs: number, what: string): Promise<number> {
    this.counters.rateLimited++;
    const delay = this.backoff(times);
    if (waitedMs + delay > this.retry.maxRateLimitWaitMs) {
      throw new StorageError(`${what} still rate limited after ${Math.round(waitedMs / 1000)} s`, 429);
    }
    await sleep(delay);
    return delay;
  }

  private backoff(attempt: number): number {
    const base = Math.min(this.retry.maxDelayMs, this.retry.baseDelayMs * 2 ** (attempt - 1));
    return Math.min(this.retry.maxDelayMs, base * (0.5 + Math.random()));
  }

  private httpError(method: string, path: string, outcome: { status: number; body: Buffer }): StorageError {
    const text = outcome.body.toString('utf8').slice(0, 200);
    const fatal = outcome.status === 401 || outcome.status === 403;
    return new StorageError(`${method} ${path}: HTTP ${outcome.status} ${text}`, outcome.status, fatal);
  }

  private async send(
    method: string,
    path: string,
    { body, headers }: { body?: Uint8Array; headers?: Record<string, string> },
  ): Promise<Outcome> {
    if (this.closed) {
      throw new StorageError('Storage client is closed');
    }
    const pools = await this.connect();
    await this.slots.acquire();
    let pinned = pools[0];
    for (const candidate of pools) {
      if (candidate.active < pinned.active) {
        pinned = candidate;
      }
    }
    pinned.active++;
    this.counters.requests++;
    const started = performance.now();
    try {
      const response = await pinned.pool.request({
        method,
        path: `/${encodePath(this.zone)}/${encodePath(path)}`,
        headers: { host: this.endpoint.hostHeader, accesskey: this.password, ...headers },
        body,
        headersTimeout: this.timeoutMs,
        bodyTimeout: this.timeoutMs,
      });
      const bytes = Buffer.from(await response.body.arrayBuffer());
      this.counters.requestMs += performance.now() - started;
      if (response.statusCode >= 500) {
        this.counters.serverErrors++;
      }
      return { status: response.statusCode, body: bytes };
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error));
      const code = (err as { code?: string }).code ?? '';
      const kind = TIMEOUT_CODES.has(code) ? 'timeout' : 'network';
      if (kind === 'timeout') {
        this.counters.timeouts++;
      } else {
        this.counters.networkErrors++;
      }
      return { error: err, kind };
    } finally {
      pinned.active--;
      this.slots.release();
    }
  }

  private connect(): Promise<PinnedPool[]> {
    this.pools ??= (async () => {
      const addresses = this.options.addresses?.length
        ? this.options.addresses
        : await resolveStorageAddresses(this.options.host ?? DEFAULT_STORAGE_HOST);
      if (addresses.length === 0) {
        throw new StorageError(`${this.endpoint.hostname} has no addresses`, undefined, true);
      }
      const { protocol, port, hostname } = this.endpoint;
      const perPool = Math.ceil(this.concurrency / addresses.length);
      return addresses.map((address) => {
        const ip = isIP(address) === 6 ? `[${address}]` : address;
        const pool = new Pool(`${protocol}//${ip}:${port}`, {
          connections: perPool,
          pipelining: 1,
          allowH2: false,
          keepAliveTimeout: 30_000,
          keepAliveMaxTimeout: 60_000,
          headersTimeout: this.timeoutMs,
          bodyTimeout: this.timeoutMs,
          connect: {
            timeout: Math.min(this.timeoutMs, 15_000),
            ...(protocol === 'https:' && !isIP(hostname) ? { servername: hostname } : {}),
          },
        });
        return { pool, active: 0 };
      });
    })();
    return this.pools;
  }
}

interface BunnyObject {
  ObjectName: string;
  Length: number;
  Checksum: string | null;
  ReplicatedZones: string | null;
  LastChanged: string;
  IsDirectory?: boolean;
}

function toStorageObject(row: BunnyObject): StorageObject {
  return {
    checksum: (row.Checksum ?? '').toUpperCase(),
    length: row.Length,
    replicatedZones: (row.ReplicatedZones ?? '')
      .split(',')
      .map((zone) => zone.trim())
      .filter(Boolean),
    lastChanged: row.LastChanged,
  };
}

function encodePath(path: string): string {
  return path.split('/').map(encodeURIComponent).join('/');
}

function describeOutcome(outcome: Outcome): string {
  if ('status' in outcome) {
    return `HTTP ${outcome.status} ${outcome.body.toString('utf8').slice(0, 120)}`.trim();
  }
  return `${outcome.kind}: ${outcome.error.message}`;
}
