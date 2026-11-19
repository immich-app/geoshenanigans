/**
 * Bunny Edge Storage client: PUT with a SHA-256 Checksum, DESCRIBE, GET, listing and DELETE over keep-alive
 * connections pinned to each address of the storage host (Bunny allows 250 connections per zone per storage server).
 *
 * Re-PUTting a file with identical bytes clears its ReplicatedZones for good, so put() never sends a file twice
 * unless it knows the first copy was not stored: a 429 or a failed connect means it was not; after any other failure
 * (network error, timeout, 5xx, 408) it asks DESCRIBE first and only PUTs again if the file is missing or holds other
 * bytes, and not before 5 x settleMs has passed since the failure (the backend may still commit behind a gateway that
 * gave up, as after a client timeout).
 *
 * DESCRIBE's Checksum can lag an overwrite: measured on Bunny, two writes of different bytes to one path within about
 * a second left 10-25% of files reporting the first version's Checksum, Length and ReplicatedZones for minutes, some
 * for good, while GET already returned the second (writes 2 s or more apart, and single overwrites of older files, were
 * always right). So holds(), and put() after an ambiguous failure, GET and hash a file whose DESCRIBE shows another
 * checksum before deciding it does not hold the bytes. Never write one path twice within a few seconds, and never run
 * a sync while a seed runs.
 *
 * Bunny allows 250 connections per zone per storage server, and that budget is shared by every tool working on the
 * zone at once (a seed defaults to 240 per address; bunny:replication to 8).
 *
 * A storage address that keeps failing is set aside for a while (doubling up to a minute) and probed one request at
 * a time: otherwise a server that fails fast would look the least busy and draw most requests and their retries.
 * setAddresses() moves a long-lived client to the addresses a later lookup returns.
 */
import { createHash } from 'node:crypto';
import { lookup } from 'node:dns/promises';
import { isIP } from 'node:net';
import { setTimeout as sleep } from 'node:timers/promises';
import { Pool } from 'undici';

export const DEFAULT_STORAGE_HOST = 'storage.bunnycdn.com';
export const DEFAULT_TIMEOUT_MS = 60_000;

/** Uppercase hex SHA-256, as Bunny's Checksum header and DESCRIBE use. */
export const sha256Hex = (bytes: Uint8Array) => createHash('sha256').update(bytes).digest('hex').toUpperCase();

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
  /** At least 1: a zero back-off would retry 429s in a tight loop. */
  baseDelayMs: number;
  maxDelayMs: number;
  /**
   * How long one request may keep getting 429s, from the first, before it fails (wall clock). 429s are otherwise
   * retried without limit.
   */
  maxRateLimitWaitMs: number;
  /**
   * Least wait between an ambiguous PUT failure and its first DESCRIBE (five times this after a client timeout). A
   * file not found then is only PUT again once five times this has passed since the failure, so a request the server
   * is still finishing lands first.
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
  /** Connections to each address at most (default: concurrency spread over the addresses, rounded up). */
  connectionsPerAddress?: number;
  /** Per request: time to response headers and between body reads (DEFAULT_TIMEOUT_MS; at least 1, as 0 means none). */
  timeoutMs?: number;
  retry?: Partial<RetryOptions>;
  /** Counters to add to; pass one object to several clients to sum them. */
  counters?: StorageCounters;
  /**
   * Ends back-off waits (429, ambiguous failures) at once with a StorageError whose `stopped` is set. Requests already
   * sent still finish or time out.
   */
  signal?: AbortSignal;
}

/** Options for one read-only call (describe()). */
export interface CallOptions {
  /**
   * Ends this call when it aborts: no request is sent or sent again after that, a wait for a request slot or a back-off
   * (429 included) ends at once, and a request in flight is abandoned. The call then fails with a StorageError whose
   * `cancelled` is set, so a caller with a deadline does not wait out the client's own retry budget
   * (maxRateLimitWaitMs) past it.
   */
  signal?: AbortSignal;
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
  /** DESCRIBEs that showed another checksum while GET returned the expected bytes (DESCRIBE lagging an overwrite). */
  staleDescribes: number;
  /** Times a storage address was set aside after failing repeatedly. */
  addressTrips: number;
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
  staleDescribes: 0,
  addressTrips: 0,
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
    /** The client's signal ended a back-off: the file's state is unknown (it may be stored). */
    readonly stopped = false,
    /** The call's own signal (CallOptions) ended it before an answer: nothing was learnt about the file. */
    readonly cancelled = false,
  ) {
    super(message);
    this.name = 'StorageError';
  }
}

/**
 * Reads BUNNY_STORAGE_ZONE, BUNNY_STORAGE_PASSWORD, BUNNY_STORAGE_HOST and the optional BUNNY_STORAGE_ADDRESSES
 * (comma-separated IPs to use instead of resolving the host, e.g. to leave out a failing storage server).
 */
export function storageOptionsFromEnv(env = process.env): BunnyStorageOptions {
  const zone = env.BUNNY_STORAGE_ZONE;
  const password = env.BUNNY_STORAGE_PASSWORD;
  if (!zone || !password) {
    throw new Error('BUNNY_STORAGE_ZONE and BUNNY_STORAGE_PASSWORD must be set');
  }
  const addresses = env.BUNNY_STORAGE_ADDRESSES ? parseAddresses(env.BUNNY_STORAGE_ADDRESSES) : undefined;
  return { zone, password, host: env.BUNNY_STORAGE_HOST || DEFAULT_STORAGE_HOST, ...(addresses && { addresses }) };
}

/** A comma-separated list of IP addresses. */
export function parseAddresses(list: string): string[] {
  const addresses = list
    .split(',')
    .map((address) => address.trim())
    .filter(Boolean);
  const bad = addresses.filter((address) => !isIP(address));
  if (addresses.length === 0 || bad.length > 0) {
    throw new Error(`Storage addresses must be IP addresses, got ${JSON.stringify(list)}`);
  }
  return addresses;
}

/** The storage host as one canonical origin, so `storage.bunnycdn.com` and `https://storage.bunnycdn.com` compare equal. */
export function storageOrigin(host = DEFAULT_STORAGE_HOST): string {
  const { protocol, hostHeader } = parseHost(host);
  return `${protocol}//${hostHeader.toLowerCase()}`;
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

/** `unsent`: the request never left (lookup or connect failed), so it was certainly not stored. */
type Outcome = ({ status: number; body: Buffer } | { error: Error; kind: 'network' | 'timeout' | 'unsent' }) & {
  pool?: PinnedPool;
};

const TIMEOUT_CODES = new Set(['UND_ERR_HEADERS_TIMEOUT', 'UND_ERR_BODY_TIMEOUT', 'ETIMEDOUT']);
// undici writes a request only once its connection is up, so these mean nothing was sent (a closed pool refuses to
// dispatch at all).
const CONNECT_CODES = new Set([
  'UND_ERR_CLOSED',
  'ECONNREFUSED',
  'UND_ERR_CONNECT_TIMEOUT',
  'ENOTFOUND',
  'EAI_AGAIN',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'ENETDOWN',
  'EHOSTDOWN',
  'EADDRNOTAVAIL',
]);

/** Failures in a row after which an address is set aside. */
const TRIP_AFTER_FAILURES = 5;
const TRIP_BASE_MS = 1_000;
const TRIP_MAX_MS = 60_000;

class Semaphore {
  private waiting: (() => void)[] = [];
  active = 0;
  constructor(private limit: number) {}

  /** Takes a slot, waiting for one if need be; false, with no slot taken, if `signal` aborts while it waits. */
  async acquire(signal?: AbortSignal): Promise<boolean> {
    if (this.active >= this.limit) {
      if (signal?.aborted) {
        // An abort listener added now would never fire.
        return false;
      }
      const woken = await new Promise<boolean>((resolve) => {
        const wake = () => {
          signal?.removeEventListener('abort', leave);
          resolve(true);
        };
        // Out of the queue, so no release() hands this caller a slot it will not use.
        const leave = () => {
          const at = this.waiting.indexOf(wake);
          if (at !== -1) {
            this.waiting.splice(at, 1);
            resolve(false);
          }
        };
        this.waiting.push(wake);
        signal?.addEventListener('abort', leave, { once: true });
      });
      if (!woken) {
        return false;
      }
    }
    this.active++;
    return true;
  }

  release() {
    this.active--;
    this.waiting.shift()?.();
  }
}

interface PinnedPool {
  address: string;
  pool: Pool;
  active: number;
  /** Failures in a row (connect and network errors, timeouts, 408, 429, 5xx) since the last good answer. */
  failures: number;
  /** Times set aside since the last good answer; each doubles the pause. */
  trips: number;
  /** While set aside, not chosen before this time (performance.now()); then one probe at a time. */
  downUntil: number;
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
  private pools?: PinnedPool[];
  private connecting?: Promise<PinnedPool[]>;
  /** Connections per address, fixed by the first address set so a later one keeps every address within its share. */
  private perPool?: number;
  /** Pools of addresses dropped by setAddresses(), closing once their requests finish. */
  private draining = new Set<Promise<void>>();
  private closed = false;

  constructor(private options: BunnyStorageOptions) {
    this.zone = options.zone;
    this.password = options.password;
    this.endpoint = parseHost(options.host ?? DEFAULT_STORAGE_HOST);
    this.retry = { ...DEFAULT_RETRY, ...options.retry };
    this.retry.baseDelayMs = Math.max(1, this.retry.baseDelayMs);
    // undici reads a timeout of 0 as none, and a request that never ends would hold up a stop for good.
    this.timeoutMs = Math.max(1, options.timeoutMs ?? DEFAULT_TIMEOUT_MS);
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
    let rateLimitedSince: number | undefined;
    let rateLimitedTimes = 0;
    // The address that failed last: the retry and the DESCRIBE go elsewhere if another is usable.
    let avoid: PinnedPool | undefined;
    for (;;) {
      this.counters.puts++;
      const outcome = await this.send('PUT', path, { body, headers }, avoid);
      avoid = outcome.pool;
      if ('status' in outcome && outcome.status >= 200 && outcome.status < 300) {
        return;
      }
      if ('status' in outcome && outcome.status === 429) {
        rateLimitedSince ??= performance.now();
        await this.rateLimitPause(++rateLimitedTimes, rateLimitedSince, `PUT ${path}`);
        this.counters.retries++;
        continue;
      }
      if ('status' in outcome && outcome.status < 500 && outcome.status !== 408) {
        throw this.httpError('PUT', path, outcome);
      }
      failures++;
      const reason = describeOutcome(outcome);
      if ('kind' in outcome && outcome.kind === 'unsent') {
        if (failures >= this.retry.maxAttempts) {
          throw new StorageError(`PUT ${path} failed ${failures} times, last: ${reason}`);
        }
        await this.pause(this.backoff(failures));
        this.counters.retries++;
        continue;
      }
      // Ambiguous: the server may have stored the body before the failure reached us, or may still store it: after a
      // client timeout, or behind a gateway that answered 5xx or dropped the connection while the backend carried on.
      // Ask after a settle (longer after a timeout); if the file is not there yet, ask again once the full window
      // since the failure has passed, and only then PUT again.
      const failedAt = performance.now();
      const window = this.retry.settleMs * 5;
      const settle = 'kind' in outcome && outcome.kind === 'timeout' ? window : this.retry.settleMs;
      await this.pause(Math.max(settle, this.backoff(failures)));
      let stored = await this.storedAs(path, checksum, avoid);
      const rest = failedAt + window - performance.now();
      if (!stored && rest > 0) {
        await this.pause(rest);
        stored = await this.storedAs(path, checksum, avoid);
      }
      if (stored) {
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

  /**
   * The stored file's checksum, size and replica regions, or null if there is none. `signal` cancels the call, retries
   * and back-offs included (see CallOptions).
   */
  async describe(path: string, { signal }: CallOptions = {}): Promise<StorageObject | null> {
    this.counters.describes++;
    const response = await this.idempotent('DESCRIBE', path, [200, 404], { signal });
    return response.status === 404 ? null : parseObject(response.body);
  }

  async get(path: string): Promise<Uint8Array | null> {
    const response = await this.idempotent('GET', path, [200, 404]);
    return response.status === 404 ? null : new Uint8Array(response.body);
  }

  /**
   * Whether `path` holds exactly the bytes with this SHA-256: DESCRIBE, and if it shows another checksum, GET and hash
   * the file (DESCRIBE can lag an overwrite, see the module comment), so a caller never PUTs bytes already there.
   */
  holds(path: string, sha256: string): Promise<boolean> {
    return this.storedAs(path, sha256.toUpperCase());
  }

  private async storedAs(path: string, checksum: string, avoid?: PinnedPool): Promise<boolean> {
    this.counters.describes++;
    const described = await this.idempotent('DESCRIBE', path, [200, 404], { avoid });
    if (described.status === 404) {
      return false;
    }
    if (parseObject(described.body).checksum === checksum) {
      return true;
    }
    const fetched = await this.idempotent('GET', path, [200, 404], { avoid });
    const same = fetched.status === 200 && sha256Hex(fetched.body) === checksum;
    if (same) {
      this.counters.staleDescribes++;
    }
    return same;
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

  /**
   * Spreads requests over `addresses` from now on: pools are added for new addresses, and the pools of addresses no
   * longer listed take no new requests and close once the requests they carry finish. Each new pool gets the same
   * connection count as the first ones, so no address goes over its share.
   */
  async setAddresses(addresses: string[]): Promise<void> {
    const next = [...new Set(addresses)];
    if (next.length === 0 || this.closed) {
      return;
    }
    this.options = { ...this.options, addresses: next };
    await this.connecting?.catch(() => {});
    if (!this.pools || this.closed) {
      // The next request connects to the new addresses.
      return;
    }
    const kept = this.pools.filter((pinned) => next.includes(pinned.address));
    const dropped = this.pools.filter((pinned) => !next.includes(pinned.address));
    const added = next
      .filter((address) => !kept.some((pinned) => pinned.address === address))
      .map((address) => this.createPool(address));
    this.pools = [...kept, ...added];
    await Promise.all(dropped.map((pinned) => this.drain(pinned)));
  }

  async close(): Promise<void> {
    this.closed = true;
    await this.connecting?.catch(() => {});
    await Promise.all([...(this.pools ?? []).map(({ pool }) => pool.close()), ...this.draining]);
  }

  private async idempotent(
    method: string,
    path: string,
    accept: number[],
    { avoid, signal }: { avoid?: PinnedPool; signal?: AbortSignal } = {},
  ): Promise<{ status: number; body: Buffer }> {
    const what = `${method} ${path}`;
    let failures = 0;
    let rateLimitedSince: number | undefined;
    let rateLimitedTimes = 0;
    for (;;) {
      const outcome = await this.send(method, path, { signal }, avoid);
      avoid = outcome.pool;
      if ('status' in outcome && accept.includes(outcome.status)) {
        return outcome;
      }
      if ('status' in outcome && outcome.status === 429) {
        rateLimitedSince ??= performance.now();
        await this.rateLimitPause(++rateLimitedTimes, rateLimitedSince, what, signal);
        this.counters.retries++;
        continue;
      }
      if ('status' in outcome && outcome.status < 500 && outcome.status !== 408) {
        throw this.httpError(method, path, outcome);
      }
      failures++;
      if (failures >= this.retry.maxAttempts) {
        throw new StorageError(`${what} failed ${failures} times, last: ${describeOutcome(outcome)}`);
      }
      await this.pause(this.backoff(failures), signal, what);
      this.counters.retries++;
    }
  }

  /** Backs off after a 429, or fails once the 429s that started at `since` would outlast maxRateLimitWaitMs. */
  private async rateLimitPause(times: number, since: number, what: string, signal?: AbortSignal): Promise<void> {
    this.counters.rateLimited++;
    const delay = this.backoff(times);
    const waited = performance.now() - since;
    if (waited + delay > this.retry.maxRateLimitWaitMs) {
      throw new StorageError(`${what} still rate limited after ${Math.round(waited / 1000)} s`, 429);
    }
    await this.pause(delay, signal, what);
  }

  /** Waits `ms`, ended early by the client's signal (a stopped error, which wins) or the call's (a cancelled one). */
  private async pause(ms: number, signal?: AbortSignal, what = 'request'): Promise<void> {
    const stop = this.options.signal;
    try {
      await sleep(ms, undefined, { signal: stop && signal ? AbortSignal.any([stop, signal]) : (stop ?? signal) });
    } catch {
      if (stop?.aborted || !signal?.aborted) {
        throw new StorageError('Stopped while waiting to retry', undefined, false, true);
      }
      throw cancelled(what);
    }
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

  /**
   * One request. `signal` (read-only calls only) keeps it from being sent once aborted, ends its wait for a slot, and
   * abandons it in flight: each way it throws a cancelled StorageError, and the address is not held to blame.
   */
  private async send(
    method: string,
    path: string,
    { body, headers, signal }: { body?: Uint8Array; headers?: Record<string, string>; signal?: AbortSignal },
    avoid?: PinnedPool,
  ): Promise<Outcome> {
    if (this.closed) {
      throw new StorageError('Storage client is closed', undefined, true);
    }
    if (signal?.aborted) {
      throw cancelled(`${method} ${path}`);
    }
    try {
      await this.connect();
    } catch (error) {
      if (error instanceof StorageError && error.fatal) {
        throw error;
      }
      this.counters.networkErrors++;
      return { error: error instanceof Error ? error : new Error(String(error)), kind: 'unsent' };
    }
    if (!(await this.slots.acquire(signal))) {
      // Aborted while waiting for a slot: it left the queue without one.
      throw cancelled(`${method} ${path}`);
    }
    if (signal?.aborted) {
      // Aborted as a slot came free: hand it on unused.
      this.slots.release();
      throw cancelled(`${method} ${path}`);
    }
    // Chosen from the current set and dispatched in the same tick, so a pool setAddresses() drops gets no more.
    const pinned = choosePool(this.pools!, avoid);
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
        ...(signal && { signal }),
      });
      const bytes = Buffer.from(await response.body.arrayBuffer());
      this.counters.requestMs += performance.now() - started;
      const status = response.statusCode;
      if (status >= 500) {
        this.counters.serverErrors++;
      }
      this.health(pinned, status < 500 && status !== 429 && status !== 408);
      return { status, body: bytes, pool: pinned };
    } catch (error) {
      if (signal?.aborted) {
        // Abandoned by the caller, not failed: no error to count against the address.
        throw cancelled(`${method} ${path}`);
      }
      const err = error instanceof Error ? error : new Error(String(error));
      const code = (err as { code?: string }).code ?? '';
      const kind = CONNECT_CODES.has(code) ? 'unsent' : TIMEOUT_CODES.has(code) ? 'timeout' : 'network';
      if (kind === 'timeout') {
        this.counters.timeouts++;
      } else {
        this.counters.networkErrors++;
      }
      this.health(pinned, false);
      return { error: err, kind, pool: pinned };
    } finally {
      pinned.active--;
      this.slots.release();
    }
  }

  private health(pinned: PinnedPool, ok: boolean) {
    if (ok) {
      pinned.failures = 0;
      pinned.trips = 0;
      pinned.downUntil = 0;
      return;
    }
    pinned.failures++;
    const now = performance.now();
    // Failures of requests sent before the address was set aside do not lengthen the pause; a failed probe does.
    if (pinned.failures >= TRIP_AFTER_FAILURES && pinned.downUntil <= now) {
      pinned.trips++;
      this.counters.addressTrips++;
      pinned.downUntil = now + Math.min(TRIP_MAX_MS, TRIP_BASE_MS * 2 ** (pinned.trips - 1));
    }
  }

  private async connect(): Promise<PinnedPool[]> {
    if (this.pools) {
      return this.pools;
    }
    // A failed lookup is not kept: the next request resolves again.
    this.connecting ??= (async () => {
      const addresses = this.options.addresses?.length
        ? this.options.addresses
        : await resolveStorageAddresses(this.options.host ?? DEFAULT_STORAGE_HOST);
      if (addresses.length === 0) {
        throw new StorageError(`${this.endpoint.hostname} has no addresses`, undefined, true);
      }
      this.perPool ??= Math.max(
        1,
        this.options.connectionsPerAddress ?? Math.ceil(this.concurrency / addresses.length),
      );
      // Addresses given to setAddresses() while the lookup ran win over the lookup.
      const chosen = this.options.addresses?.length ? this.options.addresses : addresses;
      this.pools = [...new Set(chosen)].map((address) => this.createPool(address));
      return this.pools;
    })().finally(() => {
      this.connecting = undefined;
    });
    return this.connecting;
  }

  private createPool(address: string): PinnedPool {
    const { protocol, port, hostname } = this.endpoint;
    const ip = isIP(address) === 6 ? `[${address}]` : address;
    const pool = new Pool(`${protocol}//${ip}:${port}`, {
      connections: this.perPool,
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
    return { address, pool, active: 0, failures: 0, trips: 0, downUntil: 0 };
  }

  private drain(pinned: PinnedPool): Promise<void> {
    const closing = pinned.pool.close().finally(() => this.draining.delete(closing));
    this.draining.add(closing);
    return closing;
  }
}

/**
 * The least busy usable pool: one not set aside (or due a probe, with nothing in flight) and not `avoid`, else any
 * usable one, else the least busy of all, so requests still go somewhere when every address is failing.
 */
function choosePool(pools: PinnedPool[], avoid?: PinnedPool): PinnedPool {
  const now = performance.now();
  const usable = (pool: PinnedPool) =>
    pool.failures < TRIP_AFTER_FAILURES || (pool.downUntil <= now && pool.active === 0);
  const tiers = [(pool: PinnedPool) => pool !== avoid && usable(pool), usable, () => true];
  for (const tier of tiers) {
    let best: PinnedPool | undefined;
    for (const pool of pools) {
      if (tier(pool) && (!best || pool.active < best.active)) {
        best = pool;
      }
    }
    if (best) {
      return best;
    }
  }
  return pools[0];
}

/** A call its own signal ended (CallOptions). */
function cancelled(what: string): StorageError {
  return new StorageError(`${what} cancelled`, undefined, false, false, true);
}

interface BunnyObject {
  ObjectName: string;
  Length: number;
  Checksum: string | null;
  ReplicatedZones: string | null;
  LastChanged: string;
  IsDirectory?: boolean;
}

function parseObject(body: Buffer): StorageObject {
  return toStorageObject(JSON.parse(body.toString('utf8')) as BunnyObject);
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
