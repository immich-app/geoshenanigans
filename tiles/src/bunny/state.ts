/**
 * Sync state in the state zone (`immich-tiles-state`): `state.json` names the live build and its manifest, kept in
 * `manifests/<build>.tman.zst` for the next day's diff, and the builds a sync started writing to the tiles zone without
 * making them live (`pending`). Every path can sit under a state prefix, for tests.
 *
 * A stored manifest's bytes never change: when that name holds another manifest of the build already (one built again,
 * or a full rewrite's under the live or a pending build's key), the new one goes to a name of its own
 * (`manifests/<build>-<SHA-256 prefix>.tman.zst`). A state that names a manifest therefore keeps reading what it
 * recorded, even when the write of the state that was to replace it fails.
 *
 * Where no pending build differs from the live one, the tiles zone holds the live build's bytes. Where one does, a
 * failed or killed sync may have left either build's bytes, so the next sync plans with every pending manifest
 * (planSync in diff.ts) and checks those positions before writing them. A sync therefore:
 * 1. reads the state and plans against the live and pending manifests;
 * 2. calls beginBuild before uploading any tile, which records its build as pending;
 * 3. calls publishBuild once the zone serves the build, which makes it live and clears `pending`;
 * 4. calls pruneManifests, which deletes manifests no state names but for the newest few (each is ~1.3 GB).
 *
 * state.json also records the archive the live build came from (its v2 identity, a hash of the whole file, as
 * bunny:seed checkpoints it; see archive-identity.ts), so a seed can refuse to write another build's bytes over the
 * live prefix.
 *
 * The store is BunnyStateStore below (MemoryObjectStore in tests). The state zone has no replicas, so re-uploading
 * identical bytes is harmless there.
 */
import { createHash } from 'node:crypto';
import { createReadStream, createWriteStream } from 'node:fs';
import { readFile, rename, rm, stat, writeFile } from 'node:fs/promises';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { setTimeout as sleep } from 'node:timers/promises';
import { Agent, type Dispatcher } from 'undici';
import { parseDeploymentKey, requireEnv } from '../deploy/config';
import { ArchiveIdentity, IDENTITY_PATTERN } from './archive-identity';
import { readManifestHeader } from './manifest';

export interface StoredObject {
  /** Uppercase hex SHA-256. */
  checksum: string;
  length: number;
}

export interface ObjectStore {
  get(path: string): Promise<Uint8Array | null>;
  put(path: string, body: Uint8Array, sha256Hex: string): Promise<void>;
  describe(path: string): Promise<StoredObject | null>;
  /** Streams a file up: a planet manifest is over 1 GB, too big to hold in memory (and copies of it). */
  putFile(path: string, file: string, sha256Hex: string): Promise<void>;
  /** Streams an object down to a file; false when it doesn't exist. */
  getToFile(path: string, file: string): Promise<boolean>;
  /** Names of the files directly in a folder; empty when there is none. */
  list(dir: string): Promise<string[]>;
  /** A missing file is not an error. */
  delete(path: string): Promise<void>;
}

/** A build a sync started writing to the tiles zone and hasn't made live. */
export interface PendingBuild {
  build: string;
  /** Relative to the state prefix, e.g. "manifests/20261119.tman.zst". */
  manifest: string;
  positions: number;
  startedAt: string;
}

export interface SyncState {
  liveBuild: string;
  /** Relative to the state prefix, e.g. "manifests/20261118.tman.zst". */
  manifest: string;
  /** Tile prefix in the tiles zone, e.g. "v1". */
  prefix: string;
  /** When liveBuild last changed. */
  updatedAt: string;
  osmDate: string | null;
  positions: number;
  /** The archive liveBuild came from; null when not recorded. */
  archive: ArchiveIdentity | null;
  /** Oldest first; empty unless a sync failed after it started uploading. */
  pending: PendingBuild[];
}

const STATE_FILE = 'state.json';

function statePrefix(prefix: string | undefined): string {
  const trimmed = (prefix ?? '').replace(/^\/+|\/+$/g, '');
  if (trimmed.split('/').some((part) => part === '.' || part === '..')) {
    throw new Error(`Invalid state prefix ${JSON.stringify(prefix)}`);
  }
  return trimmed ? `${trimmed}/` : '';
}

export const statePath = (prefix?: string) => `${statePrefix(prefix)}${STATE_FILE}`;

/** The manifest's name relative to the state prefix, as state.json records it. */
export const manifestName = (buildKey: string) => `manifests/${parseDeploymentKey('build', buildKey)}.tman.zst`;

export const manifestPath = (buildKey: string, prefix?: string) => `${statePrefix(prefix)}${manifestName(buildKey)}`;

/**
 * The name of its own a manifest goes to when manifestName(buildKey) holds another one: the build key and the first 16
 * hex digits of its SHA-256.
 */
export const distinctManifestName = (buildKey: string, sha256: string) =>
  `manifests/${parseDeploymentKey('build', buildKey)}-${sha256.slice(0, 16).toUpperCase()}.tman.zst`;

export const sha256Hex = (body: Uint8Array) => createHash('sha256').update(body).digest('hex').toUpperCase();

export async function sha256File(file: string): Promise<string> {
  const hash = createHash('sha256');
  await pipeline(createReadStream(file), hash);
  return hash.digest('hex').toUpperCase();
}

const isIsoDate = (value: unknown) => typeof value === 'string' && !Number.isNaN(Date.parse(value));

const isPositions = (value: unknown) => Number.isSafeInteger(value) && (value as number) >= 0;

const LEGACY_FINGERPRINT = /^[0-9a-f]{64}$/;

/**
 * The live archive's identity; null when none is recorded. A v1 fingerprint (header, root directory and size only,
 * from before the v2 identity) reads as none: it cannot tell archives with other tile bytes apart, so a seed over the
 * live prefix refuses until a sync records a v2 identity.
 */
function parseArchive(value: unknown): ArchiveIdentity | null | undefined {
  if (value === undefined || value === null) {
    return null;
  }
  const archive = value as Partial<ArchiveIdentity> & { fingerprint?: unknown };
  if (typeof archive !== 'object' || !isPositions(archive.size)) {
    return undefined;
  }
  if (archive.identity === undefined && typeof archive.fingerprint === 'string') {
    return LEGACY_FINGERPRINT.test(archive.fingerprint) ? null : undefined;
  }
  if (typeof archive.identity !== 'string' || !IDENTITY_PATTERN.test(archive.identity)) {
    return undefined;
  }
  return { size: archive.size!, identity: archive.identity };
}

function parsePending(value: unknown, liveBuild: string): PendingBuild[] | undefined {
  if (value === undefined) {
    return [];
  }
  if (!Array.isArray(value)) {
    return undefined;
  }
  const pending: PendingBuild[] = [];
  for (const item of value as Array<Partial<PendingBuild> | null>) {
    if (
      typeof item !== 'object' ||
      item === null ||
      typeof item.build !== 'string' ||
      !/^\d{8}$/.test(item.build) ||
      item.build === liveBuild ||
      pending.some((other) => other.build === item.build) ||
      typeof item.manifest !== 'string' ||
      !item.manifest ||
      !isPositions(item.positions) ||
      !isIsoDate(item.startedAt)
    ) {
      return undefined;
    }
    pending.push({
      build: item.build,
      manifest: item.manifest,
      positions: item.positions!,
      startedAt: item.startedAt!,
    });
  }
  return pending;
}

export function parseState(value: unknown): SyncState {
  const state = value as Partial<SyncState> | null;
  if (typeof state !== 'object' || state === null) {
    throw new Error('state.json is not an object');
  }
  const liveBuild = parseDeploymentKey('state.json liveBuild', state.liveBuild);
  const problems: string[] = [];
  if (typeof state.manifest !== 'string' || !state.manifest) {
    problems.push('manifest');
  }
  if (typeof state.prefix !== 'string' || !state.prefix) {
    problems.push('prefix');
  }
  if (!isIsoDate(state.updatedAt)) {
    problems.push('updatedAt');
  }
  if (state.osmDate !== undefined && state.osmDate !== null && !isIsoDate(state.osmDate)) {
    problems.push('osmDate');
  }
  if (!isPositions(state.positions)) {
    problems.push('positions');
  }
  const pending = parsePending(state.pending, liveBuild);
  if (!pending) {
    problems.push('pending');
  }
  const archive = parseArchive(state.archive);
  if (archive === undefined) {
    problems.push('archive');
  }
  if (problems.length > 0) {
    throw new Error(`state.json has invalid ${problems.join(', ')}`);
  }
  return {
    liveBuild,
    manifest: state.manifest!,
    prefix: state.prefix!,
    updatedAt: state.updatedAt!,
    osmDate: state.osmDate ?? null,
    positions: state.positions!,
    archive: archive!,
    pending: pending!,
  };
}

/** The live state, or null before the first sync. */
export async function readState(store: ObjectStore, prefix?: string): Promise<SyncState | null> {
  const bytes = await store.get(statePath(prefix));
  if (!bytes) {
    return null;
  }
  return parseState(JSON.parse(new TextDecoder().decode(bytes)));
}

export async function writeState(store: ObjectStore, state: SyncState, prefix?: string): Promise<void> {
  const body = new TextEncoder().encode(`${JSON.stringify(parseState(state), null, 2)}\n`);
  await store.put(statePath(prefix), body, sha256Hex(body));
  await checkStored(store, statePath(prefix), sha256Hex(body), body.length);
}

async function checkStored(store: ObjectStore, path: string, sha256: string, length: number) {
  const stored = await store.describe(path);
  if (!stored || stored.checksum.toUpperCase() !== sha256 || stored.length !== length) {
    throw new Error(`${path} reads back as ${stored ? `${stored.length} bytes ${stored.checksum}` : 'missing'}`);
  }
}

export interface UploadedManifest {
  /** Relative to the state prefix, as state.json records it. */
  name: string;
  path: string;
  sha256: string;
  bytes: number;
  /** Already stored with these bytes, so not sent again. */
  skipped: boolean;
}

/**
 * Stores a build's manifest and reads it back. Its name is manifestName(buildKey), unless that holds another manifest
 * (one a state may name); then distinctManifestName, so no stored manifest's bytes ever change under a state naming it.
 */
export async function uploadManifest(
  store: ObjectStore,
  buildKey: string,
  file: string,
  prefix?: string,
): Promise<UploadedManifest> {
  const header = await readManifestHeader(file);
  if (header.buildKey !== buildKey) {
    throw new Error(`${file} is the manifest of ${header.buildKey}, not ${buildKey}`);
  }
  const [sha256, { size }] = await Promise.all([sha256File(file), stat(file)]);
  let name = manifestName(buildKey);
  let path = `${statePrefix(prefix)}${name}`;
  let existing = await store.describe(path);
  if (existing && existing.checksum.toUpperCase() !== sha256) {
    name = distinctManifestName(buildKey, sha256);
    path = `${statePrefix(prefix)}${name}`;
    existing = await store.describe(path);
    if (existing && existing.checksum.toUpperCase() !== sha256) {
      throw new Error(`${path} holds another manifest (${existing.checksum}), not this one (${sha256})`);
    }
  }
  if (existing) {
    return { name, path, sha256, bytes: existing.length, skipped: true };
  }
  await store.putFile(path, file, sha256);
  await checkStored(store, path, sha256, size);
  return { name, path, sha256, bytes: size, skipped: false };
}

/**
 * Downloads the manifest of the live build (given the state) or of a pending build to `file`, checked against the
 * stored checksum and what the state says about it.
 */
export async function downloadManifest(
  store: ObjectStore,
  build: SyncState | PendingBuild,
  file: string,
  prefix?: string,
): Promise<void> {
  const buildKey = 'liveBuild' in build ? build.liveBuild : build.build;
  const path = `${statePrefix(prefix)}${build.manifest}`;
  const stored = await store.describe(path);
  if (!stored) {
    throw new Error(`state.json names ${path}, which doesn't exist`);
  }
  const partial = `${file}.partial`;
  try {
    if (!(await store.getToFile(path, partial))) {
      throw new Error(`${path} disappeared while downloading it`);
    }
    const sha256 = await sha256File(partial);
    if (sha256 !== stored.checksum.toUpperCase()) {
      throw new Error(`${path} downloaded as ${sha256}, stored as ${stored.checksum}`);
    }
    const header = await readManifestHeader(partial);
    if (header.buildKey !== buildKey || header.positions !== build.positions) {
      throw new Error(
        `${path} is ${header.buildKey} with ${header.positions} positions; state.json says ${buildKey} with ${build.positions}`,
      );
    }
    await rename(partial, file);
  } finally {
    await rm(partial, { force: true });
  }
}

const sameState = (a: SyncState, b: SyncState) =>
  a.liveBuild === b.liveBuild &&
  a.updatedAt === b.updatedAt &&
  a.pending.map((p) => p.build).join() === b.pending.map((p) => p.build).join();

export interface BeginOptions {
  buildKey: string;
  manifestFile: string;
  statePrefix?: string;
  now?: Date;
}

/**
 * Records a build as pending: its manifest is stored and read back, then state.json lists it while the live build
 * stays. Call it after planning from `current` (the state you read) and before uploading any tile, so that a sync that
 * dies part-way leaves a state saying which positions may hold this build's bytes. Running a pending build again
 * keeps one entry for it. Fails if state.json changed since `current` was read, as the plan would be stale.
 */
export async function beginBuild(store: ObjectStore, current: SyncState, options: BeginOptions): Promise<SyncState> {
  const { buildKey } = options;
  if (current.liveBuild === buildKey) {
    throw new Error(`${buildKey} is already live`);
  }
  const stored = await readState(store, options.statePrefix);
  if (!stored || !sameState(stored, current)) {
    throw new Error('state.json changed since the sync read it; plan again');
  }
  const header = await readManifestHeader(options.manifestFile);
  const { name } = await uploadManifest(store, buildKey, options.manifestFile, options.statePrefix);
  const entry: PendingBuild = {
    build: buildKey,
    manifest: name,
    positions: header.positions,
    startedAt: (options.now ?? new Date()).toISOString(),
  };
  const state: SyncState = { ...current, pending: [...current.pending.filter((p) => p.build !== buildKey), entry] };
  await writeState(store, state, options.statePrefix);
  return state;
}

export interface PublishOptions {
  buildKey: string;
  manifestFile: string;
  /** Tile prefix in the tiles zone. */
  prefix: string;
  /** Defaults to the manifest's (the archive metadata's OSM replication time). */
  osmDate?: string | null;
  /** The archive the build came from. */
  archive?: ArchiveIdentity | null;
  statePrefix?: string;
  now?: Date;
}

/**
 * Makes a build live in the state: its manifest is stored and read back first, then state.json switches to it, so
 * state.json never names a manifest that isn't there. The manifest never takes the place of one the old state names
 * (the live build's or a pending one's, under the same key: see uploadManifest), so a state write that fails leaves
 * the old state whole. `pending` is cleared: call it only once the tiles zone holds the build everywhere, i.e. after
 * applying a plan made with every pending build of the state.
 */
export async function publishBuild(store: ObjectStore, options: PublishOptions): Promise<SyncState> {
  const header = await readManifestHeader(options.manifestFile);
  const { name } = await uploadManifest(store, options.buildKey, options.manifestFile, options.statePrefix);
  const state: SyncState = {
    liveBuild: options.buildKey,
    manifest: name,
    prefix: options.prefix,
    updatedAt: (options.now ?? new Date()).toISOString(),
    osmDate: options.osmDate === undefined ? header.osmDate : options.osmDate,
    positions: header.positions,
    archive: options.archive ?? null,
    pending: [],
  };
  await writeState(store, state, options.statePrefix);
  return state;
}

/** manifestName's files and distinctManifestName's. */
const MANIFEST_FILE = /^\d{8}(-[0-9A-F]{16})?\.tman\.zst$/;

/**
 * Deletes the manifests in the state zone that state.json names neither as live nor as pending, but for the newest
 * `keep` of them (by build date), for diagnosis. Only those two are ever read: a rollback builds its manifest again
 * from the archive. Returns the deleted names, relative to the state prefix.
 */
export async function pruneManifests(
  store: ObjectStore,
  options: { keep?: number; statePrefix?: string } = {},
): Promise<string[]> {
  const state = await readState(store, options.statePrefix);
  if (!state) {
    return [];
  }
  const named = new Set([state.manifest, ...state.pending.map((pending) => pending.manifest)]);
  const others = (await store.list(`${statePrefix(options.statePrefix)}manifests`))
    .filter((name) => MANIFEST_FILE.test(name))
    .map((name) => `manifests/${name}`)
    .filter((name) => !named.has(name))
    .sort()
    .reverse();
  const doomed = others.slice(Math.max(0, options.keep ?? 7));
  for (const name of doomed) {
    await store.delete(`${statePrefix(options.statePrefix)}${name}`);
  }
  return doomed;
}

export interface LiveStatus {
  liveBuild: string | null;
  updatedAt: string | null;
  osmDate: string | null;
  /** Hours since 00:00 UTC of the live build's date. */
  buildAgeHours: number | null;
  osmAgeHours: number | null;
  /** Builds a sync started writing and didn't make live: the zone may hold their bytes where they differ. */
  pending: string[];
}

const hoursSince = (time: number, now: Date) => Math.round((now.getTime() - time) / 360_000) / 10;

export function liveStatus(state: SyncState | null, now = new Date()): LiveStatus {
  if (!state) {
    return { liveBuild: null, updatedAt: null, osmDate: null, buildAgeHours: null, osmAgeHours: null, pending: [] };
  }
  const key = state.liveBuild;
  const buildDate = Date.UTC(Number(key.slice(0, 4)), Number(key.slice(4, 6)) - 1, Number(key.slice(6, 8)));
  return {
    liveBuild: key,
    updatedAt: state.updatedAt,
    osmDate: state.osmDate,
    buildAgeHours: hoursSince(buildDate, now),
    osmAgeHours: state.osmDate ? hoursSince(Date.parse(state.osmDate), now) : null,
    pending: state.pending.map((pending) => pending.build),
  };
}

/** In-memory store for tests; it checks checksums like Bunny does and logs every call. */
export class MemoryObjectStore implements ObjectStore {
  readonly objects = new Map<string, Uint8Array>();
  readonly log: string[] = [];

  async get(path: string) {
    this.log.push(`get ${path}`);
    const body = this.objects.get(path);
    return body ? new Uint8Array(body) : null;
  }

  async put(path: string, body: Uint8Array, sha256: string) {
    this.log.push(`put ${path}`);
    if (sha256Hex(body) !== sha256.toUpperCase()) {
      throw new Error(`Checksum mismatch for ${path}`);
    }
    this.objects.set(path, new Uint8Array(body));
  }

  async describe(path: string) {
    this.log.push(`describe ${path}`);
    const body = this.objects.get(path);
    return body ? { checksum: sha256Hex(body), length: body.length } : null;
  }

  async putFile(path: string, file: string, sha256: string) {
    await this.put(path, await readFile(file), sha256);
  }

  async getToFile(path: string, file: string) {
    const body = await this.get(path);
    if (body) {
      await writeFile(file, body);
    }
    return body !== null;
  }

  async list(dir: string) {
    const folder = `${dir.replace(/\/+$/, '')}/`;
    this.log.push(`list ${folder}`);
    return [...this.objects.keys()]
      .filter((path) => path.startsWith(folder) && !path.slice(folder.length).includes('/'))
      .map((path) => path.slice(folder.length));
  }

  async delete(path: string) {
    this.log.push(`delete ${path}`);
    this.objects.delete(path);
  }
}

export interface BunnyStateStoreOptions {
  zone: string;
  password: string;
  /** Storage hostname (default storage.bunnycdn.com), or a base URL such as http://127.0.0.1:8080 for tests. */
  host?: string;
  /** Tries per request (default 5), retried on network errors, timeouts, 429 and 5xx. */
  attempts?: number;
  /** Limit for each try of a small request (state.json, DESCRIBE), in ms. */
  requestTimeoutMs?: number;
  /** A manifest transfer fails when no bytes move, or no answer comes after the last one, for this long. */
  idleTimeoutMs?: number;
  /** Limit for each try of a whole manifest transfer, in ms. */
  transferTimeoutMs?: number;
}

/**
 * Small requests fail fast, so `bunny:status` gives up within about a minute when the zone hangs (requestBudgetMs: 5
 * tries of 10 s and at most 11.25 s of backoff between them), well inside the watchdog's job limit. Transfers have no
 * fixed length limit short of an hour, only an idle one: a 1.3 GB planet manifest took 14-18 s each way from a
 * well-connected server, and Bunny answered its PUT half a second after the last byte.
 */
export const STATE_STORE_DEFAULTS = {
  attempts: 5,
  requestTimeoutMs: 10_000,
  idleTimeoutMs: 60_000,
  transferTimeoutMs: 60 * 60_000,
};

class HttpError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
  }
}

const retryable = (error: unknown) => !(error instanceof HttpError) || error.status === 429 || error.status >= 500;

/**
 * The wait before try `attempt + 1`: doubling from 500 ms up to 15 s, jittered (`random` in [0, 1)) to between half
 * and 1.5 times that.
 */
const backoffMs = (attempt: number, random = Math.random()) =>
  Math.min(15_000, 500 * 2 ** (attempt - 1)) * (0.5 + random);

/**
 * The longest a small request (state.json, a DESCRIBE) takes to fail when the zone never answers: every try runs out
 * its requestTimeoutMs, with the longest backoff (the most jitter) between them.
 */
export function requestBudgetMs(settings: Pick<BunnyStateStoreOptions, 'attempts' | 'requestTimeoutMs'> = {}): number {
  const attempts = settings.attempts ?? STATE_STORE_DEFAULTS.attempts;
  const requestTimeoutMs = settings.requestTimeoutMs ?? STATE_STORE_DEFAULTS.requestTimeoutMs;
  let total = attempts * requestTimeoutMs;
  for (let attempt = 1; attempt < attempts; attempt++) {
    total += backoffMs(attempt, 1);
  }
  return total;
}

/** A small Bunny storage client for the state zone: a few objects, the large ones streamed. */
export class BunnyStateStore implements ObjectStore {
  private origin: string;
  private settings: Required<Omit<BunnyStateStoreOptions, 'host'>>;
  private agent: Agent;

  constructor(options: BunnyStateStoreOptions) {
    const host = options.host || 'storage.bunnycdn.com';
    this.origin = /^https?:\/\//.test(host) ? host.replace(/\/+$/, '') : `https://${host}`;
    const defined = Object.entries(options).filter(([, value]) => value !== undefined);
    this.settings = { ...STATE_STORE_DEFAULTS, ...(Object.fromEntries(defined) as BunnyStateStoreOptions) };
    this.agent = new Agent({ connect: { timeout: this.settings.requestTimeoutMs } });
  }

  /**
   * BUNNY_STATE_ZONE, BUNNY_STATE_PASSWORD and optionally BUNNY_STORAGE_HOST, or BUNNY_STATE_HOST when the state zone
   * is reached elsewhere than the tiles zone (a local stand-in in tests).
   */
  static fromEnv(): BunnyStateStore {
    return new BunnyStateStore({
      zone: requireEnv('BUNNY_STATE_ZONE'),
      password: requireEnv('BUNNY_STATE_PASSWORD'),
      host: process.env.BUNNY_STATE_HOST || process.env.BUNNY_STORAGE_HOST,
    });
  }

  close(): Promise<void> {
    return this.agent.close();
  }

  private path(path: string) {
    return `/${encodeURIComponent(this.settings.zone)}/${path.split('/').map(encodeURIComponent).join('/')}`;
  }

  /**
   * One request with retries. Small requests get a deadline per try; transfers an inactivity limit instead (undici's
   * headers and body timeouts, refreshed as the body moves) plus a generous deadline.
   */
  private async request<T>(
    method: string,
    path: string,
    handle: (response: Dispatcher.ResponseData) => Promise<T>,
    init: { body?: () => Uint8Array | Readable; headers?: Record<string, string>; transfer?: boolean } = {},
  ): Promise<T> {
    const { attempts, requestTimeoutMs, idleTimeoutMs, transferTimeoutMs } = this.settings;
    const idle = init.transfer ? idleTimeoutMs : requestTimeoutMs;
    for (let attempt = 1; ; attempt++) {
      const body = init.body?.();
      try {
        const response = await this.agent.request({
          origin: this.origin,
          path: this.path(path),
          method,
          headers: { AccessKey: this.settings.password, ...init.headers },
          body,
          signal: AbortSignal.timeout(init.transfer ? transferTimeoutMs : requestTimeoutMs),
          headersTimeout: idle,
          bodyTimeout: idle,
        });
        try {
          return await handle(response);
        } finally {
          // Reads what is left of a small answer (or drops the connection), without an unhandled error event.
          await response.body.dump().catch(() => {});
        }
      } catch (error) {
        if (attempt >= attempts || !retryable(error)) {
          throw error;
        }
        await sleep(backoffMs(attempt));
      } finally {
        if (body instanceof Readable) {
          body.destroy();
        }
      }
    }
  }

  private static fail(method: string, path: string, response: Dispatcher.ResponseData): never {
    throw new HttpError(`${method} ${path}: ${response.statusCode}`, response.statusCode);
  }

  get(path: string): Promise<Uint8Array | null> {
    return this.request('GET', path, async (response) => {
      if (response.statusCode === 404) {
        return null;
      }
      if (response.statusCode !== 200) {
        BunnyStateStore.fail('GET', path, response);
      }
      return new Uint8Array(await response.body.arrayBuffer());
    });
  }

  getToFile(path: string, file: string): Promise<boolean> {
    return this.request(
      'GET',
      path,
      async (response) => {
        if (response.statusCode === 404) {
          return false;
        }
        if (response.statusCode !== 200) {
          BunnyStateStore.fail('GET', path, response);
        }
        await pipeline(response.body, createWriteStream(file));
        return true;
      },
      { transfer: true },
    );
  }

  private static expectStored(path: string) {
    return async (response: Dispatcher.ResponseData) => {
      if (response.statusCode !== 201 && response.statusCode !== 200) {
        BunnyStateStore.fail('PUT', path, response);
      }
    };
  }

  put(path: string, body: Uint8Array, sha256: string): Promise<void> {
    return this.request('PUT', path, BunnyStateStore.expectStored(path), {
      body: () => body,
      headers: { Checksum: sha256.toUpperCase(), 'Content-Type': 'application/octet-stream' },
    });
  }

  /** Read in 1 MB pieces with an explicit length, so memory stays flat and each write keeps the request alive. */
  async putFile(path: string, file: string, sha256: string): Promise<void> {
    const { size } = await stat(file);
    return this.request('PUT', path, BunnyStateStore.expectStored(path), {
      body: () => createReadStream(file, { highWaterMark: 1 << 20 }),
      headers: {
        Checksum: sha256.toUpperCase(),
        'Content-Type': 'application/octet-stream',
        'Content-Length': String(size),
      },
      transfer: true,
    });
  }

  list(dir: string): Promise<string[]> {
    const folder = dir.replace(/^\/+|\/+$/g, '');
    return this.request('GET', folder ? `${folder}/` : '', async (response) => {
      if (response.statusCode === 404) {
        return [];
      }
      if (response.statusCode !== 200) {
        BunnyStateStore.fail('GET', `${folder}/`, response);
      }
      const rows = (await response.body.json()) as Array<{ ObjectName?: string; IsDirectory?: boolean }>;
      if (!Array.isArray(rows)) {
        throw new Error(`GET ${folder}/: unexpected listing`);
      }
      return rows.filter((row) => !row.IsDirectory && typeof row.ObjectName === 'string').map((row) => row.ObjectName!);
    });
  }

  delete(path: string): Promise<void> {
    return this.request('DELETE', path, async (response) => {
      if (response.statusCode !== 200 && response.statusCode !== 204 && response.statusCode !== 404) {
        BunnyStateStore.fail('DELETE', path, response);
      }
    });
  }

  describe(path: string): Promise<StoredObject | null> {
    return this.request('DESCRIBE', path, async (response) => {
      if (response.statusCode === 404) {
        return null;
      }
      if (response.statusCode !== 200) {
        BunnyStateStore.fail('DESCRIBE', path, response);
      }
      const object = (await response.body.json()) as { Checksum?: string; Length?: number; IsDirectory?: boolean };
      if (object.IsDirectory || typeof object.Checksum !== 'string' || typeof object.Length !== 'number') {
        throw new Error(`DESCRIBE ${path}: unexpected answer`);
      }
      return { checksum: object.Checksum.toUpperCase(), length: object.Length };
    });
  }
}
