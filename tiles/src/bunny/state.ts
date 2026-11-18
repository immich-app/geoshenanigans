/**
 * Sync state in the state zone (`immich-tiles-state`): `state.json` names the live build and its manifest, kept in
 * `manifests/<build>.tman.zst` for the next day's diff. Every path can sit under a state prefix, for tests.
 *
 * The store is anything with get/put/describe, such as Track A's BunnyStorage or BunnyStateStore below. The state
 * zone has no replicas, so re-uploading identical bytes is harmless there.
 */
import { createHash } from 'node:crypto';
import { createReadStream, createWriteStream } from 'node:fs';
import { open, rename, rm } from 'node:fs/promises';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { setTimeout as sleep } from 'node:timers/promises';
import { fetch, type Response } from 'undici';
import { parseDeploymentKey, requireEnv } from '../deploy/config';
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
  /** Optional streaming download, used for manifests when present; false when the object doesn't exist. */
  getToFile?(path: string, file: string): Promise<boolean>;
}

export interface SyncState {
  liveBuild: string;
  /** Relative to the state prefix, e.g. "manifests/20261118.tman.zst". */
  manifest: string;
  /** Tile prefix in the tiles zone, e.g. "v1". */
  prefix: string;
  updatedAt: string;
  osmDate: string | null;
  positions: number;
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

export const sha256Hex = (body: Uint8Array) => createHash('sha256').update(body).digest('hex').toUpperCase();

export async function sha256File(file: string): Promise<string> {
  const hash = createHash('sha256');
  await pipeline(createReadStream(file), hash);
  return hash.digest('hex').toUpperCase();
}

// fs.readFile stops at 2 GiB; a planet manifest is over 1 GB, so read it in pieces.
async function readWholeFile(file: string): Promise<Uint8Array> {
  const handle = await open(file, 'r');
  try {
    const { size } = await handle.stat();
    const bytes = Buffer.allocUnsafe(size);
    let done = 0;
    while (done < size) {
      const { bytesRead } = await handle.read(bytes, done, Math.min(size - done, 1 << 30), done);
      if (bytesRead === 0) {
        throw new Error(`${file} shrank while reading it`);
      }
      done += bytesRead;
    }
    return bytes;
  } finally {
    await handle.close();
  }
}

const isIsoDate = (value: unknown) => typeof value === 'string' && !Number.isNaN(Date.parse(value));

export function parseState(value: unknown): SyncState {
  const state = value as Partial<SyncState> | null;
  if (typeof state !== 'object' || state === null) {
    throw new Error('state.json is not an object');
  }
  parseDeploymentKey('state.json liveBuild', state.liveBuild);
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
  if (!Number.isSafeInteger(state.positions) || (state.positions as number) < 0) {
    problems.push('positions');
  }
  if (problems.length > 0) {
    throw new Error(`state.json has invalid ${problems.join(', ')}`);
  }
  return {
    liveBuild: state.liveBuild!,
    manifest: state.manifest!,
    prefix: state.prefix!,
    updatedAt: state.updatedAt!,
    osmDate: state.osmDate ?? null,
    positions: state.positions!,
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
  await putVerified(store, statePath(prefix), body, sha256Hex(body));
}

async function putVerified(store: ObjectStore, path: string, body: Uint8Array, sha256: string) {
  await store.put(path, body, sha256);
  const stored = await store.describe(path);
  if (!stored || stored.checksum.toUpperCase() !== sha256 || stored.length !== body.length) {
    throw new Error(`${path} reads back as ${stored ? `${stored.length} bytes ${stored.checksum}` : 'missing'}`);
  }
}

export interface UploadedManifest {
  path: string;
  sha256: string;
  bytes: number;
  /** Already stored with these bytes, so not sent again. */
  skipped: boolean;
}

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
  const path = manifestPath(buildKey, prefix);
  const sha256 = await sha256File(file);
  const existing = await store.describe(path);
  if (existing && existing.checksum.toUpperCase() === sha256) {
    return { path, sha256, bytes: existing.length, skipped: true };
  }
  const body = await readWholeFile(file);
  await putVerified(store, path, body, sha256);
  return { path, sha256, bytes: body.length, skipped: false };
}

/** Downloads the live build's manifest to `file`, checked against the stored checksum and the state. */
export async function downloadManifest(
  store: ObjectStore,
  state: SyncState,
  file: string,
  prefix?: string,
): Promise<void> {
  const path = `${statePrefix(prefix)}${state.manifest}`;
  const stored = await store.describe(path);
  if (!stored) {
    throw new Error(`state.json names ${path}, which doesn't exist`);
  }
  const partial = `${file}.partial`;
  try {
    if (store.getToFile) {
      if (!(await store.getToFile(path, partial))) {
        throw new Error(`${path} disappeared while downloading it`);
      }
    } else {
      const bytes = await store.get(path);
      if (!bytes) {
        throw new Error(`${path} disappeared while downloading it`);
      }
      const handle = await open(partial, 'w');
      try {
        await handle.writeFile(bytes);
      } finally {
        await handle.close();
      }
    }
    const sha256 = await sha256File(partial);
    if (sha256 !== stored.checksum.toUpperCase()) {
      throw new Error(`${path} downloaded as ${sha256}, stored as ${stored.checksum}`);
    }
    const header = await readManifestHeader(partial);
    if (header.buildKey !== state.liveBuild || header.positions !== state.positions) {
      throw new Error(
        `${path} is ${header.buildKey} with ${header.positions} positions; state.json says ${state.liveBuild} with ${state.positions}`,
      );
    }
    await rename(partial, file);
  } finally {
    await rm(partial, { force: true });
  }
}

export interface PublishOptions {
  buildKey: string;
  manifestFile: string;
  /** Tile prefix in the tiles zone. */
  prefix: string;
  /** Defaults to the manifest's (the archive metadata's OSM replication time). */
  osmDate?: string | null;
  statePrefix?: string;
  now?: Date;
}

/**
 * Makes a build live in the state: its manifest is stored and read back first, then state.json switches to it, so
 * state.json never names a manifest that isn't there.
 */
export async function publishBuild(store: ObjectStore, options: PublishOptions): Promise<SyncState> {
  const header = await readManifestHeader(options.manifestFile);
  await uploadManifest(store, options.buildKey, options.manifestFile, options.statePrefix);
  const state: SyncState = {
    liveBuild: options.buildKey,
    manifest: manifestName(options.buildKey),
    prefix: options.prefix,
    updatedAt: (options.now ?? new Date()).toISOString(),
    osmDate: options.osmDate === undefined ? header.osmDate : options.osmDate,
    positions: header.positions,
  };
  await writeState(store, state, options.statePrefix);
  return state;
}

export interface LiveStatus {
  liveBuild: string | null;
  updatedAt: string | null;
  osmDate: string | null;
  /** Hours since 00:00 UTC of the live build's date. */
  buildAgeHours: number | null;
  osmAgeHours: number | null;
}

const hoursSince = (time: number, now: Date) => Math.round((now.getTime() - time) / 360_000) / 10;

export function liveStatus(state: SyncState | null, now = new Date()): LiveStatus {
  if (!state) {
    return { liveBuild: null, updatedAt: null, osmDate: null, buildAgeHours: null, osmAgeHours: null };
  }
  const key = state.liveBuild;
  const buildDate = Date.UTC(Number(key.slice(0, 4)), Number(key.slice(4, 6)) - 1, Number(key.slice(6, 8)));
  return {
    liveBuild: key,
    updatedAt: state.updatedAt,
    osmDate: state.osmDate,
    buildAgeHours: hoursSince(buildDate, now),
    osmAgeHours: state.osmDate ? hoursSince(Date.parse(state.osmDate), now) : null,
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
}

export interface BunnyStateStoreOptions {
  zone: string;
  password: string;
  /** Storage hostname (default storage.bunnycdn.com), or a base URL such as http://127.0.0.1:8080 for tests. */
  host?: string;
  attempts?: number;
  timeoutMs?: number;
}

class HttpError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
  }
}

const retryable = (error: unknown) => !(error instanceof HttpError) || error.status === 429 || error.status >= 500;

/** A small Bunny storage client for the state zone: a few objects, retried on network errors, 429 and 5xx. */
export class BunnyStateStore implements ObjectStore {
  private base: string;

  constructor(private options: BunnyStateStoreOptions) {
    const host = options.host || 'storage.bunnycdn.com';
    this.base = /^https?:\/\//.test(host) ? host.replace(/\/+$/, '') : `https://${host}`;
  }

  /** BUNNY_STATE_ZONE, BUNNY_STATE_PASSWORD and optionally BUNNY_STORAGE_HOST. */
  static fromEnv(): BunnyStateStore {
    return new BunnyStateStore({
      zone: requireEnv('BUNNY_STATE_ZONE'),
      password: requireEnv('BUNNY_STATE_PASSWORD'),
      host: process.env.BUNNY_STORAGE_HOST,
    });
  }

  private url(path: string) {
    const encoded = path.split('/').map(encodeURIComponent).join('/');
    return `${this.base}/${encodeURIComponent(this.options.zone)}/${encoded}`;
  }

  private async request<T>(
    method: string,
    path: string,
    handle: (response: Response) => Promise<T>,
    init: { body?: Uint8Array; headers?: Record<string, string>; timeoutMs?: number } = {},
  ): Promise<T> {
    const attempts = this.options.attempts ?? 5;
    for (let attempt = 1; ; attempt++) {
      try {
        const response = await fetch(this.url(path), {
          method,
          headers: { AccessKey: this.options.password, ...init.headers },
          body: init.body,
          signal: AbortSignal.timeout(init.timeoutMs ?? this.options.timeoutMs ?? 30 * 60_000),
        });
        try {
          return await handle(response);
        } finally {
          if (!response.bodyUsed) {
            await response.body?.cancel();
          }
        }
      } catch (error) {
        if (attempt >= attempts || !retryable(error)) {
          throw error;
        }
        await sleep(Math.min(15_000, 500 * 2 ** (attempt - 1)) * (0.5 + Math.random()));
      }
    }
  }

  private static fail(method: string, path: string, response: Response): never {
    throw new HttpError(`${method} ${path}: ${response.status} ${response.statusText}`, response.status);
  }

  get(path: string): Promise<Uint8Array | null> {
    return this.request('GET', path, async (response) => {
      if (response.status === 404) {
        return null;
      }
      if (response.status !== 200) {
        BunnyStateStore.fail('GET', path, response);
      }
      return new Uint8Array(await response.arrayBuffer());
    });
  }

  getToFile(path: string, file: string): Promise<boolean> {
    return this.request('GET', path, async (response) => {
      if (response.status === 404) {
        return false;
      }
      if (response.status !== 200 || !response.body) {
        BunnyStateStore.fail('GET', path, response);
      }
      await pipeline(Readable.fromWeb(response.body), createWriteStream(file));
      return true;
    });
  }

  put(path: string, body: Uint8Array, sha256: string): Promise<void> {
    return this.request(
      'PUT',
      path,
      async (response) => {
        if (response.status !== 201 && response.status !== 200) {
          BunnyStateStore.fail('PUT', path, response);
        }
      },
      { body, headers: { Checksum: sha256.toUpperCase(), 'Content-Type': 'application/octet-stream' } },
    );
  }

  describe(path: string): Promise<StoredObject | null> {
    return this.request(
      'DESCRIBE',
      path,
      async (response) => {
        if (response.status === 404) {
          return null;
        }
        if (response.status !== 200) {
          BunnyStateStore.fail('DESCRIBE', path, response);
        }
        const object = (await response.json()) as { Checksum?: string; Length?: number; IsDirectory?: boolean };
        if (object.IsDirectory || typeof object.Checksum !== 'string' || typeof object.Length !== 'number') {
          throw new Error(`DESCRIBE ${path}: unexpected answer`);
        }
        return { checksum: object.Checksum.toUpperCase(), length: object.Length };
      },
      { timeoutMs: 60_000 },
    );
  }
}
