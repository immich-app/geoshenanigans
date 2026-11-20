import { ChildProcess, spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { renameSync, utimesSync } from 'node:fs';
import { copyFile, mkdir, readdir, readFile, rename, stat, writeFile } from 'node:fs/promises';
import { createServer, Server } from 'node:http';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { archiveIdentity } from '../../src/bunny/archive-identity';
import { ArchiveRanges } from '../../src/bunny/archive-walk';
import { clipManifest, ManifestEntry, readManifest, writeManifest } from '../../src/bunny/manifest';
import {
  BunnyStateStore,
  downloadManifest,
  ObjectStore,
  PendingBuild,
  readState,
  SyncState,
} from '../../src/bunny/state';
import { BunnyStorage } from '../../src/bunny/storage';
import { planItems, runSync, SYNC_DEFAULTS, SyncOptions, SyncReport } from '../../src/bunny/sync';
import { tileIdZoom, tilePath, zoomEnd } from '../../src/bunny/tile-id';
import { FakeStorage, FakeStorageOptions, sha256Hex } from './fake-storage';
import {
  Build,
  contentBytes,
  expectedEntries,
  mutateBuild,
  randomBuild,
  rng,
  writeArchiveFile,
} from './manifest-fixtures';
import { scratchDir, withOtherTileBytes } from './upload-fixture';

const TILES_DIR = join(import.meta.dirname, '../..');
const REGIONS = ['UK', 'NY', 'SYD'];
/** Every position of zooms 0-6. */
const POSITIONS = zoomEnd(6);
const fast = { baseDelayMs: 1, maxDelayMs: 5, settleMs: 5, maxAttempts: 4, maxRateLimitWaitMs: 5_000 };
const STYLE = {
  name: 'light',
  body: new TextEncoder().encode(
    JSON.stringify(
      { version: 8, sources: { vector: { type: 'vector', url: 'https://tiles.example/v1.json' } } },
      null,
      2,
    ),
  ),
};
const METADATA = { name: 'test', 'planetiler:osm:osmosisreplicationtime': '2026-11-17T20:21:22Z' };
const CONTRACT_FIELDS = [
  'status',
  'build',
  'previousBuild',
  'osmDate',
  'positionsTotal',
  'positionsChanged',
  'changedFraction',
  'filesUploaded',
  'bytesUploaded',
  'retries',
  'rateLimited',
  'replicationComplete',
  'replicationWaitSeconds',
  'stepSeconds',
  'error',
];

let cleanup: (() => Promise<void> | void)[] = [];

afterEach(async () => {
  for (const step of cleanup.reverse()) {
    await step();
  }
  cleanup = [];
});

interface World {
  dir: string;
  tiles: FakeStorage;
  stateZone: FakeStorage;
  storage: BunnyStorage;
  state: BunnyStateStore;
  /** Every event in order: tile and state zone requests that write, gate DESCRIBEs and purges. */
  events: string[];
  purges: number;
  archives: Record<string, { path: string; build: Build }>;
  /** A completed seed's state dir of each build over every position under v1, as bunny:seed leaves it. */
  seedStates: Record<string, string>;
  /** The regions that replicate on their own; the others only on tiles.replicate(). */
  autoRegions: string[];
  options(build: string, extra?: Partial<SyncOptions>): SyncOptions;
  deps(): Parameters<typeof runSync>[1];
}

/** Two builds a day apart (X, then Y with some tiles changed), a tiles zone and a state zone. */
async function world(tileOptions: FakeStorageOptions = {}): Promise<World> {
  const { dir, remove } = await scratchDir();
  cleanup.push(remove);
  const r = rng(42);
  const x = randomBuild(r, POSITIONS);
  const y = mutateBuild(r, x);
  const archives: World['archives'] = {};
  for (const [key, build] of [
    ['20261117', x],
    ['20261118', y],
  ] as const) {
    const path = join(dir, `${key}.pmtiles`);
    await writeArchiveFile(path, build, r, { maxZoom: 6, metadata: METADATA, maxLeafEntries: 40 });
    archives[key] = { path, build };
  }
  const events: string[] = [];
  const replicationDelayMs = tileOptions.replicationDelayMs ?? { UK: 5, NY: 10, SYD: 20 };
  // Each one's cleanup as it opens: one that fails to start (or a seed state that can't be made) leaves none open.
  const tiles = await FakeStorage.start({
    regions: REGIONS,
    ...tileOptions,
    replicationDelayMs,
    onRequest: (request) => {
      if (request.method !== 'GET') {
        events.push(`tiles ${request.method} ${request.path}`);
      }
      tileOptions.onRequest?.(request);
    },
  });
  cleanup.push(() => tiles.close());
  const stateZone = await FakeStorage.start({
    zone: 'state-zone',
    onRequest: ({ method, path }) => method === 'PUT' && events.push(`state PUT ${path}`),
  });
  cleanup.push(() => stateZone.close());
  const storage = new BunnyStorage({
    zone: tiles.zone,
    password: tiles.password,
    host: tiles.url,
    retry: fast,
    timeoutMs: 1_000,
    concurrency: 32,
  });
  cleanup.push(() => storage.close());
  const state = new BunnyStateStore({ zone: stateZone.zone, password: stateZone.password, host: stateZone.url });
  cleanup.push(() => state.close());
  const seedStates: World['seedStates'] = {};
  for (const key of Object.keys(archives)) {
    seedStates[key] = await seedState({ dir, zone: tiles.zone, archive: archives[key].path });
  }
  const w: World = {
    dir,
    tiles,
    stateZone,
    storage,
    state,
    events,
    purges: 0,
    archives,
    seedStates,
    autoRegions: REGIONS.filter((region) => region in replicationDelayMs),
    options: (build, extra = {}) => ({
      archive: archives[build].path,
      build,
      seedStates: [seedStates[build]],
      maxChangedFraction: 1,
      replicationTimeoutMs: 5_000,
      replicationIntervalMs: 20,
      regions: REGIONS,
      publicUrl: 'https://tiles.example',
      styles: [STYLE],
      workDir: dir,
      threads: 2,
      concurrency: 16,
      adoptSamples: 200,
      ...extra,
    }),
    deps: () => ({
      state,
      tiles: storage,
      purge: async () => {
        w.purges++;
        events.push('purge');
      },
    }),
  };
  return w;
}

let seedStateDirs = 0;

/**
 * The state dir a seed leaves (seed.json and status.json), over [from, to) of an archive. `legacy`: as a bunny:seed
 * before the v2 identity left it, with only the v1 fingerprint (header, root directory and size). status.json names
 * the archive's path (`statusArchive` instead, if given) and its last run's start, just after seed.json was written
 * (`startedAt` instead, if given).
 */
async function seedState(options: {
  dir: string;
  zone: string;
  archive: string;
  prefix?: string;
  from?: number;
  to?: number;
  state?: string;
  legacy?: boolean;
  statusArchive?: string | null;
  startedAt?: string;
}): Promise<string> {
  const ranges = await ArchiveRanges.open(options.archive);
  await ranges.close();
  const dir = join(options.dir, `seed-state-${++seedStateDirs}`);
  await mkdir(dir, { recursive: true });
  const range = { from: options.from ?? 0, to: options.to ?? POSITIONS };
  const params = {
    zone: options.zone,
    host: 'http://127.0.0.1',
    prefix: options.prefix ?? 'v1',
    ...range,
    chunkSize: 64,
    archive: options.legacy
      ? { size: ranges.size, fingerprint: ranges.legacyFingerprint }
      : await archiveIdentity(options.archive),
  };
  await writeFile(join(dir, 'seed.json'), JSON.stringify(params));
  const startedAt = options.startedAt ?? new Date(Math.ceil((await stat(join(dir, 'seed.json'))).mtimeMs) + 1);
  const status = {
    state: options.state ?? 'complete',
    startedAt,
    archive: options.statusArchive === undefined ? options.archive : options.statusArchive,
    range,
    error: null,
  };
  await writeFile(join(dir, 'status.json'), JSON.stringify(status));
  return dir;
}

/** The checksum of a build's tile at a position, or undefined where it has none (a gap). */
function checksumAt(build: Build, id: number): string | undefined {
  const content = build.contents[id - build.base];
  return content === null || content === undefined ? undefined : sha256Hex(contentBytes(content));
}

/** Stores build `key` at every position it has as a seed would have, replicated everywhere. */
function seedZone(w: World, key: string, prefix = 'v1', end = POSITIONS, regions = REGIONS) {
  const { build } = w.archives[key];
  for (let id = 0; id < Math.min(end, build.contents.length); id++) {
    const content = build.contents[id];
    if (content !== null && content !== undefined) {
      w.tiles.seed(tilePath(prefix, id), contentBytes(content), regions);
    }
  }
}

/** Positions whose tile differs between two builds, a position only one of them has included. */
function changedPositions(w: World, from: string, to: string, end = POSITIONS): number[] {
  const a = w.archives[from].build;
  const b = w.archives[to].build;
  const changed: number[] = [];
  for (let id = 0; id < end; id++) {
    if (checksumAt(a, id) !== checksumAt(b, id)) {
      changed.push(id);
    }
  }
  return changed;
}

/** The zone holds build `key`: its tile at every position it has, and no file where it has none. */
function expectHolds(w: World, key: string, prefix = 'v1', end = POSITIONS) {
  const { build } = w.archives[key];
  for (let id = 0; id < end; id++) {
    expect(w.tiles.checksum(tilePath(prefix, id)), `position ${id}`).toBe(checksumAt(build, id));
  }
}

/**
 * A gate timeout for runs whose gate must pass over thousands of files: the timeout bounds the whole gate, first pass
 * included, so a short one fails on a slow runner whatever the zone holds. What ends such a gate is its files
 * replicating, on their own or when the test replicates them (afterGatePass). Below vitest's 30 s, so a gate that
 * never completes fails the test's own assertions.
 */
const GATE_TIMEOUT_MS = 20_000;

/**
 * The timeout of runs whose tile gate must pass, over a few hundred stored tiles, and whose static gate must then time
 * out (the timeout bounds each): a pass that size fits on a loaded runner with room to spare.
 */
const STATIC_TIMEOUT_MS = 1_000;

/**
 * A `log` that calls `then` after every pass of the tile gate, with the files it left pending and the regions they
 * lacked ("UK 3, SYD 12", or "none"), as its log line gives them.
 */
function onGatePass(then: (pass: number, pending: number, lacking: string) => void) {
  return (line: string) => {
    const match = /^replication pass (\d+) at [\d.]+ s: (\d+) files pending \((.*)\)$/.exec(line);
    if (match) {
      then(Number(match[1]), Number(match[2]), match[3]);
    }
  };
}

/** A `log` that calls `then` once, when the tile gate has finished pass `pass` (see onGatePass). */
function afterGatePass(pass: number, then: (pending: number, lacking: string) => void) {
  return onGatePass((done, pending, lacking) => done === pass && then(pending, lacking));
}

/**
 * A gate timeout the test ends: a gate given it (its deadline is AbortSignal.timeout(timeoutMs)) runs out of time
 * when the test calls expire(), not before, however long its passes take on a loaded runner.
 */
const CONTROLLED_TIMEOUT_MS = 3_600_017;

function controlledTimeout() {
  const timeout = AbortSignal.timeout.bind(AbortSignal);
  let deadline: AbortController | undefined;
  const spy = vi.spyOn(AbortSignal, 'timeout').mockImplementation((ms: number) => {
    if (ms !== CONTROLLED_TIMEOUT_MS) {
      return timeout(ms);
    }
    deadline = new AbortController();
    return deadline.signal;
  });
  cleanup.push(() => spy.mockRestore());
  return {
    /** Ends the latest gate given CONTROLLED_TIMEOUT_MS, as its timeout would. */
    expire: () => deadline?.abort(new DOMException('The operation was aborted due to timeout', 'TimeoutError')),
  };
}

/**
 * When each gate's passes first found every file in each region, in seconds from the gate's start as its log gives
 * them (to 0.1 s): one record per gate, in order.
 */
function gateWaits(lines: string[]): Array<Record<string, number>> {
  const gates: Array<{ regions: string[]; waits: Record<string, number> }> = [];
  for (const line of lines) {
    const start = /^replication: waiting for \d+ (?:static )?files in (.*)$/.exec(line);
    if (start) {
      gates.push({ regions: start[1].split(', '), waits: {} });
      continue;
    }
    const pass = /^replication pass \d+ at ([\d.]+) s: \d+ (?:static )?files pending \((.*)\)$/.exec(line);
    if (pass) {
      const gate = gates[gates.length - 1];
      const lacking = pass[2] === 'none' ? [] : pass[2].split(', ').map((entry) => entry.split(' ')[0]);
      for (const region of gate.regions) {
        if (!lacking.includes(region) && !(region in gate.waits)) {
          gate.waits[region] = Number(pass[1]);
        }
      }
    }
  }
  return gates.map((gate) => gate.waits);
}

/** The state zone, but every write of state.json fails, as when the zone goes away after a manifest's upload. */
function failingStateWrites(store: BunnyStateStore): ObjectStore {
  return {
    get: (path) => store.get(path),
    put: (path, body, sha256) =>
      path === 'state.json' ? Promise.reject(new Error('PUT state.json: 503')) : store.put(path, body, sha256),
    putFile: (path, file, sha256) => store.putFile(path, file, sha256),
    describe: (path) => store.describe(path),
    getToFile: (path, file) => store.getToFile(path, file),
    list: (dir) => store.list(dir),
    delete: (path) => store.delete(path),
  };
}

let storedManifests = 0;

/** The entries of the manifest `state` names (the live build's, or a pending one's), as the state zone holds it. */
async function storedEntries(w: World, state: SyncState | PendingBuild) {
  const file = join(w.dir, `stored-${++storedManifests}.tman.zst`);
  await downloadManifest(w.state, state, file);
  const entries: ManifestEntry[] = [];
  for await (const entry of readManifest(file)) {
    entries.push(entry);
  }
  return entries;
}

function lastIndex<T>(items: T[], test: (item: T) => boolean): number {
  for (let i = items.length - 1; i >= 0; i--) {
    if (test(items[i])) {
      return i;
    }
  }
  return -1;
}

const tilePuts = (w: World) => [...w.tiles.putsByPath.entries()].filter(([path]) => path.endsWith('.mvt'));

/**
 * Adopts a seed of build `key`. The TileJSON and styles it writes are gated in the regions that replicate on their own
 * (all of them unless the world holds some back), as an adoption with --regions would.
 */
async function adopted(w: World, key = '20261117', extra: Partial<SyncOptions> = {}) {
  seedZone(w, key);
  const report = await runSync(w.options(key, { adopt: true, regions: w.autoRegions, ...extra }), w.deps());
  expect(report).toMatchObject({ status: 'success', mode: 'adopt', filesUploaded: null, error: null });
  w.events.length = 0;
  w.purges = 0;
  w.tiles.putsByPath.clear();
  return report;
}

describe('world', () => {
  it('closes what it opened when a later part of it fails to start', async () => {
    const start = FakeStorage.start.bind(FakeStorage);
    const started: FakeStorage[] = [];
    const spy = vi.spyOn(FakeStorage, 'start').mockImplementation(async (options) => {
      if (started.length > 0) {
        throw new Error('listen EADDRINUSE');
      }
      const fake = await start(options);
      started.push(fake);
      return fake;
    });
    try {
      await expect(world()).rejects.toThrow(/EADDRINUSE/);
    } finally {
      spy.mockRestore();
    }
    expect(started).toHaveLength(1);
    expect((await fetch(`${started[0].url}/`)).status).toBe(401);
    // What afterEach runs: the tiles zone that did start is closed with the rest.
    for (const step of cleanup.reverse()) {
      await step();
    }
    cleanup = [];
    await expect(fetch(`${started[0].url}/`)).rejects.toThrow(/fetch failed/);
  });
});

describe('runSync', () => {
  it('adopts a seeded build without writing tiles', async () => {
    const w = await world();
    seedZone(w, '20261117');
    const report = await runSync(w.options('20261117', { adopt: true }), w.deps());

    expect(report).toMatchObject({
      status: 'success',
      mode: 'adopt',
      previousBuild: null,
      positionsTotal: POSITIONS,
      positionsVerified: 200,
      osmDate: '2026-11-17T20:21:22Z',
      staticUploaded: 3,
      replicationComplete: true,
      purged: true,
    });
    expect(tilePuts(w)).toEqual([]);
    expect(w.purges).toBe(1);
    // The TileJSON and styles it wrote reached every region before the purge.
    expect(Object.keys(report.replicationWaitSeconds!).sort()).toEqual([...REGIONS].sort());
    const put = w.events.indexOf('tiles PUT v1.json');
    expect(put).toBeGreaterThanOrEqual(0);
    expect(w.events.indexOf('tiles DESCRIBE v1.json', put + 1)).toBeGreaterThan(put);
    expect(w.events.indexOf('tiles DESCRIBE v1.json', put + 1)).toBeLessThan(w.events.indexOf('purge'));
    const state = (await readState(w.state)) as SyncState;
    expect(state).toMatchObject({ liveBuild: '20261117', prefix: 'v1', positions: POSITIONS, pending: [] });
    expect(w.stateZone.checksum(state.manifest)).toBeDefined();
    const tileJson = JSON.parse(new TextDecoder().decode(w.tiles.files.get('v1.json')!.bytes)) as { tiles: string[] };
    expect(tileJson.tiles).toEqual(['https://tiles.example/v1/{z}/{x}/{y}.mvt']);
    expect(w.tiles.files.has('v1/style/light.json')).toBe(true);
    expect(w.tiles.files.has('v1/style/light')).toBe(true);
  });

  it('adopts only once the TileJSON and styles it wrote are in every region', async () => {
    const w = await world({ replicationDelayMs: { UK: 5, NY: 10 } });
    seedZone(w, '20261117');
    const adopt = () => runSync(w.options('20261117', { adopt: true, replicationTimeoutMs: 300 }), w.deps());
    const first = await adopt();
    expect(first).toMatchObject({ status: 'failed', staticUploaded: 3, replicationComplete: false, purged: false });
    expect(first.error).toMatch(/Replication of the TileJSON and styles incomplete.*\(not in SYD\).*--regions none/);
    expect(first.replicationWaitSeconds).toMatchObject({ SYD: null });
    expect(w.purges).toBe(0);
    expect(await readState(w.state)).toBeNull();

    // The retry finds them stored, writes nothing and adopts once they have replicated.
    w.tiles.replicate();
    const second = await adopt();
    expect(second.error).toBeNull();
    expect(second).toMatchObject({ status: 'success', staticUploaded: 0, replicationComplete: true });
    expect(w.purges).toBe(1);
    expect(w.tiles.identicalReputs).toEqual([]);
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261117', pending: [] });
  });

  it("refuses to adopt unless the seed's state dirs say it completed, for this archive, zone and prefix", async () => {
    const w = await world();
    seedZone(w, '20261117');
    const archive = w.archives['20261117'].path;
    const half = Math.floor(POSITIONS / 2);
    const cases: Array<[string[], RegExp]> = [
      [[await seedState({ dir: w.dir, zone: w.tiles.zone, archive, state: 'failed' })], /last run ended failed/],
      [[await seedState({ dir: w.dir, zone: 'other-zone', archive })], /seeded zone other-zone/],
      [[await seedState({ dir: w.dir, zone: w.tiles.zone, archive, prefix: 'v2' })], /seeded v2\/, not v1\//],
      [[w.seedStates['20261118']], /another archive/],
      [
        [await seedState({ dir: w.dir, zone: w.tiles.zone, archive, to: half })],
        new RegExp(`cover tile IDs \\[0, ${half}\\), not tile ${half} of the build`),
      ],
      [
        [
          await seedState({ dir: w.dir, zone: w.tiles.zone, archive, to: half - 1 }),
          await seedState({ dir: w.dir, zone: w.tiles.zone, archive, from: half }),
        ],
        new RegExp(`not tile ${half - 1} of the build`),
      ],
      [[join(w.dir, 'nothing')], /not a seed's state dir/],
      [[], /needs .*--seed-state/],
    ];
    for (const [seedStates, error] of cases) {
      const report = await runSync(w.options('20261117', { adopt: true, seedStates }), w.deps());
      expect(report.status, String(error)).toBe('failed');
      expect(report.error).toMatch(error);
    }
    expect(await readState(w.state)).toBeNull();
    expect(w.purges).toBe(0);

    const split = [
      await seedState({ dir: w.dir, zone: w.tiles.zone, archive, from: half }),
      await seedState({ dir: w.dir, zone: w.tiles.zone, archive, to: half }),
    ];
    const report = await runSync(w.options('20261117', { adopt: true, seedStates: split }), w.deps());
    expect(report).toMatchObject({ status: 'success', error: null });
  });

  it("checks the seeds' ranges against the tile IDs the build has, not its count of positions", async () => {
    const w = await world();
    // Zooms 1-3 only (tile IDs from 1) with no tiles at [21, 31): 74 positions, the last at tile ID 84.
    const build = {
      base: 1,
      contents: Array.from({ length: zoomEnd(3) - 1 }, (_, i) => (i >= 20 && i < 30 ? null : 1000 + i)),
    };
    const path = join(w.dir, '20261119.pmtiles');
    await writeArchiveFile(path, build, rng(7), { minZoom: 1, maxZoom: 3, metadata: METADATA });
    w.archives['20261119'] = { path, build };
    build.contents.forEach((content, i) => {
      if (content !== null) {
        w.tiles.seed(tilePath('v1', build.base + i), contentBytes(content), REGIONS);
      }
    });
    const seeds = (...ranges: Array<[number, number]>) =>
      Promise.all(ranges.map(([from, to]) => seedState({ dir: w.dir, zone: w.tiles.zone, archive: path, from, to })));
    const adopt = async (seedStates: string[]) => runSync(w.options('20261119', { adopt: true, seedStates }), w.deps());

    // [0, 74) is as many IDs as the build has positions, but not the IDs it has.
    const short = await adopt(await seeds([0, 74]));
    expect(short.status).toBe('failed');
    expect(short.error).toMatch(/cover tile IDs \[0, 74\), not tile 74 of the build/);
    const high = await adopt(await seeds([1, 21], [31, 80]));
    expect(high.status).toBe('failed');
    expect(high.error).toMatch(/cover tile IDs \[1, 21\), \[31, 80\), not tile 80 of the build/);
    expect(await readState(w.state)).toBeNull();
    expect(w.purges).toBe(0);

    // Seeds that start where the build does and leave out only IDs it has no tile at cover it.
    const report = await adopt(await seeds([1, 21], [31, 85]));
    expect(report.error).toBeNull();
    expect(report).toMatchObject({ status: 'success', positionsTotal: 74 });
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261119', positions: 74 });
  });

  it('refuses the seed of an archive sharing the header, root directory and size but not the tile bytes', async () => {
    const w = await world();
    seedZone(w, '20261117');
    const other = join(w.dir, 'other.pmtiles');
    await withOtherTileBytes(w.archives['20261117'].path, other);
    const seeded = await seedState({ dir: w.dir, zone: w.tiles.zone, archive: other });
    const report = await runSync(w.options('20261117', { adopt: true, seedStates: [seeded] }), w.deps());
    expect(report.status).toBe('failed');
    expect(report.error).toMatch(/it seeded another archive than/);
    expect(await readState(w.state)).toBeNull();
    expect(w.purges).toBe(0);
  });

  it("adopts a seed state with only a v1 fingerprint only given the archive's MD5", async () => {
    const w = await world();
    seedZone(w, '20261117');
    const archive = w.archives['20261117'].path;
    const md5 = createHash('md5')
      .update(await readFile(archive))
      .digest('base64');
    const legacy = await seedState({ dir: w.dir, zone: w.tiles.zone, archive, legacy: true });
    const adopt = (extra: Partial<SyncOptions>) =>
      runSync(w.options('20261117', { adopt: true, seedStates: [legacy], ...extra }), w.deps());

    const without = await adopt({});
    expect(without.status).toBe('failed');
    expect(without.error).toMatch(/only a v1 archive fingerprint .*--archive-md5/);
    const wrong = await adopt({ archiveMd5: createHash('md5').update('another build').digest('base64') });
    expect(wrong.status).toBe('failed');
    expect(wrong.error).toMatch(/has MD5 .*, not the .* given with --archive-md5/);
    // The archive's MD5 is no use for the seed of another build: its fingerprint must match this archive's.
    const otherBuild = await seedState({
      dir: w.dir,
      zone: w.tiles.zone,
      archive: w.archives['20261118'].path,
      legacy: true,
    });
    const another = await adopt({ archiveMd5: md5, seedStates: [otherBuild] });
    expect(another.status).toBe('failed');
    expect(another.error).toMatch(/it seeded another archive than/);
    expect(await readState(w.state)).toBeNull();
    expect(w.purges).toBe(0);

    const report = await adopt({ archiveMd5: md5 });
    expect(report.error).toBeNull();
    expect(report).toMatchObject({ status: 'success', mode: 'adopt' });
    expect((await readState(w.state))?.archive).toEqual(await archiveIdentity(archive));
    expect(w.purges).toBe(1);
  });

  it("refuses a v1 seed state whose archive changed after the seed began, though it now has the build's MD5", async () => {
    const w = await world();
    seedZone(w, '20261117');
    const real = w.archives['20261117'].path;
    const md5 = createHash('md5')
      .update(await readFile(real))
      .digest('base64');
    for (const [name, replace] of [
      ['rewritten', (path: string) => copyFile(real, path)],
      [
        'replaced',
        async (path: string) => {
          await copyFile(real, `${path}.new`);
          await rename(`${path}.new`, path);
        },
      ],
    ] as const) {
      // The seed read a damaged copy (the build's header, root directory and size, other tile bytes), which the
      // build itself took the place of once the seed had begun.
      const path = join(w.dir, `${name}.pmtiles`);
      await withOtherTileBytes(real, path);
      const legacy = await seedState({ dir: w.dir, zone: w.tiles.zone, archive: path, legacy: true });
      await sleep(50);
      await replace(path);
      const report = await runSync(
        w.options('20261117', { archive: path, adopt: true, seedStates: [legacy], archiveMd5: md5 }),
        w.deps(),
      );
      expect(report.status, name).toBe('failed');
      expect(report.error, name).toMatch(new RegExp(`${name}\\.pmtiles changed \\(.*\\) after the seed began`));
    }
    expect(await readState(w.state)).toBeNull();
    expect(w.purges).toBe(0);
  });

  it('refuses a v1 seed state whose seed read another file than the archive given, or names none it can check', async () => {
    const w = await world();
    seedZone(w, '20261117');
    const real = w.archives['20261117'].path;
    const md5 = createHash('md5')
      .update(await readFile(real))
      .digest('base64');
    const copy = join(w.dir, 'copy.pmtiles');
    await copyFile(real, copy);
    const cases: Array<[Parameters<typeof seedState>[0], RegExp]> = [
      // Even a copy with the same bytes: nothing says which file the seed read.
      [{ dir: w.dir, zone: w.tiles.zone, archive: copy }, /it read .*copy\.pmtiles, not .*20261117\.pmtiles/],
      [{ dir: w.dir, zone: w.tiles.zone, archive: real, statusArchive: '20261117.pmtiles' }, /not an absolute path/],
      [{ dir: w.dir, zone: w.tiles.zone, archive: real, statusArchive: null }, /archive the seed read as null/],
      [{ dir: w.dir, zone: w.tiles.zone, archive: join(w.dir, 'gone.pmtiles') }, /can't be checked \(ENOENT/],
      // seed.json may be newer than the seed (a copied state dir): its last run's start bounds it too.
      [
        { dir: w.dir, zone: w.tiles.zone, archive: real, startedAt: new Date(Date.now() - 3_600_000).toISOString() },
        /changed \(.*\) after the seed began/,
      ],
    ];
    for (const [options, error] of cases) {
      // seed.json holds the real archive's v1 fingerprint; status.json names the path under test.
      const statusArchive = options.statusArchive === undefined ? options.archive : options.statusArchive;
      const legacy = await seedState({ ...options, archive: real, statusArchive, legacy: true });
      const report = await runSync(
        w.options('20261117', { adopt: true, seedStates: [legacy], archiveMd5: md5 }),
        w.deps(),
      );
      expect(report.status, String(error)).toBe('failed');
      expect(report.error).toMatch(error);
    }
    expect(await readState(w.state)).toBeNull();
    expect(w.purges).toBe(0);
  });

  it('refuses to adopt a build the zone does not hold', async () => {
    const w = await world();
    seedZone(w, '20261118');
    const report = await runSync(w.options('20261117', { adopt: true }), w.deps());
    expect(report.status).toBe('failed');
    expect(report.error).toMatch(/sampled positions don't hold 20261117/);
    expect(await readState(w.state)).toBeNull();
    expect(w.purges).toBe(0);
  });

  it('uploads exactly the changed positions, then gates, purges once and switches the state', async () => {
    const w = await world();
    await adopted(w);
    const changed = changedPositions(w, '20261117', '20261118');
    expect(changed.length).toBeGreaterThan(20);

    const report = await runSync(w.options('20261118'), w.deps());

    expect(report.error).toBeNull();
    expect(report).toMatchObject({
      status: 'success',
      mode: 'diff',
      build: '20261118',
      previousBuild: '20261117',
      positionsTotal: POSITIONS,
      positionsChanged: changed.length,
      changedFraction: changed.length / POSITIONS,
      filesUploaded: changed.length,
      filesSkipped: 0,
      filesFailed: 0,
      filesDeleted: 0,
      positionsVerified: changed.length,
      positionsPending: 0,
      replicationComplete: true,
      replicationIncomplete: 0,
      purged: true,
      dryRun: false,
    });
    for (const field of CONTRACT_FIELDS) {
      expect(report, field).toHaveProperty(field);
    }
    expect(Object.keys(report.stepSeconds).sort()).toEqual(
      ['diff', 'identity', 'manifest', 'purge', 'replication', 'state', 'static', 'upload'].sort(),
    );
    // The live archive is recorded by its v2 identity, a hash of the whole file.
    expect((await readState(w.state))?.archive).toEqual(await archiveIdentity(w.archives['20261118'].path));
    expect(Object.keys(report.replicationWaitSeconds!).sort()).toEqual([...REGIONS].sort());
    expect(Object.values(report.replicationWaitSeconds!).every((value) => typeof value === 'number')).toBe(true);

    // Only the changed positions were written, once each, and never with bytes they already held.
    expect(
      tilePuts(w)
        .map(([path]) => path)
        .sort(),
    ).toEqual(changed.map((id) => tilePath('v1', id)).sort());
    expect(tilePuts(w).every(([, count]) => count === 1)).toBe(true);
    expect(w.tiles.identicalReputs).toEqual([]);
    expectHolds(w, '20261118');
    expect(report.bytesUploaded).toBe(
      changed.reduce((sum, id) => sum + contentBytes(w.archives['20261118'].build.contents[id]!).length, 0),
    );

    // The state switched last: after every tile PUT, the gate, the static files and the purge.
    expect(w.purges).toBe(1);
    const state = (await readState(w.state)) as SyncState;
    expect(state).toMatchObject({ liveBuild: '20261118', pending: [], manifest: 'manifests/20261118.tman.zst' });
    const lastStatePut = w.events.lastIndexOf('state PUT state.json');
    const purge = w.events.indexOf('purge');
    const lastTileEvent = lastIndex(w.events, (event) => event.startsWith('tiles '));
    expect(purge).toBeGreaterThan(lastTileEvent);
    expect(lastStatePut).toBeGreaterThan(purge);
    // The manifest went up before the pending record and the switch, and the pending record before any tile.
    const manifestPut = w.events.indexOf('state PUT manifests/20261118.tman.zst');
    expect(manifestPut).toBeGreaterThanOrEqual(0);
    expect(manifestPut).toBeLessThan(w.events.indexOf('state PUT state.json'));
    expect(w.events.indexOf('state PUT state.json')).toBeLessThan(
      w.events.findIndex((event) => event.startsWith('tiles PUT ') && event.endsWith('.mvt')),
    );
    expect(w.events.indexOf('state PUT state.json')).toBeLessThan(lastStatePut);
  });

  it('keeps the build pending when the purge fails, and the retry purges without writing again', async () => {
    const w = await world();
    await adopted(w);
    const changed = changedPositions(w, '20261117', '20261118');
    const failing = {
      ...w.deps(),
      purge: async () => {
        w.events.push('purge failed');
        throw new Error('purge: 500 from the API');
      },
    };
    const first = await runSync(w.options('20261118'), failing);
    expect(first).toMatchObject({ status: 'failed', filesUploaded: changed.length, purged: false });
    expect(first.error).toMatch(/purge: 500 from the API/);
    expect(w.events).toContain('purge failed');
    // The live build stays live, the new one pending: nothing said the PoPs dropped the old bytes.
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261117', pending: [{ build: '20261118' }] });

    const second = await runSync(w.options('20261118'), w.deps());
    expect(second.error).toBeNull();
    expect(second).toMatchObject({ status: 'success', filesUploaded: 0, filesSkipped: changed.length, purged: true });
    expect(w.purges).toBe(1);
    expect(tilePuts(w)).toHaveLength(changed.length);
    expect(tilePuts(w).every(([, count]) => count === 1)).toBe(true);
    expect(w.tiles.identicalReputs).toEqual([]);
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261118', pending: [] });
  });

  it('PUTs without DESCRIBEing first with plainPuts, where no pending build wrote', async () => {
    const w = await world();
    await adopted(w);
    const changed = changedPositions(w, '20261117', '20261118');
    const report = await runSync(w.options('20261118', { plainPuts: true }), w.deps());
    expect(report).toMatchObject({ status: 'success', positionsVerified: 0, filesUploaded: changed.length });
    const lastPut = lastIndex(w.events, (event) => event.startsWith('tiles PUT ') && event.endsWith('.mvt'));
    expect(w.events.slice(0, lastPut).filter((event) => event.startsWith('tiles DESCRIBE '))).toEqual([]);
    expect(w.tiles.identicalReputs).toEqual([]);
    expectHolds(w, '20261118');
  });

  it('does not re-PUT positions that already hold the new build, whatever the state says', async () => {
    const w = await world();
    // The zone holds 20261118 (a seed of it), but 20261117 was adopted.
    seedZone(w, '20261117');
    expect((await runSync(w.options('20261117', { adopt: true, adoptSamples: 1 }), w.deps())).status).toBe('success');
    seedZone(w, '20261118');
    w.tiles.putsByPath.clear();
    const report = await runSync(w.options('20261118'), w.deps());
    expect(report).toMatchObject({ status: 'success', filesUploaded: 0 });
    expect(report.filesSkipped).toBe(changedPositions(w, '20261117', '20261118').length);
    expect(tilePuts(w)).toEqual([]);
    expect(w.tiles.identicalReputs).toEqual([]);
  });

  it('skips a build that is live already without building its manifest', async () => {
    const w = await world();
    await adopted(w);
    const requests = { ...w.tiles.requests };
    const report = await runSync(w.options('20261117'), w.deps());
    expect(report).toMatchObject({
      status: 'skipped',
      previousBuild: '20261117',
      positionsTotal: POSITIONS,
      error: null,
    });
    expect(report.stepSeconds).toEqual({});
    expect(w.tiles.requests).toEqual(requests);
    expect(w.purges).toBe(0);
  });

  it('stops at the guard rail before writing anything', async () => {
    const w = await world();
    await adopted(w);
    const before = await w.state.get('state.json');
    const report = await runSync(w.options('20261118', { maxChangedFraction: 0.001 }), w.deps());

    expect(report.status).toBe('guard_rail');
    expect(report.filesUploaded).toBe(0);
    expect(report.positionsChanged).toBe(changedPositions(w, '20261117', '20261118').length);
    expect(report.error).toMatch(/guard rail/);
    expect(w.events).toEqual([]);
    expect(await w.state.get('state.json')).toEqual(before);
  });

  it('refuses without a state unless adopting or forced', async () => {
    const w = await world();
    const report = await runSync(w.options('20261118'), w.deps());
    expect(report.status).toBe('failed');
    expect(report.error).toMatch(/No state.json/);
    expect(w.events).toEqual([]);
  });

  it('plans a dry run without writing to either zone', async () => {
    const w = await world();
    await adopted(w);
    const report = await runSync(w.options('20261118', { dryRun: true }), { state: w.state });
    expect(report).toMatchObject({
      status: 'success',
      dryRun: true,
      positionsChanged: changedPositions(w, '20261117', '20261118').length,
      filesUploaded: null,
    });
    expect(w.events).toEqual([]);
  });

  it('resumes after a sync stopped part-way without re-PUTting what it stored', async () => {
    const controller = new AbortController();
    let puts = 0;
    const w = await world({
      latencyMs: () => 1 + Math.random() * 3,
      onRequest: ({ method, path }) => {
        if (method === 'PUT' && path.endsWith('.mvt') && ++puts === 15) {
          controller.abort('test');
        }
      },
    });
    await adopted(w);
    const changed = changedPositions(w, '20261117', '20261118');

    const first = await runSync(w.options('20261118', { signal: controller.signal, concurrency: 4 }), w.deps());
    expect(first.status).toBe('failed');
    expect(first.error).toMatch(/stopped/);
    expect(first.filesUploaded).toBeGreaterThan(0);
    expect(first.filesUploaded).toBeLessThan(changed.length);
    expect(w.purges).toBe(0);
    const pending = (await readState(w.state)) as SyncState;
    expect(pending.liveBuild).toBe('20261117');
    expect(pending.pending.map((p) => p.build)).toEqual(['20261118']);

    // With plainPuts too: the positions the pending build may have written are DESCRIBEd all the same.
    const second = await runSync(w.options('20261118', { plainPuts: true }), w.deps());
    expect(second.error).toBeNull();
    expect(second.status).toBe('success');
    expect(second.positionsVerified).toBe(changed.length);
    expect(second.positionsPending).toBe(changed.length);
    expect(second.filesSkipped).toBe(first.filesUploaded);
    expect(second.filesUploaded).toBe(changed.length - first.filesUploaded!);
    expect(w.tiles.identicalReputs).toEqual([]);
    expect(tilePuts(w).every(([, count]) => count === 1)).toBe(true);
    expectHolds(w, '20261118');
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261118', pending: [] });
    expect(w.purges).toBe(1);
  });

  it('fails on files that keep failing and repairs them on the next run', async () => {
    let broken = true;
    const w = await world({
      fault: ({ method, path }) =>
        broken && method === 'PUT' && path.endsWith('.mvt') && path.length % 3 === 0 ? 'server-error' : undefined,
    });
    await adopted(w);
    const changed = changedPositions(w, '20261117', '20261118');
    const first = await runSync(w.options('20261118'), w.deps());
    expect(first.status).toBe('failed');
    expect(first.filesFailed).toBeGreaterThan(0);
    expect(first.failures.length).toBeGreaterThan(0);
    expect(w.purges).toBe(0);

    broken = false;
    const second = await runSync(w.options('20261118'), w.deps());
    expect(second.status).toBe('success');
    expect(second.filesUploaded).toBe(first.filesFailed);
    expect(second.filesSkipped).toBe(changed.length - first.filesFailed!);
    expect(w.tiles.identicalReputs).toEqual([]);
    expectHolds(w, '20261118');
  });

  it('fails when replication times out, and the next run waits again without writing', async () => {
    const w = await world({ replicationDelayMs: { UK: 5, NY: 10 } });
    await adopted(w);
    const changed = changedPositions(w, '20261117', '20261118');
    // Each gate's timeout passes once one of its passes has had an answer for every file, each held by every region
    // but SYD (which never replicates on its own): what it reports is then what the zone holds, not what a pass cut
    // short by a slow runner left unchecked.
    const deadline = controlledTimeout();
    const timeOutWhenOnlySydLacks = onGatePass(
      (_, pending, lacking) => pending === changed.length && lacking === `SYD ${changed.length}` && deadline.expire(),
    );
    const timingOut = { replicationTimeoutMs: CONTROLLED_TIMEOUT_MS, log: timeOutWhenOnlySydLacks };
    const first = await runSync(w.options('20261118', timingOut), w.deps());

    expect(first.status).toBe('failed');
    expect(first.replicationComplete).toBe(false);
    expect(first.replicationIncomplete).toBe(changed.length);
    expect(first.replicationWaitSeconds).toMatchObject({ SYD: null });
    expect(typeof first.replicationWaitSeconds!.UK).toBe('number');
    expect(first.error).toMatch(/Replication of the tiles incomplete.*SYD/);
    // The files still lacking a region are named, so one that never replicates can be found.
    expect(first.replicationStragglers.length).toBe(Math.min(20, changed.length));
    expect(first.replicationStragglers[0]).toEqual({ path: expect.stringMatching(/^v1\/.*\.mvt$/), regions: ['SYD'] });
    expect(first.error).toContain(`e.g. ${first.replicationStragglers[0].path} (not in SYD)`);
    expect(w.purges).toBe(0);
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261117', pending: [{ build: '20261118' }] });

    // A retry while SYD still lacks them writes nothing, but gates the files it finds stored: no purge yet.
    const retry = await runSync(w.options('20261118', timingOut), w.deps());
    expect(retry).toMatchObject({ status: 'failed', filesUploaded: 0, filesSkipped: changed.length });
    expect(retry).toMatchObject({ replicationComplete: false, replicationIncomplete: changed.length, purged: false });
    expect(retry.replicationWaitSeconds).toMatchObject({ SYD: null });
    expect(retry.replicationStragglers[0]).toEqual({ path: expect.stringMatching(/\.mvt$/), regions: ['SYD'] });
    expect(retry.error).toMatch(/Replication of the tiles incomplete.*SYD/);
    expect(w.purges).toBe(0);
    expect(tilePuts(w)).toHaveLength(changed.length);

    w.tiles.replicate();
    const second = await runSync(w.options('20261118'), w.deps());
    expect(second).toMatchObject({ status: 'success', filesUploaded: 0, filesSkipped: changed.length });
    expect(second.replicationComplete).toBe(true);
    expect(w.tiles.identicalReputs).toEqual([]);
    expect(tilePuts(w)).toHaveLength(changed.length);
    expect(w.purges).toBe(1);
  });

  it('only checks storage when the gate has no regions', async () => {
    const w = await world({ replicationDelayMs: {} });
    await adopted(w);
    const report = await runSync(w.options('20261118', { regions: [], replicationTimeoutMs: 60_000 }), w.deps());
    expect(report).toMatchObject({ status: 'success', replicationComplete: true, replicationWaitSeconds: {} });
    expect(report.stepSeconds.replication).toBeLessThan(5);
    expect(w.purges).toBe(1);
  });

  it('still checks each file is stored with its bytes when the gate has no regions, and fails before the purge', async () => {
    const w = await world({ replicationDelayMs: {} });
    await adopted(w);
    const changed = changedPositions(w, '20261117', '20261118');
    // Between the uploads and the gate, one file the run wrote goes missing and another comes to hold other bytes.
    const gone = tilePath('v1', changed[0]);
    const other = tilePath('v1', changed[1]);
    let tampered = false;
    const onStep = (step: string) => {
      if (step === 'replication' && !tampered) {
        tampered = true;
        w.tiles.files.delete(gone);
        w.tiles.seed(other, new TextEncoder().encode('other bytes'));
      }
    };
    const report = await runSync(
      w.options('20261118', {
        regions: [],
        replicationTimeoutMs: 60_000,
        // The gate gives up on such files after this long rather than wait out its timeout.
        replicationMismatchGraceMs: 100,
        onStep,
      }),
      w.deps(),
    );
    expect(tampered).toBe(true);
    expect(report).toMatchObject({
      status: 'failed',
      filesUploaded: changed.length,
      replicationComplete: false,
      replicationIncomplete: 2,
      purged: false,
    });
    expect(report.error).toMatch(
      new RegExp(
        `^Replication of the tiles incomplete .*: 0 files not in every region \\(\\), 1 missing, 1 with other bytes, e\\.g\\. ${gone}$`,
      ),
    );
    expect(report.stepSeconds.replication).toBeLessThan(30);
    expect(w.purges).toBe(0);
    // Neither the TileJSON nor a style was written, and the build stays pending.
    expect(w.events.filter((event) => event.startsWith('tiles PUT ') && !event.endsWith('.mvt'))).toEqual([]);
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261117', pending: [{ build: '20261118' }] });
  });

  it('limits everything to --max-zoom', async () => {
    const w = await world();
    const end = zoomEnd(4);
    seedZone(w, '20261117', 'v1', end);
    const adopt = await runSync(w.options('20261117', { adopt: true, maxZoom: 4 }), w.deps());
    expect(adopt).toMatchObject({ status: 'success', positionsTotal: end, maxZoom: 4 });
    w.tiles.putsByPath.clear();

    const changed = changedPositions(w, '20261117', '20261118', end);
    const report = await runSync(w.options('20261118', { maxZoom: 4 }), w.deps());
    expect(report).toMatchObject({ status: 'success', positionsTotal: end, filesUploaded: changed.length });
    expect(tilePuts(w).every(([path]) => Number(path.split('/')[1]) <= 4)).toBe(true);
    expectHolds(w, '20261118', 'v1', end);
    const state = (await readState(w.state)) as SyncState;
    expect(state.positions).toBe(end);
    const tileJson = JSON.parse(new TextDecoder().decode(w.tiles.files.get('v1.json')!.bytes)) as { maxzoom: number };
    expect(tileJson.maxzoom).toBe(4);
  });

  it('writes every position with --force-full, DESCRIBEing each first, and gates those it finds stored too', async () => {
    const w = await world();
    // Half the zone already holds the build (a seed that stopped half-way), in no replica region yet.
    const { build } = w.archives['20261118'];
    for (let id = 0; id < POSITIONS; id += 2) {
      w.tiles.seed(tilePath('v1', id), contentBytes(build.contents[id]!));
    }
    const stored = Math.ceil(POSITIONS / 2);
    // Without a state all of it goes live at once: the files it found stored must replicate as well. What it writes
    // reaches every region within 20 ms, before the gate's second pass begins, so that pass leaves exactly the stored
    // files pending; only then do they replicate.
    const waited: string[] = [];
    const log = afterGatePass(2, (pending, lacking) => {
      waited.push(`${pending} pending (${lacking})`);
      w.tiles.replicate();
      w.events.push('replicated');
    });
    const report = await runSync(
      w.options('20261118', { forceFull: true, replicationTimeoutMs: GATE_TIMEOUT_MS, log }),
      w.deps(),
    );
    expect(report.error).toBeNull();
    expect(report).toMatchObject({
      status: 'success',
      mode: 'force-full',
      positionsVerified: POSITIONS,
      filesSkipped: stored,
      filesUploaded: POSITIONS - stored,
      filesDeleted: 0,
      positionsChanged: null,
      replicationComplete: true,
      replicationIncomplete: 0,
      purged: true,
    });
    expect(waited).toEqual([`${stored} pending (UK ${stored}, NY ${stored}, SYD ${stored})`]);
    expect(w.events.indexOf('purge')).toBeGreaterThan(w.events.indexOf('replicated'));
    expect(w.tiles.identicalReputs).toEqual([]);
    expectHolds(w, '20261118');
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261118', pending: [] });
  });

  it('gates the files a --force-full retry finds stored, which the failed attempt may have written', async () => {
    const w = await world({ replicationDelayMs: { UK: 5, NY: 10 } });
    await adopted(w);
    w.tiles.replicate();
    const changed = changedPositions(w, '20261117', '20261118');
    // The first attempt gates only what it writes (the rest of the zone is the live build's), and SYD never has it:
    // every file of its gate lacks SYD, whether or not a pass reached it before the timeout.
    const first = await runSync(w.options('20261118', { forceFull: true, replicationTimeoutMs: 300 }), w.deps());
    expect(first).toMatchObject({ status: 'failed', filesUploaded: changed.length, replicationComplete: false });
    expect(first.replicationIncomplete).toBe(changed.length);
    expect(first.error).toMatch(/Replication of the tiles incomplete.*SYD/);
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261117', pending: [{ build: '20261118' }] });

    // The retry finds those files stored and writes nothing, but gates every position: its first pass leaves exactly
    // the files the attempt wrote pending, for SYD only, and it purges only once SYD has them.
    const waited: string[] = [];
    const log = afterGatePass(1, (pending, lacking) => {
      waited.push(`${pending} pending (${lacking})`);
      w.tiles.replicate();
      w.events.push('replicated');
    });
    const retry = await runSync(
      w.options('20261118', { forceFull: true, replicationTimeoutMs: GATE_TIMEOUT_MS, log }),
      w.deps(),
    );
    expect(retry.error).toBeNull();
    expect(retry).toMatchObject({
      status: 'success',
      filesUploaded: 0,
      filesSkipped: POSITIONS,
      positionsVerified: POSITIONS,
      replicationComplete: true,
      purged: true,
    });
    expect(waited).toEqual([`${changed.length} pending (SYD ${changed.length})`]);
    expect(w.events.indexOf('purge')).toBeGreaterThan(w.events.indexOf('replicated'));
    expect(w.purges).toBe(1);
    expect(tilePuts(w)).toHaveLength(changed.length);
    expect(w.tiles.identicalReputs).toEqual([]);
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261118', pending: [] });
  });

  it('repairs the live build with --force-full under its key, writing only the files that lost its bytes', async () => {
    const w = await world();
    await adopted(w);
    const damaged = tilePath('v1', 3);
    const lost = tilePath('v1', 7);
    w.tiles.seed(damaged, contentBytes(424_242), REGIONS);
    w.tiles.files.delete(lost);
    const report = await runSync(
      w.options('20261117', { forceFull: true, replicationTimeoutMs: GATE_TIMEOUT_MS }),
      w.deps(),
    );
    expect(report.error).toBeNull();
    expect(report).toMatchObject({
      status: 'success',
      previousBuild: '20261117',
      positionsChanged: 0,
      positionsVerified: POSITIONS,
      filesUploaded: 2,
      filesSkipped: POSITIONS - 2,
      filesDeleted: 0,
      purged: true,
    });
    expect(
      tilePuts(w)
        .map(([path]) => path)
        .sort(),
    ).toEqual([damaged, lost].sort());
    expectHolds(w, '20261117');
    expect(w.tiles.identicalReputs).toEqual([]);
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261117', pending: [] });
  });

  it('refuses a sync under the live build key with other tiles than its manifest, before writing anything', async () => {
    const w = await world();
    await adopted(w);
    const changed = changedPositions(w, '20261117', '20261118');
    const above = POSITIONS - zoomEnd(4);
    const refused = async (extra: Partial<SyncOptions>, deps = w.deps()) => {
      const before = await w.state.get('state.json');
      w.events.length = 0;
      const report = await runSync(w.options('20261117', extra), deps);
      expect(report.status).toBe('failed');
      expect(report.error).toMatch(/^20261117 is the live build, and .* has other tiles than its manifest at /);
      expect(report.error).toMatch(/a sync under the live key only puts back the live build's own bytes/);
      // Not a request that writes, nor a DESCRIBE: the plan refused it.
      expect(w.events).toEqual([]);
      expect(await w.state.get('state.json')).toEqual(before);
      return report.error!;
    };
    // A --force-full "repair" with another build's tiles, or cut at another zoom: it would record no pending build,
    // and publish a manifest with those tiles as the live one.
    const archive = w.archives['20261118'].path;
    expect(await refused({ forceFull: true, archive })).toContain(
      `at ${changed.length} positions from zoom ${tileIdZoom(changed[0])} (${changed.length} changed, 0 added, 0 removed)`,
    );
    expect(await refused({ forceFull: true, maxZoom: 4 })).toContain(
      `at ${above} positions from zoom 5 (0 changed, 0 added, ${above} removed, with --max-zoom 4)`,
    );
    expect(await refused({ forceFull: true, archive, dryRun: true }, { state: w.state })).toContain('other tiles');
    expectHolds(w, '20261117');

    // The revert of a pending build under the live key needs the live tiles too.
    const failing = { ...w.deps(), purge: () => Promise.reject(new Error('purge: 500 from the API')) };
    expect((await runSync(w.options('20261118'), failing)).status).toBe('failed');
    expect(await refused({ archive })).toContain(`(${changed.length} changed, 0 added, 0 removed)`);
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261117', pending: [{ build: '20261118' }] });
    expect(w.purges).toBe(0);
    // With the live archive the revert goes ahead, and puts the live bytes back.
    expect(await runSync(w.options('20261117'), w.deps())).toMatchObject({ status: 'success', error: null });
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261117', pending: [] });
    expectHolds(w, '20261117');
  });

  it('deletes with --force-full what the live or a pending build has and the new one does not', async () => {
    const w = await world();
    const x = w.archives['20261117'].build;
    const y = w.archives['20261118'].build;
    // Positions only a pending build (20261117) has, and positions only the live one (20261116) has.
    const pendingOnly = [3, 40, 41, 42, 1500, 5000];
    const liveOnly = [5, 90, 91, 2000, POSITIONS - 1];
    const without = (build: Build, ids: number[]): Build => ({
      base: build.base,
      contents: build.contents.map((content, id) => (ids.includes(id) ? null : content)),
    });
    const r = rng(77);
    for (const [key, build] of [
      ['20261116', without(x, pendingOnly)],
      ['20261119', without(y, [...pendingOnly, ...liveOnly])],
    ] as const) {
      const path = join(w.dir, `${key}.pmtiles`);
      await writeArchiveFile(path, build, r, { maxZoom: 6, metadata: METADATA, maxLeafEntries: 40 });
      w.archives[key] = { path, build };
      w.seedStates[key] = await seedState({ dir: w.dir, zone: w.tiles.zone, archive: path });
    }
    await adopted(w, '20261116');
    // A sync to 20261117 writes the positions only it has, then fails at the purge: they stay pending.
    const failing = { ...w.deps(), purge: () => Promise.reject(new Error('purge: 500 from the API')) };
    expect(await runSync(w.options('20261117'), failing)).toMatchObject({
      status: 'failed',
      filesUploaded: pendingOnly.length,
    });
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261116', pending: [{ build: '20261117' }] });
    const obsolete = [...pendingOnly, ...liveOnly].map((id) => tilePath('v1', id));
    expect(obsolete.filter((path) => !w.tiles.files.has(path))).toEqual([]);

    const report = await runSync(
      w.options('20261119', { forceFull: true, replicationTimeoutMs: GATE_TIMEOUT_MS }),
      w.deps(),
    );
    expect(report.error).toBeNull();
    expect(report).toMatchObject({
      status: 'success',
      positionsTotal: POSITIONS - obsolete.length,
      positionsChanged: changedPositions(w, '20261116', '20261119').length,
      positionsVerified: POSITIONS - obsolete.length,
      filesDeleted: obsolete.length,
      purged: true,
    });
    // Neither those only the pending build wrote nor those only the live one has are left.
    expect(pendingOnly.filter((id) => w.tiles.files.has(tilePath('v1', id)))).toEqual([]);
    expect(liveOnly.filter((id) => w.tiles.files.has(tilePath('v1', id)))).toEqual([]);
    expectHolds(w, '20261119');
    expect(w.tiles.identicalReputs).toEqual([]);
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261119', pending: [] });
  });

  it('stops --force-full at the guard rail on what it deletes, before writing anything', async () => {
    const w = await world();
    await adopted(w);
    const before = await w.state.get('state.json');
    // The CLI's default guard rail (the world's options raise it to 1).
    const refused = async (extra: Partial<SyncOptions>, removed: number) => {
      w.events.length = 0;
      const report = await runSync(
        w.options('20261118', {
          forceFull: true,
          maxChangedFraction: SYNC_DEFAULTS.maxChangedFraction,
          replicationTimeoutMs: GATE_TIMEOUT_MS,
          ...extra,
        }),
        w.deps(),
      );
      expect(report).toMatchObject({ status: 'guard_rail', filesUploaded: 0, filesDeleted: null, purged: false });
      expect(report.error).toContain(
        `--force-full would delete ${removed} of the live build's ${POSITIONS} positions (${removed / POSITIONS}), ` +
          'over the guard rail of 0.05: nothing uploaded',
      );
      // Not a DESCRIBE, PUT or DELETE in either zone, no purge, and the state as it was.
      expect(w.events).toEqual([]);
      expect(w.purges).toBe(0);
      expect(await w.state.get('state.json')).toEqual(before);
      expectHolds(w, '20261117');
      return report.error!;
    };
    // --max-zoom under the live build's top zoom: every position of the live build above it would go, and the cut
    // manifest would become the live one.
    const above = POSITIONS - zoomEnd(3);
    expect(await refused({ maxZoom: 3 }, above)).toMatch(
      new RegExp(
        `\\. ${above} of them are above --max-zoom 3, which would also make a manifest cut there the live one$`,
      ),
    );
    // Another, smaller archive given by mistake (the new build without zoom 6): the same, without --max-zoom.
    const z6 = POSITIONS - zoomEnd(5);
    const y = w.archives['20261118'].build;
    const smaller = join(w.dir, 'smaller.pmtiles');
    await writeArchiveFile(
      smaller,
      { base: y.base, contents: y.contents.map((content, id) => (id < zoomEnd(5) ? content : null)) },
      rng(78),
      { maxZoom: 6, metadata: METADATA, maxLeafEntries: 40 },
    );
    expect(await refused({ archive: smaller }, z6)).toMatch(/nothing uploaded$/);
    // A dry run stops there too.
    expect(await refused({ maxZoom: 3, dryRun: true }, above)).toContain('above --max-zoom 3');

    // Raised on purpose, the guard rail lets it delete them and make the cut build live.
    const report = await runSync(
      w.options('20261118', {
        forceFull: true,
        maxZoom: 3,
        maxChangedFraction: 1,
        replicationTimeoutMs: GATE_TIMEOUT_MS,
      }),
      w.deps(),
    );
    expect(report.error).toBeNull();
    expect(report).toMatchObject({ status: 'success', filesDeleted: above, positionsTotal: zoomEnd(3) });
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261118', positions: zoomEnd(3), pending: [] });
  });

  it('keeps --force-full to the live prefix', async () => {
    const w = await world();
    await adopted(w);
    const before = await w.state.get('state.json');
    const report = await runSync(w.options('20261118', { forceFull: true, prefix: 'v2' }), w.deps());
    expect(report.status).toBe('failed');
    expect(report.error).toMatch(/The live build is under v1\/, not v2\/: .*--adopt --replace/);
    expect(w.events).toEqual([]);
    expect(await w.state.get('state.json')).toEqual(before);
    expect([...w.tiles.files.keys()].filter((path) => path.startsWith('v2/'))).toEqual([]);
  });

  it('refuses another archive under a pending build key until the live build has put its bytes back', async () => {
    const delays: Record<string, number> = { UK: 5, NY: 10 };
    const w = await world({ replicationDelayMs: delays });
    await adopted(w);
    const x = w.archives['20261117'].build;
    const y = w.archives['20261118'].build;
    const changed = changedPositions(w, '20261117', '20261118');
    // The first attempt writes the changed tiles, then times out in the gate.
    expect((await runSync(w.options('20261118', { replicationTimeoutMs: 200 }), w.deps())).status).toBe('failed');
    const attempted = w.stateZone.checksum('manifests/20261118.tman.zst');
    expect(attempted).toBeDefined();
    // The build comes out again with some of those tiles back at the live bytes: a plan from it alone would never
    // visit them, and they would keep the first attempt's bytes.
    const restored = changed.filter((_, i) => i % 3 === 0);
    const again = {
      base: y.base,
      contents: y.contents.map((content, id) => (restored.includes(id) ? x.contents[id] : content)),
    };
    const path = join(w.dir, '20261118-again.pmtiles');
    await writeArchiveFile(path, again, rng(9), { maxZoom: 6, metadata: METADATA, maxLeafEntries: 40 });
    w.archives.again = { path, build: again };
    w.tiles.replicate();
    delays.SYD = 20;
    w.events.length = 0;

    const refused = await runSync(w.options('20261118', { archive: path }), w.deps());
    expect(refused.status).toBe('failed');
    expect(refused.error).toMatch(
      new RegExp(
        `^20261118 is pending from an attempt with other tiles than .* \\(tile ${restored[0]} holds other bytes ` +
          'in this archive\\).* Sync the live build 20261117 first',
      ),
    );
    expect(w.events).toEqual([]);
    expect(w.stateZone.checksum('manifests/20261118.tman.zst')).toBe(attempted);
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261117', pending: [{ build: '20261118' }] });

    // Syncing the live build undoes the attempt; the new archive then syncs like any other.
    expect(await runSync(w.options('20261117'), w.deps())).toMatchObject({ status: 'success', error: null });
    expectHolds(w, '20261117');
    const report = await runSync(w.options('20261118', { archive: path }), w.deps());
    expect(report.error).toBeNull();
    expect(report.status).toBe('success');
    expectHolds(w, 'again');
    expect(w.tiles.identicalReputs).toEqual([]);
  });

  it.each(['manifest', 'diff', 'identity'] as const)(
    'refuses a file put at the archive path at the %s step: its manifest, identity and uploads are of one file',
    async (at) => {
      const w = await world();
      await adopted(w);
      const before = await readState(w.state);
      const archive = w.archives['20261118'].path;
      // The same size, header and root directory, one tile byte flipped: only the whole-file hash tells it apart.
      const other = join(w.dir, 'other.pmtiles');
      await withOtherTileBytes(archive, other);
      const report = await runSync(
        w.options('20261118', { onStep: (step) => step === at && renameSync(other, archive) }),
        w.deps(),
      );
      expect(report.status).toBe('failed');
      expect(report.error).toMatch(/replaced or written to/);
      expect(tilePuts(w)).toEqual([]);
      expect(w.events).toEqual([]);
      expect(await readState(w.state)).toEqual(before);
      expect(w.purges).toBe(0);
    },
  );

  it('neither purges nor makes a build live when its archive is written to while it uploads', async () => {
    const w = await world();
    await adopted(w);
    const archive = w.archives['20261118'].path;
    let written = false;
    const report = await runSync(
      w.options('20261118', {
        // The tile gate, right after the uploads.
        onStep: (step) =>
          step === 'replication' && !written && ((written = true), utimesSync(archive, new Date(), new Date())),
      }),
      w.deps(),
    );
    expect(report.status).toBe('failed');
    expect(report.error).toMatch(/was written to while it was read/);
    expect(w.purges).toBe(0);
    // No TileJSON or style written: the state's PUT is the pending record, from before the uploads.
    expect(w.events.filter((event) => event.startsWith('tiles PUT') && event.endsWith('.json'))).toEqual([]);
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261117', pending: [{ build: '20261118' }] });
  });

  it('reverts a failed sync when the live build is synced again', async () => {
    const delays: Record<string, number> = { UK: 5, NY: 10 };
    const w = await world({ replicationDelayMs: delays });
    await adopted(w);
    const changed = changedPositions(w, '20261117', '20261118');
    const failed = await runSync(w.options('20261118', { replicationTimeoutMs: 200 }), w.deps());
    expect(failed.status).toBe('failed');
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261117', pending: [{ build: '20261118' }] });
    expectHolds(w, '20261118');
    w.tiles.replicate();
    delays.SYD = 20;
    w.tiles.putsByPath.clear();

    const revert = await runSync(w.options('20261117'), w.deps());
    expect(revert.error).toBeNull();
    expect(revert).toMatchObject({
      status: 'success',
      previousBuild: '20261117',
      pendingBuilds: ['20261118'],
      positionsChanged: 0,
      positionsPending: changed.length,
      filesUploaded: changed.length,
      replicationComplete: true,
      purged: true,
    });
    expectHolds(w, '20261117');
    expect(tilePuts(w)).toHaveLength(changed.length);
    expect(w.tiles.identicalReputs).toEqual([]);
    expect(w.purges).toBe(1);
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261117', pending: [] });
    // Only now is the live build skipped.
    expect((await runSync(w.options('20261117'), w.deps())).status).toBe('skipped');
  });

  it('gates changed static files before the purge', async () => {
    const w = await world();
    await adopted(w);
    const report = await runSync(w.options('20261118', { publicUrl: 'https://tiles-next.example' }), w.deps());
    expect(report).toMatchObject({ status: 'success', staticUploaded: 3 });
    const put = w.events.indexOf('tiles PUT v1.json');
    const purge = w.events.indexOf('purge');
    expect(put).toBeGreaterThan(
      lastIndex(w.events, (event) => event.startsWith('tiles PUT ') && event.endsWith('.mvt')),
    );
    expect(w.events.indexOf('tiles DESCRIBE v1.json', put + 1)).toBeGreaterThan(put);
    expect(w.events.indexOf('tiles DESCRIBE v1.json', put + 1)).toBeLessThan(purge);
  });

  it('fails before the purge when changed static files do not replicate', async () => {
    const w = await world({ replicationDelayMs: { UK: 5, NY: 10 } });
    await adopted(w);
    // A first run leaves the changed tiles stored, and they replicate after it timed out.
    expect((await runSync(w.options('20261118', { replicationTimeoutMs: 200 }), w.deps())).status).toBe('failed');
    w.tiles.replicate();
    w.events.length = 0;

    // Its tile gate must pass, first pass and all, before the timeout that ends both gates (see STATIC_TIMEOUT_MS).
    const report = await runSync(
      w.options('20261118', { publicUrl: 'https://tiles-next.example', replicationTimeoutMs: STATIC_TIMEOUT_MS }),
      w.deps(),
    );
    expect(report).toMatchObject({ status: 'failed', filesUploaded: 0, staticUploaded: 3, replicationComplete: false });
    expect(report.error).toMatch(/Replication of the TileJSON and styles incomplete.*e\.g\. v1\.json \(not in SYD\)/);
    // The tiles reached every region, the TileJSON and styles never SYD: its wait says so.
    expect(report.replicationWaitSeconds).toMatchObject({ SYD: null });
    expect(typeof report.replicationWaitSeconds!.UK).toBe('number');
    expect(w.purges).toBe(0);
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261117', pending: [{ build: '20261118' }] });
  });

  it("adds up each region's waits in the tile and static gates", async () => {
    const w = await world({ replicationDelayMs: { UK: 5, NY: 10, SYD: 300 } });
    await adopted(w);
    const lines: string[] = [];
    const report = await runSync(
      w.options('20261118', { publicUrl: 'https://tiles-next.example', log: (line) => lines.push(line) }),
      w.deps(),
    );
    expect(report).toMatchObject({ status: 'success', staticUploaded: 3 });
    // What each gate saw: when its passes first found every file in each region.
    const gates = gateWaits(lines);
    expect(gates).toHaveLength(2);
    const [tileGate, staticGate] = gates;
    for (const region of REGIONS) {
      expect(tileGate[region], region).toBeGreaterThanOrEqual(0);
      expect(staticGate[region], region).toBeGreaterThanOrEqual(0);
      // The log gives each to 0.1 s and the report rounds their sum to 0.1 s: they agree to within those roundings.
      const sum = tileGate[region] + staticGate[region];
      expect(Math.abs(report.replicationWaitSeconds![region]! - sum), `${region}: ${sum}`).toBeLessThanOrEqual(0.16);
    }
  });

  it("stops before writing a static file when asked to during the file's DESCRIBE", async () => {
    const controller = new AbortController();
    let armed = false;
    const w = await world({
      onRequest: ({ method, path }) => {
        if (armed && method === 'DESCRIBE' && !path.endsWith('.mvt')) {
          armed = false;
          controller.abort('test');
        }
      },
    });
    await adopted(w);
    armed = true;
    const report = await runSync(
      w.options('20261118', { publicUrl: 'https://tiles-next.example', signal: controller.signal }),
      w.deps(),
    );
    expect(report).toMatchObject({ status: 'failed', staticUploaded: 0, purged: false });
    expect(report.error).toMatch(/stopped \(test\)/);
    expect(armed).toBe(false);
    expect(w.events.filter((event) => event.startsWith('tiles PUT ') && !event.endsWith('.mvt'))).toEqual([]);
    expect(w.purges).toBe(0);
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261117', pending: [{ build: '20261118' }] });
  });

  it('keeps its manifests in a directory of its own in the work dir, leaving what else is there', async () => {
    const w = await world();
    await adopted(w);
    const work = join(w.dir, 'work');
    await mkdir(work);
    const others = ['20261118.tman.zst', '20261118.z6.tman.zst', 'live-20261117.tman.zst'];
    for (const name of others) {
      await writeFile(join(work, name), name);
    }
    const garbage = join(w.dir, 'garbage.pmtiles');
    await writeFile(garbage, 'not an archive');
    const dryRun = (extra: Partial<SyncOptions>) =>
      runSync(w.options('20261118', { dryRun: true, workDir: work, ...extra }), { state: w.state });

    // One that fails building its manifest, and one that builds, clips and downloads them.
    expect((await dryRun({ archive: garbage })).status).toBe('failed');
    expect(await dryRun({ maxZoom: 6 })).toMatchObject({ status: 'success', error: null });
    expect((await readdir(work)).sort()).toEqual(others);
    for (const name of others) {
      expect(await readFile(join(work, name), 'utf8')).toBe(name);
    }
  });

  it('gates every static file on a retry, also those an earlier attempt wrote', async () => {
    const w = await world({ replicationDelayMs: { UK: 5, NY: 10 } });
    await adopted(w);
    w.tiles.replicate();
    // A first run stores the changed tiles, which replicate after it timed out.
    expect((await runSync(w.options('20261118', { replicationTimeoutMs: 200 }), w.deps())).status).toBe('failed');
    w.tiles.replicate();
    // Each run's tile gate must pass within the timeout, first pass and all (see STATIC_TIMEOUT_MS).
    const next = { publicUrl: 'https://tiles-next.example', replicationTimeoutMs: STATIC_TIMEOUT_MS };
    // The second writes the new TileJSON and styles, which never reach SYD before its gate gives up.
    const second = await runSync(w.options('20261118', next), w.deps());
    expect(second).toMatchObject({ status: 'failed', staticUploaded: 3, replicationComplete: false });
    expect(w.tiles.checksum('v1.json')).toBeDefined();
    w.events.length = 0;

    // The retry finds their bytes stored and writes nothing. SYD now holds the TileJSON and one style, but still lacks
    // the other style: no purge.
    w.tiles.replicate('v1.json');
    w.tiles.replicate('v1/style/light.json');
    const retry = await runSync(w.options('20261118', next), w.deps());
    expect(retry).toMatchObject({ status: 'failed', filesUploaded: 0, staticUploaded: 0, replicationComplete: false });
    expect(retry.error).toMatch(
      /Replication of the TileJSON and styles incomplete.*: 1 files not in every region .*e\.g\. v1\/style\/light \(not in SYD\)$/,
    );
    expect(retry.replicationStragglers).toEqual([{ path: 'v1/style/light', regions: ['SYD'] }]);
    expect(w.events.filter((event) => event.startsWith('tiles PUT'))).toEqual([]);
    for (const path of ['v1.json', 'v1/style/light.json', 'v1/style/light']) {
      expect(w.events).toContain(`tiles DESCRIBE ${path}`);
    }
    expect(w.purges).toBe(0);
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261117', pending: [{ build: '20261118' }] });

    w.tiles.replicate('v1/style/light');
    const done = await runSync(w.options('20261118', next), w.deps());
    expect(done).toMatchObject({ status: 'success', staticUploaded: 0, replicationComplete: true });
    expect(w.purges).toBe(1);
    expect(w.tiles.identicalReputs).toEqual([]);
  });

  it('switches to a build seeded under another prefix with --adopt --replace, and syncs there after', async () => {
    const w = await world();
    await adopted(w);
    seedZone(w, '20261118', 'v1/e2');
    const seeded = await seedState({
      dir: w.dir,
      zone: w.tiles.zone,
      archive: w.archives['20261118'].path,
      prefix: 'v1/e2',
    });
    w.tiles.putsByPath.clear();
    const report = await runSync(
      w.options('20261118', { adopt: true, replace: true, prefix: 'v1/e2', seedStates: [seeded] }),
      w.deps(),
    );
    expect(report.error).toBeNull();
    expect(report).toMatchObject({ status: 'success', mode: 'replace', prefix: 'v1/e2', previousBuild: '20261117' });
    expect(tilePuts(w)).toEqual([]);
    expect(w.purges).toBe(1);
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261118', prefix: 'v1/e2', pending: [] });
    // The TileJSON and styles stay where Immich loads them; only the tile template moves.
    const tileJson = JSON.parse(new TextDecoder().decode(w.tiles.files.get('v1.json')!.bytes)) as { tiles: string[] };
    expect(tileJson.tiles).toEqual(['https://tiles.example/v1/e2/{z}/{x}/{y}.mvt']);
    expect(w.tiles.files.has('v1/e2.json')).toBe(false);
    const put = w.events.indexOf('tiles PUT v1.json');
    const gated = w.events.indexOf('tiles DESCRIBE v1.json', put + 1);
    expect(put).toBeGreaterThanOrEqual(0);
    expect(gated).toBeGreaterThan(put);
    expect(gated).toBeLessThan(w.events.indexOf('purge'));

    // A daily sync follows the live prefix without being told.
    const back = await runSync(w.options('20261117'), w.deps());
    expect(back).toMatchObject({ status: 'success', prefix: 'v1/e2' });
    expectHolds(w, '20261117', 'v1/e2');
  });

  it("keeps the live and pending manifests whole when a switch under one's key fails to write the state", async () => {
    const delays: Record<string, number> = { UK: 5, NY: 10 };
    const w = await world({ replicationDelayMs: delays });
    await adopted(w);
    // A daily sync of 20261118 that timed out in its gate: pending, with its manifest.
    expect((await runSync(w.options('20261118', { replicationTimeoutMs: 200 }), w.deps())).status).toBe('failed');
    w.tiles.replicate();
    delays.SYD = 20;
    const before = (await readState(w.state)) as SyncState;
    expect(before).toMatchObject({ liveBuild: '20261117', prefix: 'v1', pending: [{ build: '20261118' }] });
    const named = [before.manifest, before.pending[0].manifest];
    const stored = named.map((name) => w.stateZone.checksum(name));
    const entries = {
      '20261117': expectedEntries(w.archives['20261117'].build),
      '20261118': expectedEntries(w.archives['20261118'].build),
    };
    expect(await storedEntries(w, before)).toEqual(entries['20261117']);
    expect(await storedEntries(w, before.pending[0])).toEqual(entries['20261118']);

    // Full rewrites, each under a prefix of its own and the live or the pending build's key, with the other build's
    // tiles: the purge goes through, the state's write doesn't.
    for (const [key, tiles] of [
      ['20261117', '20261118'],
      ['20261118', '20261117'],
    ] as const) {
      const prefix = `v1/e2-${key}`;
      seedZone(w, tiles, prefix);
      const archive = w.archives[tiles].path;
      const seeded = await seedState({ dir: w.dir, zone: w.tiles.zone, archive, prefix });
      const report = await runSync(
        w.options(key, { archive, adopt: true, replace: true, prefix, seedStates: [seeded] }),
        { ...w.deps(), state: failingStateWrites(w.state) },
      );
      expect(report).toMatchObject({ status: 'failed', mode: 'replace', purged: true });
      expect(report.error).toMatch(/PUT state\.json: 503/);
      // The state, and the manifests it names, are as they were: plans against v1 still read what v1 holds.
      expect(await readState(w.state)).toEqual(before);
      expect(named.map((name) => w.stateZone.checksum(name))).toEqual(stored);
      expect(await storedEntries(w, before)).toEqual(entries['20261117']);
      expect(await storedEntries(w, before.pending[0])).toEqual(entries['20261118']);
    }

    // Retried with the state zone back, the switch makes the build live with its own manifest.
    const archive = w.archives['20261118'].path;
    const prefix = 'v1/e2-20261117';
    const seeded = await seedState({ dir: w.dir, zone: w.tiles.zone, archive, prefix });
    const report = await runSync(
      w.options('20261117', { archive, adopt: true, replace: true, prefix, seedStates: [seeded] }),
      w.deps(),
    );
    expect(report.error).toBeNull();
    const after = (await readState(w.state)) as SyncState;
    expect(after).toMatchObject({ liveBuild: '20261117', prefix, pending: [] });
    expect(after.manifest).toMatch(/^manifests\/20261117-[0-9A-F]{16}\.tman\.zst$/);
    expect(await storedEntries(w, after)).toEqual(entries['20261118']);
  });

  it('refuses --replace onto an overlapping prefix, or before the new prefix has replicated', async () => {
    const w = await world();
    await adopted(w);
    const archive = w.archives['20261118'].path;
    for (const prefix of ['v1', 'v1/3']) {
      const seeded = await seedState({ dir: w.dir, zone: w.tiles.zone, archive, prefix });
      const report = await runSync(
        w.options('20261118', { adopt: true, replace: true, prefix, seedStates: [seeded] }),
        w.deps(),
      );
      expect(report.error).toMatch(/a prefix apart from the live v1\//);
    }
    seedZone(w, '20261118', 'v2', POSITIONS, ['UK', 'NY']);
    const seeded = await seedState({ dir: w.dir, zone: w.tiles.zone, archive, prefix: 'v2' });
    const report = await runSync(
      w.options('20261118', { adopt: true, replace: true, prefix: 'v2', seedStates: [seeded] }),
      w.deps(),
    );
    expect(report.status).toBe('failed');
    expect(report.error).toMatch(/200 of 200 sampled positions under v2\/ are not in every region yet \(SYD 200\)/);
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261117', prefix: 'v1' });
    expect(w.purges).toBe(0);

    // Only the regions given, each once and as the zone names it, however they are spelled.
    const some = await runSync(
      w.options('20261118', {
        adopt: true,
        replace: true,
        prefix: 'v2',
        seedStates: [seeded],
        regions: [' uk', 'UK', 'ny', ''],
      }),
      w.deps(),
    );
    expect(some.error).toBeNull();
    expect(some).toMatchObject({ status: 'success', mode: 'replace', prefix: 'v2' });
    expect(Object.keys(some.replicationWaitSeconds ?? {}).sort()).toEqual(['NY', 'UK']);
  });

  it('deletes manifests no state names once the build is live', async () => {
    const w = await world();
    await adopted(w);
    const report = await runSync(w.options('20261118', { keepManifests: 0 }), w.deps());
    expect(report).toMatchObject({ status: 'success', manifestsDeleted: 1 });
    expect(w.stateZone.files.has('manifests/20261117.tman.zst')).toBe(false);
    expect(w.stateZone.files.has('manifests/20261118.tman.zst')).toBe(true);
  });

  it('does not rewrite static files whose bytes did not change', async () => {
    const w = await world();
    await adopted(w);
    const report = await runSync(w.options('20261118'), w.deps());
    expect(report.staticUploaded).toBe(0);
    expect(w.tiles.identicalReputs).toEqual([]);
  });
});

describe('planItems', () => {
  it('yields the archive tile of every changed position, reading repeats once', async () => {
    const w = await world();
    const ranges = await ArchiveRanges.open(w.archives['20261118'].path);
    cleanup.push(() => ranges.close());
    let reads = 0;
    const read = ranges.archive.readTileData.bind(ranges.archive);
    ranges.archive.readTileData = (offset, length) => {
      reads++;
      return read(offset, length);
    };
    const plan = [
      { start: 0, count: 100, kind: 'changed' as const, verify: false },
      { start: 100, count: 5, kind: 'removed' as const, verify: false },
      { start: 200, count: 1000, kind: 'added' as const, verify: true },
    ];
    const items = [];
    for await (const item of planItems(ranges, plan)) {
      items.push(item);
    }
    const { contents } = w.archives['20261118'].build;
    expect(items.map((item) => item.tileId)).toEqual([
      ...Array.from({ length: 100 }, (_, i) => i),
      ...Array.from({ length: 1000 }, (_, i) => 200 + i),
    ]);
    for (const item of items) {
      expect(item.sha256).toBe(sha256Hex(contentBytes(contents[item.tileId]!)));
      expect(item.verify).toBe(item.tileId >= 200);
    }
    const distinct = new Set(items.map((item) => contents[item.tileId]));
    expect(reads).toBeLessThanOrEqual(distinct.size + 20);
  });
});

describe('clipManifest', () => {
  it('cuts a manifest at a tile id, splitting the run across it', async () => {
    const { dir, remove } = await scratchDir();
    cleanup.push(remove);
    const source = join(dir, 'full.tman.zst');
    await writeManifest(source, { buildKey: '20261118', minZoom: 0, maxZoom: 6, osmDate: '2026-11-18T00:00:00Z' }, [
      { tileId: 0, runLength: 3, hash: '00000000000000AA' },
      { tileId: 3, runLength: 10, hash: '00000000000000BB' },
      { tileId: 15, runLength: 5, hash: '00000000000000CC' },
    ]);
    const header = await clipManifest(source, join(dir, 'cut.tman.zst'), 8, 1);
    expect(header).toMatchObject({ buildKey: '20261118', positions: 8, entries: 2, maxZoom: 1, minZoom: 0 });
    expect(header.osmDate).toBe('2026-11-18T00:00:00Z');
    const entries = [];
    for await (const entry of readManifest(join(dir, 'cut.tman.zst'))) {
      entries.push(entry);
    }
    expect(entries).toEqual([
      { tileId: 0, runLength: 3, hash: '00000000000000AA' },
      { tileId: 3, runLength: 5, hash: '00000000000000BB' },
    ]);
  });
});

interface ChildResult {
  code: number | null;
  stdout: string;
  stderr: string;
}

async function purgeApi() {
  const calls: string[] = [];
  const server: Server = createServer((request, response) => {
    calls.push(`${request.method} ${request.url} ${request.headers.accesskey}`);
    response.writeHead(204).end();
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  cleanup.push(() => new Promise<void>((resolve) => server.close(() => resolve())));
  return { calls, url: `http://127.0.0.1:${(server.address() as { port: number }).port}` };
}

function startSync(w: World, args: string[], env: Record<string, string> = {}) {
  const child: ChildProcess = spawn(process.execPath, ['--import=tsx', 'src/bunny/cli/sync.ts', ...args], {
    cwd: TILES_DIR,
    env: {
      ...process.env,
      // The fake listens on loopback: storage addresses pinned in the developer's environment must not apply.
      BUNNY_STORAGE_ADDRESSES: '127.0.0.1',
      BUNNY_STORAGE_ZONE: w.tiles.zone,
      BUNNY_STORAGE_PASSWORD: w.tiles.password,
      BUNNY_STORAGE_HOST: w.tiles.url,
      BUNNY_STATE_ZONE: w.stateZone.zone,
      BUNNY_STATE_PASSWORD: w.stateZone.password,
      BUNNY_STATE_HOST: w.stateZone.url,
      BUNNY_API_KEY: 'test-key',
      BUNNY_PULL_ZONE_ID: '123',
      BUNNY_PUBLIC_URL: 'https://tiles.example',
      ...env,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '';
  let stderr = '';
  child.stdout!.on('data', (chunk) => (stdout += chunk));
  child.stderr!.on('data', (chunk) => (stderr += chunk));
  // 'close' comes once the output pipes are drained too, unlike 'exit'.
  const done = new Promise<ChildResult>((resolve, reject) => {
    child.once('error', reject);
    child.once('close', (code) => resolve({ code, stdout, stderr }));
  });
  cleanup.push(() => {
    child.kill('SIGKILL');
  });
  return { child, done };
}

describe('bunny:sync CLI', () => {
  async function cliWorld(options: FakeStorageOptions = {}) {
    const w = await world(options);
    await adopted(w);
    const styles = join(w.dir, 'styles');
    await mkdir(styles);
    await writeFile(join(styles, 'light.json'), STYLE.body);
    const api = await purgeApi();
    const report = join(w.dir, 'report.json');
    const args = (build: string, extra: string[] = []) => [
      '--archive',
      w.archives[build].path,
      '--build',
      build,
      '--report',
      report,
      '--styles-dir',
      styles,
      '--work-dir',
      join(w.dir, 'work'),
      '--threads',
      '2',
      '--regions',
      REGIONS.join(','),
      '--replication-interval-s',
      '1',
      // The fixture's day changes far more than a real one; the last --max-changed-fraction given wins.
      '--max-changed-fraction',
      '1',
      ...extra,
    ];
    const readReport = async () => JSON.parse(await readFile(report, 'utf8')) as SyncReport;
    return { w, api, args, readReport };
  }

  it('syncs, purges once through the API and writes the report', async () => {
    const { w, api, args, readReport } = await cliWorld();
    const { done } = startSync(w, args('20261118', ['--osm-date', '2026-11-18T01:02:03Z']), {
      BUNNY_API_URL: api.url,
    });
    const result = await done;
    expect(result.stderr).not.toMatch(/test-key/);
    expect(result.code).toBe(0);
    const report = await readReport();
    expect(report).toMatchObject({ status: 'success', osmDate: '2026-11-18T01:02:03Z', error: null });
    expect(JSON.parse(result.stdout)).toEqual(report);
    expect(api.calls).toEqual(['POST /pullzone/123/purgeCache test-key']);
    expect(w.tiles.identicalReputs).toEqual([]);
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261118' });
  });

  it('adopts a seed state with only a v1 fingerprint with --archive-md5, checked over the whole archive', async () => {
    const w = await world();
    seedZone(w, '20261117');
    const styles = join(w.dir, 'styles');
    await mkdir(styles);
    await writeFile(join(styles, 'light.json'), STYLE.body);
    const api = await purgeApi();
    const archive = w.archives['20261117'].path;
    const legacy = await seedState({ dir: w.dir, zone: w.tiles.zone, archive, legacy: true });
    const md5 = createHash('md5')
      .update(await readFile(archive))
      .digest('base64');
    const args = (extra: string[]) => [
      '--archive',
      archive,
      '--build',
      '20261117',
      '--adopt',
      '--seed-state',
      legacy,
      '--styles-dir',
      styles,
      '--work-dir',
      join(w.dir, 'work'),
      '--threads',
      '2',
      '--regions',
      REGIONS.join(','),
      '--adopt-samples',
      '50',
      '--replication-interval-s',
      '1',
      ...extra,
    ];
    const env = { BUNNY_API_URL: api.url };

    const malformed = await startSync(w, args(['--archive-md5', '!not-base64!']), env).done;
    expect(malformed.code).toBe(1);
    expect(malformed.stderr).toMatch(/--archive-md5 must be a base64 MD5/);
    const without = await startSync(w, args([]), env).done;
    expect(without.code).toBe(1);
    expect(without.stderr).toMatch(/only a v1 archive fingerprint/);
    const wrong = await startSync(w, args(['--archive-md5', 'hKPqtB5lzZzHKO5NlYsxDg==']), env).done;
    expect(wrong.code).toBe(1);
    expect(wrong.stderr).toMatch(/not the hKPqtB5lzZzHKO5NlYsxDg== given with --archive-md5/);
    expect(api.calls).toEqual([]);

    const adopted = await startSync(w, args(['--archive-md5', md5]), env).done;
    expect(adopted.code, adopted.stderr).toBe(0);
    expect(api.calls).toHaveLength(1);
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261117', archive: await archiveIdentity(archive) });
  });

  it('dry-runs with only the state zone credentials, as the workflow passes them', async () => {
    const { w, args, readReport } = await cliWorld();
    const { done } = startSync(w, args('20261118', ['--dry-run']), {
      BUNNY_STORAGE_PASSWORD: '',
      BUNNY_API_KEY: '',
      BUNNY_PULL_ZONE_ID: '',
      BUNNY_PUBLIC_URL: '',
    });
    const result = await done;
    expect(result.stderr).toContain('Dry run');
    expect(result.code).toBe(0);
    expect(await readReport()).toMatchObject({ status: 'success', dryRun: true, filesUploaded: null, error: null });
    expect(w.events).toEqual([]);
  });

  it('exits 3 at the guard rail and writes the report', async () => {
    const { w, api, args, readReport } = await cliWorld();
    const { done } = startSync(w, args('20261118', ['--max-changed-fraction', '0.0001']), { BUNNY_API_URL: api.url });
    expect((await done).code).toBe(3);
    expect(await readReport()).toMatchObject({ status: 'guard_rail', filesUploaded: 0 });
    expect(api.calls).toEqual([]);
  });

  it('exits 1 with a report when stopped by SIGTERM', async () => {
    const running: { child?: ChildProcess } = {};
    // SYD never replicates, so the run waits in the gate until the signal.
    const { w, api, args, readReport } = await cliWorld({
      replicationDelayMs: { UK: 5, NY: 5 },
      onRequest: ({ method, path }) => {
        if (method === 'PUT' && path.endsWith('.mvt') && running.child && !running.child.killed) {
          running.child.kill('SIGTERM');
        }
      },
    });
    // The signal comes with the first tile PUT: the requests in flight finish, then the run ends.
    const started = startSync(w, args('20261118'), { BUNNY_API_URL: api.url });
    running.child = started.child;
    const result = await started.done;
    expect(result.code).toBe(1);
    const report = await readReport();
    expect(report.status).toBe('failed');
    expect(report.error).toMatch(/SIGTERM/);
    expect(report.filesUploaded).toBeGreaterThan(0);
    expect(api.calls).toEqual([]);
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261117', pending: [{ build: '20261118' }] });
    expect(w.tiles.identicalReputs).toEqual([]);
  });
});
