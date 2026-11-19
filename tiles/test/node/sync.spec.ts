import { ChildProcess, spawn } from 'node:child_process';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { createServer, Server } from 'node:http';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { ArchiveRanges } from '../../src/bunny/archive-walk';
import { clipManifest, readManifest, writeManifest } from '../../src/bunny/manifest';
import { BunnyStateStore, readState, SyncState } from '../../src/bunny/state';
import { BunnyStorage } from '../../src/bunny/storage';
import { planItems, runSync, SyncOptions, SyncReport } from '../../src/bunny/sync';
import { tilePath, zoomEnd } from '../../src/bunny/tile-id';
import { FakeStorage, FakeStorageOptions, sha256Hex } from './fake-storage';
import { Build, contentBytes, mutateBuild, randomBuild, rng, writeArchiveFile } from './manifest-fixtures';
import { scratchDir } from './upload-fixture';

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
  const metadata = { name: 'test', 'planetiler:osm:osmosisreplicationtime': '2026-11-17T20:21:22Z' };
  const archives: World['archives'] = {};
  for (const [key, build] of [
    ['20261117', x],
    ['20261118', y],
  ] as const) {
    const path = join(dir, `${key}.pmtiles`);
    await writeArchiveFile(path, build, r, { maxZoom: 6, metadata, maxLeafEntries: 40 });
    archives[key] = { path, build };
  }
  const events: string[] = [];
  const tiles = await FakeStorage.start({
    regions: REGIONS,
    replicationDelayMs: { UK: 5, NY: 10, SYD: 20 },
    ...tileOptions,
    onRequest: (request) => {
      if (request.method !== 'GET') {
        events.push(`tiles ${request.method} ${request.path}`);
      }
      tileOptions.onRequest?.(request);
    },
  });
  const stateZone = await FakeStorage.start({
    zone: 'state-zone',
    onRequest: ({ method, path }) => method === 'PUT' && events.push(`state PUT ${path}`),
  });
  const storage = new BunnyStorage({
    zone: tiles.zone,
    password: tiles.password,
    host: tiles.url,
    retry: fast,
    timeoutMs: 1_000,
    concurrency: 32,
  });
  const state = new BunnyStateStore({ zone: stateZone.zone, password: stateZone.password, host: stateZone.url });
  cleanup.push(
    () => tiles.close(),
    () => stateZone.close(),
    () => storage.close(),
    () => state.close(),
  );
  const w: World = {
    dir,
    tiles,
    stateZone,
    storage,
    state,
    events,
    purges: 0,
    archives,
    options: (build, extra = {}) => ({
      archive: archives[build].path,
      build,
      prefix: 'v1',
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

/** Stores build `key` at every position as a seed would have, replicated everywhere. */
function seedZone(w: World, key: string, prefix = 'v1', end = POSITIONS) {
  const { build } = w.archives[key];
  for (let id = 0; id < Math.min(end, build.contents.length); id++) {
    w.tiles.seed(tilePath(prefix, id), contentBytes(build.contents[id]!), REGIONS);
  }
}

function changedPositions(w: World, from: string, to: string, end = POSITIONS): number[] {
  const a = w.archives[from].build.contents;
  const b = w.archives[to].build.contents;
  const changed: number[] = [];
  for (let id = 0; id < end; id++) {
    if (sha256Hex(contentBytes(a[id]!)) !== sha256Hex(contentBytes(b[id]!))) {
      changed.push(id);
    }
  }
  return changed;
}

function expectHolds(w: World, key: string, prefix = 'v1', end = POSITIONS) {
  const { build } = w.archives[key];
  for (let id = 0; id < end; id++) {
    expect(w.tiles.checksum(tilePath(prefix, id)), `position ${id}`).toBe(sha256Hex(contentBytes(build.contents[id]!)));
  }
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

async function adopted(w: World, key = '20261117', extra: Partial<SyncOptions> = {}) {
  seedZone(w, key);
  const report = await runSync(w.options(key, { adopt: true, ...extra }), w.deps());
  expect(report).toMatchObject({ status: 'success', mode: 'adopt', filesUploaded: null, error: null });
  w.events.length = 0;
  w.purges = 0;
  w.tiles.putsByPath.clear();
  return report;
}

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
      purged: true,
    });
    expect(tilePuts(w)).toEqual([]);
    expect(w.purges).toBe(1);
    const state = (await readState(w.state)) as SyncState;
    expect(state).toMatchObject({ liveBuild: '20261117', prefix: 'v1', positions: POSITIONS, pending: [] });
    expect(w.stateZone.checksum(state.manifest)).toBeDefined();
    const tileJson = JSON.parse(new TextDecoder().decode(w.tiles.files.get('v1.json')!.bytes)) as { tiles: string[] };
    expect(tileJson.tiles).toEqual(['https://tiles.example/v1/{z}/{x}/{y}.mvt']);
    expect(w.tiles.files.has('v1/style/light.json')).toBe(true);
    expect(w.tiles.files.has('v1/style/light')).toBe(true);
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
      ['diff', 'manifest', 'purge', 'replication', 'state', 'static', 'upload'].sort(),
    );
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
    // The manifest went up before the pending record and the switch.
    const manifestPut = w.events.indexOf('state PUT manifests/20261118.tman.zst');
    expect(manifestPut).toBeGreaterThanOrEqual(0);
    expect(manifestPut).toBeLessThan(w.events.indexOf('state PUT state.json'));
    expect(manifestPut).toBeLessThan(
      w.events.findIndex((event) => event.startsWith('tiles PUT ') && event.endsWith('.mvt')),
    );
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

    const second = await runSync(w.options('20261118'), w.deps());
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
    const first = await runSync(w.options('20261118', { replicationTimeoutMs: 300 }), w.deps());

    expect(first.status).toBe('failed');
    expect(first.replicationComplete).toBe(false);
    expect(first.replicationIncomplete).toBe(changed.length);
    expect(first.replicationWaitSeconds).toMatchObject({ SYD: null });
    expect(typeof first.replicationWaitSeconds!.UK).toBe('number');
    expect(first.error).toMatch(/Replication incomplete.*SYD/);
    expect(w.purges).toBe(0);
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261117', pending: [{ build: '20261118' }] });

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

  it('writes every position with --force-full, DESCRIBEing each first', async () => {
    const w = await world();
    // Half the zone already holds the build (a seed that stopped half-way).
    const { build } = w.archives['20261118'];
    for (let id = 0; id < POSITIONS; id += 2) {
      w.tiles.seed(tilePath('v1', id), contentBytes(build.contents[id]!));
    }
    const report = await runSync(w.options('20261118', { forceFull: true }), w.deps());
    expect(report.error).toBeNull();
    expect(report).toMatchObject({
      status: 'success',
      mode: 'force-full',
      positionsVerified: POSITIONS,
      filesSkipped: Math.ceil(POSITIONS / 2),
      filesUploaded: Math.floor(POSITIONS / 2),
      positionsChanged: null,
    });
    expect(w.tiles.identicalReputs).toEqual([]);
    expectHolds(w, '20261118');
    expect(await readState(w.state)).toMatchObject({ liveBuild: '20261118', pending: [] });
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
  const done = new Promise<ChildResult>((resolve) => child.once('exit', (code) => resolve({ code, stdout, stderr })));
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
