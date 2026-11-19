import { createHash, randomBytes } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { writeManifest } from '../../src/bunny/manifest';
import {
  beginBuild,
  BunnyStateStore,
  downloadManifest,
  manifestPath,
  MemoryObjectStore,
  ObjectStore,
  parseState,
  pruneManifests,
  publishBuild,
  readState,
  STATE_STORE_DEFAULTS,
  statePath,
  SyncState,
  writeState,
} from '../../src/bunny/state';
import { scratchDir } from './manifest-fixtures';
import { startStateServer } from './state-server';

const hex = (bytes: Uint8Array) => createHash('sha256').update(bytes).digest('hex').toUpperCase();

let scratch: Awaited<ReturnType<typeof scratchDir>>;
beforeEach(async () => {
  scratch = await scratchDir();
});
afterEach(async () => {
  await scratch.cleanup();
});

async function manifestFile(buildKey: string, osmDate: string | null = '2026-11-17T23:59:59Z') {
  const path = scratch.path(`${buildKey}.tman.zst`);
  await writeManifest(path, { buildKey, osmDate }, [
    { tileId: 0, runLength: 21, hash: 'AAAAAAAAAAAAAAAA' },
    { tileId: 21, runLength: 64, hash: 'BBBBBBBBBBBBBBBB' },
  ]);
  return path;
}

const state: SyncState = {
  liveBuild: '20261118',
  manifest: 'manifests/20261118.tman.zst',
  prefix: 'v1',
  updatedAt: '2026-11-18T10:40:00.000Z',
  osmDate: '2026-11-17T23:59:59Z',
  positions: 85,
  archive: null,
  pending: [],
};

describe('state paths', () => {
  it('sit at the zone root or under a state prefix', () => {
    expect(statePath()).toBe('state.json');
    expect(statePath('itest-b-1')).toBe('itest-b-1/state.json');
    expect(statePath('/itest-b-1/')).toBe('itest-b-1/state.json');
    expect(manifestPath('20261118')).toBe('manifests/20261118.tman.zst');
    expect(manifestPath('20261118', 'itest-b-1')).toBe('itest-b-1/manifests/20261118.tman.zst');
    expect(() => manifestPath('../x')).toThrow(/YYYYMMDD/);
    expect(() => statePath('a/../b')).toThrow(/Invalid state prefix/);
  });
});

describe('parseState', () => {
  it('accepts a state without an OSM date', () => {
    const withoutOsm: Partial<SyncState> = { ...state };
    delete withoutOsm.osmDate;
    expect(parseState(withoutOsm)).toEqual({ ...state, osmDate: null });
  });

  it('reads a state.json written before pending builds were recorded', () => {
    const old: Partial<SyncState> = { ...state };
    delete old.pending;
    expect(parseState(old)).toEqual(state);
  });

  it('checks the pending builds', () => {
    const entry = {
      build: '20261119',
      manifest: 'manifests/20261119.tman.zst',
      positions: 85,
      startedAt: state.updatedAt,
    };
    expect(parseState({ ...state, pending: [entry] }).pending).toEqual([entry]);
    for (const pending of [
      {},
      [{ ...entry, build: 'latest' }],
      [{ ...entry, build: '20261118' }],
      [entry, entry],
      [{ ...entry, positions: -1 }],
      [{ ...entry, startedAt: undefined }],
      [null],
    ]) {
      expect(() => parseState({ ...state, pending }), JSON.stringify(pending)).toThrow(
        'state.json has invalid pending',
      );
    }
  });

  it('checks the archive the live build came from', () => {
    const archive = { size: 139_600_000_000, identity: `sha256-tree-v2:${'ab'.repeat(32)}` };
    expect(parseState({ ...state, archive }).archive).toEqual(archive);
    for (const bad of [
      {},
      { ...archive, size: -1 },
      { ...archive, identity: `sha256-tree-v2:${'AB'.repeat(32)}` },
      { ...archive, identity: 'ab'.repeat(32) },
      { size: archive.size, fingerprint: 'AB'.repeat(32) },
      'x',
    ]) {
      expect(() => parseState({ ...state, archive: bad }), JSON.stringify(bad)).toThrow(
        'state.json has invalid archive',
      );
    }
  });

  it('reads a v1 archive fingerprint as no identity, which vouches for no archive', () => {
    const legacy = { size: 139_600_000_000, fingerprint: 'ab'.repeat(32) };
    expect(parseState({ ...state, archive: legacy }).archive).toBeNull();
  });

  it('names every invalid field', () => {
    expect(() => parseState({ ...state, liveBuild: 'latest' })).toThrow(/liveBuild/);
    expect(() => parseState({ ...state, manifest: '', updatedAt: 'yesterday', positions: -1 })).toThrow(
      'state.json has invalid manifest, updatedAt, positions',
    );
    expect(() => parseState(null)).toThrow(/not an object/);
  });
});

describe('publishBuild', () => {
  it('stores the manifest and reads it back before switching state.json', async () => {
    const store = new MemoryObjectStore();
    const file = await manifestFile('20261118');
    const published = await publishBuild(store, {
      buildKey: '20261118',
      manifestFile: file,
      prefix: 'v1',
      now: new Date('2026-11-18T10:40:00Z'),
    });
    expect(published).toEqual(state);
    expect(store.log.filter((call) => call.startsWith('put'))).toEqual([
      'put manifests/20261118.tman.zst',
      'put state.json',
    ]);
    // Read back before the state switches.
    expect(
      store.log.indexOf('describe manifests/20261118.tman.zst', store.log.indexOf('put manifests/20261118.tman.zst')),
    ).toBeLessThan(store.log.indexOf('put state.json'));
    expect(await readState(store)).toEqual(state);
    expect(store.objects.get('manifests/20261118.tman.zst')).toEqual(new Uint8Array(await readFile(file)));
  });

  it('does not send a manifest that is already stored', async () => {
    const store = new MemoryObjectStore();
    const file = await manifestFile('20261118');
    await publishBuild(store, { buildKey: '20261118', manifestFile: file, prefix: 'v1' });
    store.log.length = 0;
    await publishBuild(store, { buildKey: '20261118', manifestFile: file, prefix: 'v1', osmDate: null });
    expect(store.log.filter((call) => call.startsWith('put'))).toEqual(['put state.json']);
    expect((await readState(store))?.osmDate).toBeNull();
  });

  it('refuses a manifest of another build', async () => {
    const store = new MemoryObjectStore();
    const file = await manifestFile('20261117');
    await expect(publishBuild(store, { buildKey: '20261118', manifestFile: file, prefix: 'v1' })).rejects.toThrow(
      /manifest of 20261117/,
    );
    expect(store.objects.size).toBe(0);
  });

  it('fails when the store does not hold what was sent', async () => {
    const memory = new MemoryObjectStore();
    const lossy: ObjectStore = {
      get: (path) => memory.get(path),
      put: (path, body, sha256) => memory.put(path, body.subarray(0, body.length - 1), sha256).catch(() => {}),
      putFile: async (path, file, sha256) => lossy.put(path, await readFile(file), sha256),
      describe: (path) => memory.describe(path),
      getToFile: (path, file) => memory.getToFile(path, file),
      list: (dir) => memory.list(dir),
      delete: (path) => memory.delete(path),
    };
    const file = await manifestFile('20261118');
    await expect(publishBuild(lossy, { buildKey: '20261118', manifestFile: file, prefix: 'v1' })).rejects.toThrow(
      /reads back as missing/,
    );
    expect(await readState(memory)).toBeNull();
  });
});

describe('beginBuild', () => {
  async function live(store: MemoryObjectStore) {
    return publishBuild(store, { buildKey: '20261118', manifestFile: await manifestFile('20261118'), prefix: 'v1' });
  }

  it('records the build as pending before any tile is written, keeping the live build', async () => {
    const store = new MemoryObjectStore();
    const current = await live(store);
    store.log.length = 0;
    const file = await manifestFile('20261119');
    const begun = await beginBuild(store, current, {
      buildKey: '20261119',
      manifestFile: file,
      now: new Date('2026-11-19T10:00:00Z'),
    });
    expect(begun).toEqual({
      ...current,
      pending: [
        {
          build: '20261119',
          manifest: 'manifests/20261119.tman.zst',
          positions: 85,
          startedAt: '2026-11-19T10:00:00.000Z',
        },
      ],
    });
    expect(store.log.filter((call) => call.startsWith('put'))).toEqual([
      'put manifests/20261119.tman.zst',
      'put state.json',
    ]);
    expect(await readState(store)).toEqual(begun);
    expect(store.objects.get('manifests/20261119.tman.zst')).toEqual(new Uint8Array(await readFile(file)));
  });

  it('keeps every build that did not go live until one does', async () => {
    const store = new MemoryObjectStore();
    let current = await live(store);
    current = await beginBuild(store, current, { buildKey: '20261119', manifestFile: await manifestFile('20261119') });
    const file20 = await manifestFile('20261120');
    current = await beginBuild(store, current, { buildKey: '20261120', manifestFile: file20 });
    // A re-run of a pending build keeps one entry for it.
    current = await beginBuild(store, current, { buildKey: '20261119', manifestFile: await manifestFile('20261119') });
    expect(current.pending.map((p) => p.build)).toEqual(['20261120', '20261119']);
    expect((await readState(store))?.pending.map((p) => p.build)).toEqual(['20261120', '20261119']);

    const pendingFile = scratch.path('pending.tman.zst');
    await downloadManifest(store, current.pending[0], pendingFile);
    expect(await readFile(pendingFile)).toEqual(await readFile(file20));

    const published = await publishBuild(store, {
      buildKey: '20261121',
      manifestFile: await manifestFile('20261121'),
      prefix: 'v1',
    });
    expect(published.pending).toEqual([]);
    expect((await readState(store))?.pending).toEqual([]);
  });

  it('refuses a live build or a state that changed since the sync read it', async () => {
    const store = new MemoryObjectStore();
    const current = await live(store);
    const file = await manifestFile('20261119');
    await expect(beginBuild(store, current, { buildKey: '20261118', manifestFile: file })).rejects.toThrow(
      /already live/,
    );
    await beginBuild(store, current, { buildKey: '20261119', manifestFile: file });
    store.log.length = 0;
    await expect(
      beginBuild(store, current, { buildKey: '20261120', manifestFile: await manifestFile('20261120') }),
    ).rejects.toThrow(/changed since/);
    expect(store.log.filter((call) => call.startsWith('put'))).toEqual([]);
  });
});

describe('pruneManifests', () => {
  const put = (store: ObjectStore, path: string) => {
    const body = new TextEncoder().encode(path);
    return store.put(path, body, createHash('sha256').update(body).digest('hex').toUpperCase());
  };

  it('deletes manifests no state names, keeping the newest few', async () => {
    const store = new MemoryObjectStore();
    const file = await manifestFile('20261118');
    await publishBuild(store, { buildKey: '20261118', manifestFile: file, prefix: 'v1' });
    const pending = {
      build: '20261101',
      manifest: 'manifests/20261101.tman.zst',
      positions: 85,
      startedAt: state.updatedAt,
    };
    await writeState(store, { ...state, pending: [pending] });
    for (const day of ['01', '05', '10', '12', '15', '17', '19']) {
      await put(store, `manifests/202611${day}.tman.zst`);
    }
    await put(store, 'manifests/notes.txt');

    expect(await pruneManifests(store, { keep: 2 })).toEqual([
      'manifests/20261115.tman.zst',
      'manifests/20261112.tman.zst',
      'manifests/20261110.tman.zst',
      'manifests/20261105.tman.zst',
    ]);
    expect([...store.objects.keys()].sort()).toEqual([
      'manifests/20261101.tman.zst',
      'manifests/20261117.tman.zst',
      'manifests/20261118.tman.zst',
      'manifests/20261119.tman.zst',
      'manifests/notes.txt',
      'state.json',
    ]);
    expect(await pruneManifests(store, { keep: 2 })).toEqual([]);
  });

  it('does nothing without a state', async () => {
    const store = new MemoryObjectStore();
    await put(store, 'manifests/20261115.tman.zst');
    expect(await pruneManifests(store, { keep: 0 })).toEqual([]);
    expect(store.objects.size).toBe(1);
  });
});

describe('downloadManifest', () => {
  it('fetches the live manifest and checks it against state.json', async () => {
    const store = new MemoryObjectStore();
    const file = await manifestFile('20261118');
    const live = await publishBuild(store, { buildKey: '20261118', manifestFile: file, prefix: 'v1' });
    const out = scratch.path('live.tman.zst');
    await downloadManifest(store, live, out);
    expect(await readFile(out)).toEqual(await readFile(file));

    await expect(downloadManifest(store, { ...live, positions: 86 }, scratch.path('x.tman.zst'))).rejects.toThrow(
      /state.json says 20261118 with 86/,
    );
    await expect(
      downloadManifest(store, { ...live, manifest: 'manifests/20261101.tman.zst' }, scratch.path('y.tman.zst')),
    ).rejects.toThrow(/doesn't exist/);
  });

  it('rejects bytes that do not match the stored checksum', async () => {
    const memory = new MemoryObjectStore();
    const file = await manifestFile('20261118');
    const live = await publishBuild(memory, { buildKey: '20261118', manifestFile: file, prefix: 'v1' });
    const corrupting: ObjectStore = {
      get: (path) => memory.get(path),
      put: (path, body, sha256) => memory.put(path, body, sha256),
      putFile: (path, file, sha256) => memory.putFile(path, file, sha256),
      describe: (path) => memory.describe(path),
      list: (dir) => memory.list(dir),
      delete: (path) => memory.delete(path),
      getToFile: async (path, file) => {
        const bytes = await memory.get(path);
        bytes![bytes!.length - 1] ^= 1;
        await writeFile(file, bytes!);
        return true;
      },
    };
    await expect(downloadManifest(corrupting, live, scratch.path('live.tman.zst'))).rejects.toThrow(/downloaded as/);
  });
});

describe('BunnyStateStore', () => {
  let server: Awaited<ReturnType<typeof startStateServer>>;
  beforeEach(async () => {
    server = await startStateServer('immich-tiles-state', 'zone-password');
  });
  afterEach(async () => {
    await server.close();
  });

  let options: { zone: string; password: string; host: string };
  beforeEach(() => {
    options = { zone: 'immich-tiles-state', password: 'zone-password', host: server.host };
  });
  const store = (password = 'zone-password') => new BunnyStateStore({ ...options, password, attempts: 3 });

  it('publishes, reads and streams manifests through the storage API under a state prefix', async () => {
    const file = await manifestFile('20261118');
    const live = await publishBuild(store(), {
      buildKey: '20261118',
      manifestFile: file,
      prefix: 'v1',
      statePrefix: 'itest-b-1',
    });
    expect([...server.objects.keys()].sort()).toEqual([
      'itest-b-1/manifests/20261118.tman.zst',
      'itest-b-1/state.json',
    ]);
    expect(await readState(store(), 'itest-b-1')).toEqual(live);
    expect(await readState(store())).toBeNull();

    const out = scratch.path('live.tman.zst');
    await downloadManifest(store(), live, out, 'itest-b-1');
    expect(await readFile(out)).toEqual(await readFile(file));
    expect(await store().describe('itest-b-1/missing')).toBeNull();
    expect(await store().getToFile('itest-b-1/missing', scratch.path('missing'))).toBe(false);

    // Listing and deleting, as pruneManifests does.
    const older = await manifestFile('20261110');
    await store().putFile('itest-b-1/manifests/20261110.tman.zst', older, hex(await readFile(older)));
    expect((await store().list('itest-b-1/manifests')).sort()).toEqual(['20261110.tman.zst', '20261118.tman.zst']);
    expect(await store().list('itest-b-1/nothing')).toEqual([]);
    expect(await pruneManifests(store(), { keep: 0, statePrefix: 'itest-b-1' })).toEqual([
      'manifests/20261110.tman.zst',
    ]);
    expect([...server.objects.keys()].sort()).toEqual([
      'itest-b-1/manifests/20261118.tman.zst',
      'itest-b-1/state.json',
    ]);
    await store().delete('itest-b-1/missing');
  });

  it('retries 5xx and 429 answers', async () => {
    server.failNext.push(503, 429);
    await writeState(store(), state);
    expect(server.requests.filter((request) => request.startsWith('PUT'))).toHaveLength(3);
    expect(await readState(store())).toEqual(state);
  });

  it('gives up at once on a wrong password', async () => {
    await expect(store('wrong').get('state.json')).rejects.toThrow(/401/);
    expect(server.requests).toHaveLength(1);
  });

  it('reports a checksum the zone rejects', async () => {
    await expect(store().put('x.json', new Uint8Array([1, 2, 3]), 'AB'.repeat(32))).rejects.toThrow(/400/);
  });

  it('gives up on a zone that takes requests and never answers, within its per-try limit', async () => {
    server.faults.stallNext = 10;
    const hung = new BunnyStateStore({ ...options, attempts: 2, requestTimeoutMs: 300 });
    const started = performance.now();
    await expect(hung.get('state.json')).rejects.toThrow();
    expect(performance.now() - started).toBeLessThan(3000);
    expect(server.requests.filter((request) => request.endsWith('(stalled)'))).toHaveLength(2);
    await hung.close();
  });

  it('keeps a hung status check to about a minute by default, inside the watchdog job', () => {
    expect(STATE_STORE_DEFAULTS.attempts * STATE_STORE_DEFAULTS.requestTimeoutMs).toBeLessThanOrEqual(60_000);
  });

  it('streams a manifest over a slow link without timing out while bytes move', async () => {
    const bytes = randomBytes(32 << 20);
    const file = scratch.path('big.tman.zst');
    await writeFile(file, bytes);
    const sha256 = createHash('sha256').update(bytes).digest('hex').toUpperCase();
    server.faults.readBytesPerSecond = 12 << 20;
    const slow = new BunnyStateStore({ ...options, attempts: 1, idleTimeoutMs: 1000 });
    await slow.putFile('itest-b-1/big.tman.zst', file, sha256);
    expect(server.objects.get('itest-b-1/big.tman.zst')?.equals(bytes)).toBe(true);
    await slow.close();
  });

  it('retries a transfer that stops moving', async () => {
    const file = scratch.path('m.tman.zst');
    await writeFile(file, 'manifest');
    const sha256 = createHash('sha256').update('manifest').digest('hex').toUpperCase();
    server.faults.stallNext = 1;
    const quick = new BunnyStateStore({ ...options, attempts: 2, idleTimeoutMs: 300 });
    await quick.putFile('m.tman.zst', file, sha256);
    expect(server.requests).toEqual([
      'PUT /immich-tiles-state/m.tman.zst (stalled)',
      'PUT /immich-tiles-state/m.tman.zst',
    ]);
    server.faults.stallNext = 2;
    await expect(quick.putFile('m.tman.zst', file, sha256)).rejects.toThrow(/Headers Timeout/);
    await quick.close();
  });

  it('reads its settings from the environment', () => {
    const saved = { ...process.env };
    try {
      delete process.env.BUNNY_STATE_ZONE;
      expect(() => BunnyStateStore.fromEnv()).toThrow(/BUNNY_STATE_ZONE/);
      Object.assign(process.env, { BUNNY_STATE_ZONE: 'immich-tiles-state', BUNNY_STATE_PASSWORD: 'p' });
      expect(BunnyStateStore.fromEnv()).toBeInstanceOf(BunnyStateStore);
    } finally {
      process.env = saved;
    }
  });
});
