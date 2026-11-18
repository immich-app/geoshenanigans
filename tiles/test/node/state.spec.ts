import { readFile } from 'node:fs/promises';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { writeManifest } from '../../src/bunny/manifest';
import {
  BunnyStateStore,
  downloadManifest,
  manifestPath,
  MemoryObjectStore,
  ObjectStore,
  parseState,
  publishBuild,
  readState,
  statePath,
  SyncState,
  writeState,
} from '../../src/bunny/state';
import { scratchDir } from './manifest-fixtures';
import { startStateServer } from './state-server';

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
      describe: (path) => memory.describe(path),
    };
    const file = await manifestFile('20261118');
    await expect(publishBuild(lossy, { buildKey: '20261118', manifestFile: file, prefix: 'v1' })).rejects.toThrow(
      /reads back as missing/,
    );
    expect(await readState(memory)).toBeNull();
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
      get: async (path) => {
        const bytes = await memory.get(path);
        bytes![bytes!.length - 1] ^= 1;
        return bytes;
      },
      put: (path, body, sha256) => memory.put(path, body, sha256),
      describe: (path) => memory.describe(path),
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

  const store = (password = 'zone-password') =>
    new BunnyStateStore({ zone: 'immich-tiles-state', password, host: server.host, attempts: 3 });

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
