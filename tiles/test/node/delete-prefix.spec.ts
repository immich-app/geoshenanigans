import { execFile } from 'node:child_process';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { BunnyStateStore, SyncState, writeState } from '../../src/bunny/state';
import { tilePath, zoomEnd, zoomStart } from '../../src/bunny/tile-id';
import { FakeStorage } from './fake-storage';
import { startStateServer } from './state-server';

const TILES_DIR = join(import.meta.dirname, '../..');
let cleanup: (() => Promise<void>)[] = [];

afterEach(async () => {
  vi.unstubAllEnvs();
  for (const step of cleanup.reverse()) {
    await step();
  }
  cleanup = [];
});

const live: SyncState = {
  liveBuild: '20261118',
  manifest: 'manifests/20261118.tman.zst',
  prefix: 'v1/e2',
  updatedAt: '2026-11-18T10:40:00.000Z',
  osmDate: null,
  positions: zoomEnd(8),
  archive: null,
  pending: [],
};

/** A TileJSON as bunny:sync writes it, pointing tiles at `prefix`. */
const tileJson = (prefix: string, base = 'https://tiles.example.com') =>
  new TextEncoder().encode(JSON.stringify({ tilejson: '3.0.0', tiles: [`${base}/${prefix}/{z}/{x}/{y}.mvt`] }));

async function setup(state: SyncState | null = live) {
  const tiles = await FakeStorage.start();
  const server = await startStateServer('state-zone', 'state-password');
  cleanup.push(
    () => tiles.close(),
    () => server.close(),
  );
  if (state) {
    const store = new BunnyStateStore({ zone: 'state-zone', password: 'state-password', host: server.host });
    await writeState(store, state);
    await store.close();
  }
  // The old build under v1 and the new one under v1/e2, zooms 0-8, beside the TileJSON and a style.
  for (let id = 0; id < zoomEnd(8); id++) {
    tiles.seed(tilePath('v1', id), new TextEncoder().encode(`old ${id}`));
    tiles.seed(tilePath('v1/e2', id), new TextEncoder().encode(`new ${id}`));
  }
  tiles.seed('v1.json', tileJson(state?.prefix ?? 'v1/e2'));
  tiles.seed('v1/style/light.json', new TextEncoder().encode('{}'));
  const run = (...args: string[]) =>
    promisify(execFile)(process.execPath, ['--import=tsx', 'src/bunny/cli/delete-prefix.ts', ...args], {
      cwd: TILES_DIR,
      env: {
        ...process.env,
        BUNNY_STORAGE_ZONE: tiles.zone,
        BUNNY_STORAGE_PASSWORD: tiles.password,
        BUNNY_STORAGE_HOST: tiles.url,
        // Resolve the fake's host: storage IPs pinned in the shell would send every request elsewhere.
        BUNNY_STORAGE_ADDRESSES: '',
        BUNNY_STATE_ZONE: 'state-zone',
        BUNNY_STATE_PASSWORD: 'state-password',
        BUNNY_STATE_HOST: server.host,
      },
    }).catch((error: { code: number; stderr: string }) => error);
  return { tiles, run };
}

const under = (tiles: FakeStorage, prefix: string) =>
  [...tiles.files.keys()].filter((path) => path.startsWith(`${prefix}/`) && /^\d/.test(path.slice(prefix.length + 1)));

describe('bunny:delete-prefix', () => {
  it("deletes an old prefix's tile folders only, after a dry run that deletes nothing", async () => {
    const { tiles, run } = await setup();
    // Not a tile folder: tile URLs never name zoom 9 `09`.
    tiles.seed('v1/09/0/0.mvt', new TextEncoder().encode('other'));
    const dry = await run('--prefix', 'v1');
    expect(dry.stderr).toMatch(/Would delete v1\/ zooms 0, 1, 2, 3, 4, 5, 6, 7, 8\n/);
    expect(under(tiles, 'v1')).toHaveLength(zoomEnd(8) + 1);

    // Each zoom's folder goes only once every one of its column DELETEs has been answered.
    tiles.options.latencyMs = () => Math.random() * 3;
    const inFlightAtParent: number[] = [];
    tiles.options.onRequest = (request) => {
      if (request.method === 'DELETE' && request.path === 'v1/8/') {
        inFlightAtParent.push(tiles.inFlight);
      }
    };
    const result = await run('--prefix', 'v1', '--yes', '--concurrency', '8');
    expect(result.stderr).toMatch(/Deleted v1\/ zooms 0, 1, 2, 3, 4, 5, 6, 7, 8 \(265 folders\)/);
    expect(under(tiles, 'v1')).toEqual(['v1/09/0/0.mvt']);
    expect(under(tiles, 'v1/e2')).toHaveLength(zoomEnd(8));
    expect(tiles.files.has('v1.json')).toBe(true);
    expect(tiles.files.has('v1/style/light.json')).toBe(true);
    // Zoom 8 went column by column, then its folder; nothing was sent to v1/9/.
    expect(tiles.requests.DELETE).toBe(9 + 2 ** 8);
    expect(inFlightAtParent).toEqual([1]);

    expect((await run('--prefix', 'v1', '--yes')).stderr).toMatch(/nothing to delete/);
  });

  it('deletes only the folders still there after a run stopped part-way', async () => {
    const { tiles, run } = await setup();
    // An earlier run deleted zooms 0-3 and the first half of zoom 8's columns.
    for (const path of [...tiles.files.keys()]) {
      const [, z, x] = path.split('/').map(Number);
      if (path.startsWith('v1/') && (z <= 3 || (z === 8 && x < 128))) {
        tiles.files.delete(path);
      }
    }
    const result = await run('--prefix', 'v1', '--yes');
    expect(result.stderr).toMatch(/Deleted v1\/ zooms 4, 5, 6, 7, 8 \(133 folders\)/);
    expect(tiles.requests.DELETE).toBe(5 + 128);
    expect(under(tiles, 'v1')).toEqual([]);
    expect(under(tiles, 'v1/e2')).toHaveLength(zoomEnd(8));
  });

  it('refuses the live prefix, a prefix holding it, and a zone without a state', async () => {
    const { tiles, run } = await setup({ ...live, prefix: 'v1/3' });
    for (const prefix of ['v1/3', 'v1']) {
      const refused = await run('--prefix', prefix, '--yes');
      expect(refused).toMatchObject({ code: 1 });
      expect(refused.stderr).toMatch(/20261118 is live under v1\/3\//);
    }
    expect(tiles.requests.DELETE ?? 0).toBe(0);

    const empty = await setup(null);
    const refused = await empty.run('--prefix', 'v1', '--yes');
    expect(refused).toMatchObject({ code: 1 });
    expect(refused.stderr).toMatch(/No state.json/);
    expect((await empty.run()).stderr).toMatch(/--prefix is required/);
  });

  it('refuses a zoom range that holds no zoom, with the default --max-zoom too', async () => {
    const { tiles, run } = await setup();
    for (const args of [
      ['--min-zoom', '16'],
      ['--min-zoom', '9', '--max-zoom', '8'],
      ['--max-zoom', '21'],
    ]) {
      const refused = await run('--prefix', 'v1', '--yes', ...args);
      expect(refused, args.join(' ')).toMatchObject({ code: 2 });
      expect(refused.stderr).toMatch(/--max-zoom must be an integer from \d+ to 20/);
    }
    expect(tiles.requests).toEqual({});
    // Given both, a range above the default is fine: zooms 16-20 hold no folders here.
    const above = await run('--prefix', 'v1', '--yes', '--min-zoom', '16', '--max-zoom', '20');
    expect(above.stderr).toMatch(/No tile folders of zooms 16-20 under v1\/: nothing to delete/);
    expect(tiles.requests.DELETE ?? 0).toBe(0);
  });

  it('uses the storage it is given whatever storage IPs the shell pins', async () => {
    // A developer's shell pinning the real storage servers (here an address nothing listens on).
    vi.stubEnv('BUNNY_STORAGE_ADDRESSES', '127.0.0.2');
    const { tiles, run } = await setup();
    const dry = await run('--prefix', 'v1');
    expect(dry.stderr).toMatch(/Would delete v1\/ zooms 0, 1, 2, 3, 4, 5, 6, 7, 8\n/);
    expect(tiles.requests.GET).toBeGreaterThan(0);
  });

  it('refuses the prefix the TileJSON serves when a switch failed before state.json, and an unreadable TileJSON', async () => {
    // A --replace to v1/e2 wrote v1.json and failed before making the build live: state.json still says v1.
    const { tiles, run } = await setup({ ...live, prefix: 'v1' });
    tiles.seed('v1.json', tileJson('v1/e2'));
    for (const args of [['--yes'], []]) {
      const refused = await run('--prefix', 'v1/e2', ...args);
      expect(refused).toMatchObject({ code: 1 });
      expect(refused.stderr).toMatch(/v1\.json points tiles at v1\/e2\/: they are under v1\/e2\/'s folders/);
    }
    // A public URL with a path of its own in front of the prefix.
    tiles.seed('v1.json', tileJson('v1/e2', 'https://cdn.example.com/maps'));
    expect((await run('--prefix', 'v1/e2', '--yes')).stderr).toMatch(/v1\.json points tiles at v1\/e2\//);
    // Another static prefix's TileJSON.
    tiles.seed('v2.json', tileJson('v1/e2'));
    expect((await run('--prefix', 'v1/e2', '--yes', '--static-prefix', 'v2')).stderr).toMatch(/v2\.json points/);
    // Every template counts, not just the first: here only the second names v1/e2.
    tiles.seed(
      'v1.json',
      new TextEncoder().encode(
        JSON.stringify({
          tilejson: '3.0.0',
          tiles: ['https://tiles.example.com/v3/{z}/{x}/{y}.mvt', 'https://tiles.example.com/v1/e2/{z}/{x}/{y}.mvt'],
        }),
      ),
    );
    const second = await run('--prefix', 'v1/e2', '--yes');
    expect(second).toMatchObject({ code: 1 });
    expect(second.stderr).toMatch(/v1\.json points tiles at v1\/e2\/: they are under v1\/e2\/'s folders/);
    expect(tiles.requests.DELETE ?? 0).toBe(0);

    const unreadable: [Uint8Array | null, RegExp][] = [
      [null, /v1\.json is not in the tiles zone/],
      [new TextEncoder().encode('<html>'), /v1\.json is not JSON/],
      [new TextEncoder().encode('{"tiles":[]}'), /v1\.json lists no tile templates/],
      [new TextEncoder().encode('null'), /v1\.json is not JSON/],
    ];
    for (const template of [
      'https://tiles.example.com/{z}/{x}/{y}.mvt',
      'https://tiles.example.com/v1/e2/{z}/{y}/{x}.mvt',
      'https://tiles.example.com/v1/%65%32/{z}/{x}/{y}.mvt',
      'https://tiles.example.com/v1/../e2/{z}/{x}/{y}.mvt',
    ]) {
      unreadable.push([new TextEncoder().encode(JSON.stringify({ tiles: [template] })), /tile template this cannot/]);
    }
    for (const [body, message] of unreadable) {
      if (body) {
        tiles.seed('v1.json', body);
      } else {
        tiles.files.delete('v1.json');
      }
      const refused = await run('--prefix', 'v1/e2', '--yes');
      expect(refused).toMatchObject({ code: 1 });
      expect(refused.stderr).toMatch(message);
      expect(refused.stderr).toMatch(/nothing is deleted/);
    }
    expect(tiles.requests.DELETE ?? 0).toBe(0);
    expect(under(tiles, 'v1/e2')).toHaveLength(zoomEnd(8));
  });

  it('refuses, before any DELETE, a prefix whose tile folders hold the styles or anything else but tile columns', async () => {
    const { tiles, run } = await setup();
    // A rewrite to v1/e2 that keeps the TileJSON and styles under --static-prefix v1/0: its styles are in zoom 0's folder.
    tiles.seed('v1/0.json', tileJson('v1/e2'));
    tiles.seed('v1/0/style/light.json', new TextEncoder().encode('{}'));
    for (const args of [['--yes'], []]) {
      const refused = await run('--prefix', 'v1', '--static-prefix', 'v1/0', ...args);
      expect(refused).toMatchObject({ code: 1 });
      expect(refused.stderr).toMatch(
        /--static-prefix v1\/0 keeps its styles \(v1\/0\/style\/\) inside v1\/'s tile folders/,
      );
    }
    // No request at all: the arguments alone say so.
    expect(tiles.requests).toEqual({});
    // Left out, the default v1's TileJSON (a stale one, still pointing at v1/e2) passes, but zoom 0's folder does not.
    for (const args of [['--yes'], []]) {
      const refused = await run('--prefix', 'v1', ...args);
      expect(refused).toMatchObject({ code: 1 });
      expect(refused.stderr).toMatch(
        /v1\/0\/ holds v1\/0\/style\/, not a tile column: deleting v1\/0\/ would delete it/,
      );
    }
    expect(tiles.files.has('v1/0/style/light.json')).toBe(true);

    // In a zoom above 7 too, which goes column by column and then as a folder: still before the first DELETE.
    tiles.files.delete('v1/0/style/light.json');
    tiles.seed('v1/8/notes.txt', new TextEncoder().encode('kept'));
    const stray = await run('--prefix', 'v1', '--yes');
    expect(stray).toMatchObject({ code: 1 });
    expect(stray.stderr).toMatch(/v1\/8\/ holds v1\/8\/notes\.txt, not a tile column/);
    expect(tiles.requests.DELETE ?? 0).toBe(0);
    // Every tile of v1, its v1/0.json and the stray file.
    expect(under(tiles, 'v1')).toHaveLength(zoomEnd(8) + 2);
    expect(tiles.files.has('v1/8/notes.txt')).toBe(true);
  });

  it('stops before its next folder when the zone switches to the prefix while it deletes', async () => {
    const { tiles, run } = await setup();
    tiles.options.onRequest = (request) => {
      if (request.method === 'DELETE') {
        // Someone switches back to v1 while the first folder is deleted.
        tiles.seed('v1.json', tileJson('v1'));
      }
    };
    const refused = await run('--prefix', 'v1', '--yes', '--concurrency', '1');
    expect(refused).toMatchObject({ code: 1 });
    expect(refused.stderr).toMatch(/v1\.json points tiles at v1\//);
    expect(tiles.requests.DELETE).toBe(1);
    expect(under(tiles, 'v1')).toHaveLength(zoomEnd(8) - zoomStart(1));
  });
});
