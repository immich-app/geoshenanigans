import { execFile } from 'node:child_process';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { afterEach, describe, expect, it } from 'vitest';
import { BunnyStateStore, SyncState, writeState } from '../../src/bunny/state';
import { tilePath, zoomEnd } from '../../src/bunny/tile-id';
import { FakeStorage } from './fake-storage';
import { startStateServer } from './state-server';

const TILES_DIR = join(import.meta.dirname, '../..');
let cleanup: (() => Promise<void>)[] = [];

afterEach(async () => {
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
  tiles.seed('v1.json', new TextEncoder().encode('{}'));
  tiles.seed('v1/style/light.json', new TextEncoder().encode('{}'));
  const run = (...args: string[]) =>
    promisify(execFile)(process.execPath, ['--import=tsx', 'src/bunny/cli/delete-prefix.ts', ...args], {
      cwd: TILES_DIR,
      env: {
        ...process.env,
        BUNNY_STORAGE_ZONE: tiles.zone,
        BUNNY_STORAGE_PASSWORD: tiles.password,
        BUNNY_STORAGE_HOST: tiles.url,
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
    const dry = await run('--prefix', 'v1');
    expect(dry.stderr).toMatch(/Would delete v1\/ zooms 0, 1, 2, 3, 4, 5, 6, 7, 8/);
    expect(under(tiles, 'v1')).toHaveLength(zoomEnd(8));

    const result = await run('--prefix', 'v1', '--yes', '--concurrency', '8');
    expect(result.stderr).toMatch(/Deleted v1\/ zooms 0, .*, 8 \((\d+) folders\)/);
    expect(under(tiles, 'v1')).toEqual([]);
    expect(under(tiles, 'v1/e2')).toHaveLength(zoomEnd(8));
    expect(tiles.files.has('v1.json')).toBe(true);
    expect(tiles.files.has('v1/style/light.json')).toBe(true);
    // Zoom 8 went column by column.
    expect(tiles.requests.DELETE).toBe(9 + 2 ** 8);

    expect((await run('--prefix', 'v1', '--yes')).stderr).toMatch(/nothing to delete/);
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
});
