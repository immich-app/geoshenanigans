import { execFile } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { BunnyStateStore, liveStatus, SyncState, writeState } from '../../src/bunny/state';
import { startStateServer } from './state-server';

const run = promisify(execFile);
const TILES_DIR = fileURLToPath(new URL('../..', import.meta.url));

const state: SyncState = {
  liveBuild: '20261118',
  manifest: 'manifests/20261118.tman.zst',
  prefix: 'v1',
  updatedAt: '2026-11-18T10:40:00.000Z',
  osmDate: '2026-11-17T23:59:59Z',
  positions: 1_431_655_765,
  archive: null,
  pending: [],
};

describe('liveStatus', () => {
  it('measures the build from its date and the data from its OSM time', () => {
    // OSM data two hours older than the build date, so the two ages differ.
    expect(liveStatus({ ...state, osmDate: '2026-11-17T22:00:00Z' }, new Date('2026-11-19T06:06:00Z'))).toEqual({
      liveBuild: '20261118',
      updatedAt: '2026-11-18T10:40:00.000Z',
      osmDate: '2026-11-17T22:00:00Z',
      buildAgeHours: 30.1,
      osmAgeHours: 32.1,
      pending: [],
    });
    expect(liveStatus({ ...state, osmDate: null }, new Date('2026-11-18T12:00:00Z'))).toMatchObject({
      buildAgeHours: 12,
      osmAgeHours: null,
    });
  });

  it('is all nulls before the first sync', () => {
    expect(liveStatus(null)).toEqual({
      liveBuild: null,
      updatedAt: null,
      osmDate: null,
      buildAgeHours: null,
      osmAgeHours: null,
      pending: [],
    });
  });
});

describe('bunny:status', () => {
  // The CLI measures ages from the real clock, so its live build is yesterday's (UTC): 24 to 48 hours old.
  const liveBuild = new Date(Date.now() - 86_400_000).toISOString().slice(0, 10).replaceAll('-', '');
  const live: SyncState = { ...state, liveBuild, manifest: `manifests/${liveBuild}.tman.zst` };
  // Bunny settings in the caller's environment (a state host of their own, say) must not reach the CLI.
  const inherited = Object.fromEntries(Object.entries(process.env).filter(([name]) => !name.startsWith('BUNNY_')));

  let server: Awaited<ReturnType<typeof startStateServer>>;
  beforeEach(async () => {
    server = await startStateServer('immich-tiles-state', 'zone-password');
  });
  afterEach(async () => {
    await server.close();
  });

  const write = async (written: SyncState) => {
    const store = new BunnyStateStore({ zone: 'immich-tiles-state', password: 'zone-password', host: server.host });
    await writeState(store, written, 'itest-b-1').finally(() => store.close());
  };

  const cli = (args: string[], hosts: Record<string, string>) =>
    run(process.execPath, ['--import=tsx', 'src/bunny/cli/status.ts', ...args], {
      cwd: TILES_DIR,
      env: {
        ...inherited,
        BUNNY_STATE_ZONE: 'immich-tiles-state',
        BUNNY_STATE_PASSWORD: 'zone-password',
        ...hosts,
      },
    });
  const status = (...args: string[]) => cli(args, { BUNNY_STATE_HOST: server.host });

  it('prints the live build as JSON', async () => {
    await write(live);
    const { stdout } = await status('--json', '--state-prefix', 'itest-b-1');
    const printed = JSON.parse(stdout);
    expect(Object.keys(printed)).toEqual([
      'liveBuild',
      'updatedAt',
      'osmDate',
      'buildAgeHours',
      'osmAgeHours',
      'pending',
    ]);
    expect(printed).toMatchObject({ liveBuild, osmDate: '2026-11-17T23:59:59Z' });
    expect(printed.buildAgeHours).toBeGreaterThanOrEqual(24);
    expect(printed.buildAgeHours).toBeLessThan(49);

    const human = await status('--state-prefix', 'itest-b-1');
    expect(human.stdout).toMatch(new RegExp(`^live build ${liveBuild}, [\\d.]+ h old \\(prefix v1\\)`));
    expect(human.stdout).not.toMatch(/pending/);
  });

  it('reads the state zone at BUNNY_STATE_HOST over BUNNY_STORAGE_HOST, and at BUNNY_STORAGE_HOST without it', async () => {
    await write(live);
    const empty = await startStateServer('immich-tiles-state', 'zone-password');
    try {
      const preferred = await cli(['--json', '--state-prefix', 'itest-b-1'], {
        BUNNY_STATE_HOST: server.host,
        BUNNY_STORAGE_HOST: empty.host,
      });
      expect(JSON.parse(preferred.stdout)).toMatchObject({ liveBuild });
    } finally {
      await empty.close();
    }
    const fallback = await cli(['--json', '--state-prefix', 'itest-b-1'], { BUNNY_STORAGE_HOST: server.host });
    expect(JSON.parse(fallback.stdout)).toMatchObject({ liveBuild });
  });

  it('names builds a sync started and did not finish, in the JSON too (the workflow reverts them)', async () => {
    const pending = [
      { build: '20261119', manifest: 'manifests/20261119.tman.zst', positions: 1, startedAt: '2026-11-19T10:00:00Z' },
    ];
    await write({ ...live, pending });
    const human = await status('--state-prefix', 'itest-b-1');
    expect(human.stdout).toMatch(/^pending 20261119 since 2026-11-19T10:00:00Z/m);
    const { stdout } = await status('--json', '--state-prefix', 'itest-b-1');
    expect(JSON.parse(stdout)).toMatchObject({ liveBuild, pending: ['20261119'] });
  });

  it('exits 1 with nulls when nothing has been synced', async () => {
    const failure = await status('--json').catch((error: { code: number; stdout: string; stderr: string }) => error);
    expect(failure).toMatchObject({ code: 1 });
    expect(JSON.parse((failure as { stdout: string }).stdout)).toMatchObject({ liveBuild: null, buildAgeHours: null });
    expect((failure as { stderr: string }).stderr).toMatch(/No state.json/);
  });
});
