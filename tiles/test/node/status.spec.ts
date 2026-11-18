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
};

describe('liveStatus', () => {
  it('measures the build from its date and the data from its OSM time', () => {
    expect(liveStatus(state, new Date('2026-11-19T06:06:00Z'))).toEqual({
      liveBuild: '20261118',
      updatedAt: '2026-11-18T10:40:00.000Z',
      osmDate: '2026-11-17T23:59:59Z',
      buildAgeHours: 30.1,
      osmAgeHours: 30.1,
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
    });
  });
});

describe('bunny:status', () => {
  let server: Awaited<ReturnType<typeof startStateServer>>;
  beforeEach(async () => {
    server = await startStateServer('immich-tiles-state', 'zone-password');
  });
  afterEach(async () => {
    await server.close();
  });

  const status = (...args: string[]) =>
    run(process.execPath, ['--import=tsx', 'src/bunny/cli/status.ts', ...args], {
      cwd: TILES_DIR,
      env: {
        ...process.env,
        BUNNY_STATE_ZONE: 'immich-tiles-state',
        BUNNY_STATE_PASSWORD: 'zone-password',
        BUNNY_STORAGE_HOST: server.host,
      },
    });

  it('prints the live build as JSON', async () => {
    const store = new BunnyStateStore({ zone: 'immich-tiles-state', password: 'zone-password', host: server.host });
    await writeState(store, state, 'itest-b-1');
    const { stdout } = await status('--json', '--state-prefix', 'itest-b-1');
    const printed = JSON.parse(stdout);
    expect(Object.keys(printed)).toEqual(['liveBuild', 'updatedAt', 'osmDate', 'buildAgeHours', 'osmAgeHours']);
    expect(printed).toMatchObject({ liveBuild: '20261118', osmDate: '2026-11-17T23:59:59Z' });
    expect(printed.buildAgeHours).toBeGreaterThan(0);

    const human = await status('--state-prefix', 'itest-b-1');
    expect(human.stdout).toMatch(/^live build 20261118, [\d.]+ h old \(prefix v1\)/);
  });

  it('exits 1 with nulls when nothing has been synced', async () => {
    const failure = await status('--json').catch((error: { code: number; stdout: string; stderr: string }) => error);
    expect(failure).toMatchObject({ code: 1 });
    expect(JSON.parse((failure as { stdout: string }).stdout)).toMatchObject({ liveBuild: null, buildAgeHours: null });
    expect((failure as { stderr: string }).stderr).toMatch(/No state.json/);
  });
});
