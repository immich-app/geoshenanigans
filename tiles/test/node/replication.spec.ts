import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { checkReplication, classify, ReplicationItem, waitForReplication } from '../../src/bunny/replication';
import { BunnyStorage } from '../../src/bunny/storage';
import { tilePath, zoomEnd } from '../../src/bunny/tile-id';
import { runSeed } from '../../src/bunny/upload';
import { FakeStorage, FakeStorageOptions, sha256Hex } from './fake-storage';
import { scratchDir, writeFixtureArchive } from './upload-fixture';

const TILES_DIR = join(import.meta.dirname, '../..');
const REGIONS = ['UK', 'NY', 'SYD'];
const fast = { baseDelayMs: 1, maxDelayMs: 5, settleMs: 5, maxAttempts: 3, maxRateLimitWaitMs: 1_000 };
let cleanup: (() => Promise<void>)[] = [];

afterEach(async () => {
  for (const step of cleanup.reverse()) {
    await step();
  }
  cleanup = [];
});

async function setup(options: FakeStorageOptions = {}) {
  const fake = await FakeStorage.start({ regions: REGIONS, ...options });
  const storage = new BunnyStorage({ zone: fake.zone, password: fake.password, host: fake.url, retry: fast });
  cleanup.push(
    () => fake.close(),
    () => storage.close(),
  );
  return { fake, storage };
}

async function upload(storage: BunnyStorage, count: number): Promise<ReplicationItem[]> {
  return Promise.all(
    Array.from({ length: count }, async (_, i) => {
      const bytes = new TextEncoder().encode(`file ${i}`);
      const item = { path: `r/${i}.mvt`, sha256: sha256Hex(bytes) };
      await storage.put(item.path, bytes, item.sha256);
      return item;
    }),
  );
}

describe('classify', () => {
  const object = { checksum: 'AB', length: 1, replicatedZones: ['UK', 'NY'], lastChanged: '' };
  it('tells replicated, partial, missing and mismatched files apart', () => {
    expect(classify(object, 'ab', ['UK', 'NY'])).toEqual({ state: 'replicated', missingRegions: [] });
    expect(classify(object, 'AB', REGIONS)).toEqual({ state: 'partial', missingRegions: ['SYD'] });
    expect(classify(null, 'AB', REGIONS).state).toBe('missing');
    expect(classify(object, 'CD', REGIONS).state).toBe('mismatched');
  });
});

describe('waitForReplication', () => {
  it('follows each region until it holds every file, re-checking only what is still missing', async () => {
    const { fake, storage } = await setup({ replicationDelayMs: { UK: 20, NY: 150, SYD: 400 } });
    const items = await upload(storage, 60);
    const describesBefore = fake.requests.DESCRIBE ?? 0;
    const passes: number[] = [];
    const result = await waitForReplication(storage, items, REGIONS, {
      timeoutMs: 5_000,
      intervalMs: 30,
      onPass: ({ pending }) => passes.push(pending),
    });

    expect(result).toMatchObject({ complete: true, missing: [], mismatched: [], incomplete: 0 });
    expect(result.missingByRegion).toEqual({ UK: 0, NY: 0, SYD: 0 });
    expect(result.regionWaitMs.UK!).toBeLessThan(result.regionWaitMs.NY!);
    expect(result.regionWaitMs.NY!).toBeLessThan(result.regionWaitMs.SYD!);
    expect(result.regionWaitMs.SYD!).toBeGreaterThanOrEqual(350);
    expect(passes.at(-1)).toBe(0);
    // Later passes only DESCRIBE files that some region still lacked.
    expect(fake.requests.DESCRIBE - describesBefore).toBeLessThan(60 * result.passes);
  });

  it('fails at once on missing and mismatched files', async () => {
    const { fake, storage } = await setup({ replicationDelayMs: { UK: 1, NY: 1, SYD: 1 } });
    const items = await upload(storage, 5);
    fake.seed('r/other.mvt', new TextEncoder().encode('other'));
    const result = await waitForReplication(
      storage,
      [...items, { path: 'r/none.mvt', sha256: 'AA' }, { path: 'r/other.mvt', sha256: items[0].sha256 }],
      REGIONS,
      { timeoutMs: 10_000, intervalMs: 20 },
    );
    expect(result.complete).toBe(false);
    expect(result.missing).toEqual(['r/none.mvt']);
    expect(result.mismatched).toEqual(['r/other.mvt']);
    expect(result.waitedMs).toBeLessThan(5_000);
    expect(result.missingByRegion).toEqual({ UK: 2, NY: 2, SYD: 2 });
  });

  it('times out on a file whose replication an identical re-PUT cleared', async () => {
    const { fake, storage } = await setup();
    const items = await upload(storage, 3);
    fake.replicate();
    // What the uploader must never do: PUT the same bytes again.
    const bytes = new TextEncoder().encode('file 1');
    await storage.put(items[1].path, bytes, items[1].sha256);
    expect(fake.identicalReputs).toEqual([items[1].path]);

    const result = await waitForReplication(storage, items, REGIONS, { timeoutMs: 150, intervalMs: 20 });
    expect(result).toMatchObject({ complete: false, incomplete: 1, missingByRegion: { UK: 1, NY: 1, SYD: 1 } });
    expect(result.regionWaitMs).toEqual({ UK: null, NY: null, SYD: null });
  });
});

describe('checkReplication', () => {
  it('reports coverage per region and per zoom from one DESCRIBE per file', async () => {
    const { fake, storage } = await setup();
    const items: ReplicationItem[] = [];
    for (let tileId = 0; tileId < 21; tileId++) {
      const bytes = new TextEncoder().encode(`t${tileId}`);
      fake.seed(tilePath('v1', tileId), bytes);
      items.push({ path: tilePath('v1', tileId), sha256: sha256Hex(bytes), tileId });
    }
    fake.replicate('v1/0/');
    fake.replicate('v1/1/', ['UK']);
    items.push({ path: tilePath('v1', 21), sha256: 'AA', tileId: 21 });
    items[20] = { ...items[20], sha256: 'BB' };

    const report = await checkReplication(storage, items, REGIONS, { concurrency: 4 });
    expect(report).toMatchObject({ checked: 22, replicated: 1, partial: 19, missing: 1, mismatched: 1, errors: 0 });
    expect(report.regions).toEqual({ UK: 5, NY: 1, SYD: 1 });
    expect(report.byZoom['0']).toMatchObject({ checked: 1, replicated: 1 });
    expect(report.byZoom['1'].regions).toEqual({ UK: 4, NY: 0, SYD: 0 });
    expect(report.byZoom['2']).toMatchObject({ checked: 16, mismatched: 1, partial: 15 });
    expect(report.byZoom['3']).toMatchObject({ checked: 1, missing: 1 });
    expect(report.missingPaths).toEqual([tilePath('v1', 21)]);
    expect(report.mismatchedPaths).toEqual([tilePath('v1', 20)]);
    expect(report.coverage.UK).toBeCloseTo(5 / 22);
  });
});

describe('bunny:replication CLI', () => {
  function run(fake: FakeStorage, args: string[]) {
    const child = spawn(process.execPath, ['--import=tsx', 'src/bunny/cli/replication.ts', ...args], {
      cwd: TILES_DIR,
      env: {
        ...process.env,
        BUNNY_STORAGE_ZONE: fake.zone,
        BUNNY_STORAGE_PASSWORD: fake.password,
        BUNNY_STORAGE_HOST: fake.url,
      },
    });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk) => (stdout += chunk));
    child.stderr.on('data', (chunk) => (stderr += chunk));
    return new Promise<{ code: number | null; report: Record<string, any>; stderr: string }>((resolve) =>
      child.once('exit', (code) => resolve({ code, report: stdout ? JSON.parse(stdout) : {}, stderr })),
    );
  }

  it('checks samples and whole ranges against the archive', async () => {
    const scratch = await scratchDir();
    cleanup.push(scratch.remove);
    const positions = zoomEnd(5);
    const fixture = await writeFixtureArchive(scratch.dir, positions, 21, 100);
    const { fake } = await setup({ regions: ['UK', 'SYD'] });
    await runSeed({
      archivePath: fixture.path,
      prefix: 'itest',
      from: 0,
      to: positions,
      chunkSize: 256,
      workers: 2,
      concurrency: 16,
      stateDir: join(scratch.dir, 'state'),
      verifyExisting: false,
      storage: { zone: fake.zone, password: fake.password, host: fake.url, retry: fast },
      log: () => {},
    });
    const common = ['--archive', fixture.path, '--prefix', 'itest', '--regions', 'UK,SYD'];
    const base = [...common, '--max-zoom', '5'];

    const pending = await run(fake, [...base, '--sample', '20', '--per-zoom', '--seed', '5']);
    expect(pending.code, pending.stderr).toBe(2);
    expect(pending.report).toMatchObject({ mode: 'sample-per-zoom', seed: 5, missing: 0, mismatched: 0 });
    // z0-2 have fewer than 20 positions: all of them.
    expect(pending.report.checked).toBe(1 + 4 + 16 + 20 * 3);
    expect(pending.report.byZoom['5'].checked).toBe(20);

    fake.replicate();
    const done = await run(fake, [...base]);
    expect(done.code, done.stderr).toBe(0);
    expect(done.report).toMatchObject({ mode: 'range', checked: positions, replicated: positions });
    expect(done.stderr).toMatch(/z5 +1,024 files, 100.0% everywhere/);

    fake.seed(tilePath('itest', 7), new TextEncoder().encode('wrong'));
    fake.files.delete(tilePath('itest', 9));
    const firstZooms = [...common, '--from-id', '0', '--to-id', '21'];
    const broken = await run(fake, firstZooms);
    expect(broken.code).toBe(1);
    expect(broken.report).toMatchObject({ checked: 21, missing: 1, mismatched: 1 });
    expect(broken.report.mismatchedPaths).toEqual([tilePath('itest', 7)]);
    expect(broken.report.missingPaths).toEqual([tilePath('itest', 9)]);

    const waited = await run(fake, [...firstZooms, '--wait', '--interval-s', '1', '--timeout-min', '1']);
    expect(waited.code, waited.stderr).toBe(1);
    expect(waited.report).toMatchObject({ complete: false, files: 21, missing: [tilePath('itest', 9)] });
  }, 60_000);
});
