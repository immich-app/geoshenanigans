import { execFile, ExecFileException } from 'node:child_process';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { buildManifest, writeManifest } from '../../src/bunny/manifest';
import {
  checkReplication,
  checksumMatches,
  classify,
  gateExitCode,
  normalizeRegions,
  ReplicationItem,
  TileGateList,
  waitForReplication,
} from '../../src/bunny/replication';
import { BunnyStateStore, publishBuild, writeState } from '../../src/bunny/state';
import { BunnyStorage, DEFAULT_RETRY, StorageObject } from '../../src/bunny/storage';
import { tilePath, zoomEnd, zoomStart } from '../../src/bunny/tile-id';
import { runSeed } from '../../src/bunny/upload';
import { FakeStorage, FakeStorageOptions, sha256Hex } from './fake-storage';
import { startStateServer } from './state-server';
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

/**
 * A storage stand-in whose DESCRIBE answers `answer(path, call)` (call counts from 1 per path) after `delayMs`, for
 * gate tests that need exact answers in order rather than an HTTP fake.
 */
function stubStorage(
  answer: (path: string, call: number) => Partial<StorageObject> | null,
  delayMs = 0,
): { storage: BunnyStorage; describes: string[] } {
  const describes: string[] = [];
  const storage = {
    describe: async (path: string) => {
      describes.push(path);
      if (delayMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
      const object = answer(path, describes.filter((described) => described === path).length);
      return object && { checksum: 'AB'.repeat(32), length: 1, replicatedZones: [], lastChanged: '', ...object };
    },
  } as unknown as BunnyStorage;
  return { storage, describes };
}

/**
 * A fake whose DESCRIBEs get `fault` (from the `from`th attempt on, the ones before answered or failed by `before`),
 * and a client whose back-offs (10 s) and request timeout (60 s) outlast any test, with the default half-hour budget
 * for 429s: only a cancelled call returns in time.
 */
async function stuckDescribes(
  fault: 'rate-limit' | 'server-error' | 'hang',
  { from = 1, before, maxAttempts }: { from?: number; before?: 'server-error'; maxAttempts?: number } = {},
) {
  const fake = await FakeStorage.start({
    regions: REGIONS,
    fault: ({ method, attempt }) => (method !== 'DESCRIBE' ? undefined : attempt >= from ? fault : before),
  });
  const storage = new BunnyStorage({
    zone: fake.zone,
    password: fake.password,
    host: fake.url,
    timeoutMs: 60_000,
    retry: { ...DEFAULT_RETRY, baseDelayMs: 10_000, maxDelayMs: 10_000, ...(maxAttempts && { maxAttempts }) },
  });
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

  it("compares a manifest's 8-byte hash with the start of the stored SHA-256", () => {
    const sha = sha256Hex(new TextEncoder().encode('tile'));
    expect(checksumMatches(sha, sha.toLowerCase())).toBe(true);
    expect(checksumMatches(sha, sha.slice(0, 16))).toBe(true);
    expect(checksumMatches(sha, `${sha.slice(0, 15)}0`)).toBe(sha[15] === '0');
    // Too short to tell files apart.
    expect(checksumMatches(sha, sha.slice(0, 8))).toBe(false);
    expect(checksumMatches(sha, '')).toBe(false);
  });
});

describe('TileGateList', () => {
  it('keeps each file in 40 bytes and gives back its path and checksum', () => {
    const sha = sha256Hex(new TextEncoder().encode('tile'));
    const list = new TileGateList('v1');
    for (let i = 0; i < 3000; i++) {
      list.push(i * 1000, i === 7 ? sha.toLowerCase() : sha);
    }
    expect(list.length).toBe(3000);
    expect(list.tileId(2999)).toBe(2_999_000);
    expect(list.path(2)).toBe(tilePath('v1', 2000));
    expect(list.sha256(7)).toBe(sha);
    expect(TileGateList.BYTES_PER_FILE).toBe(40);
    expect(() => list.push(1, 'AB')).toThrow(/Not a hex SHA-256/);
  });

  it('gates millions of files within a small heap', async () => {
    // As objects with path and hex strings, 3M files took ~1.3 GB of heap; a 96 MB heap must do.
    const { stdout } = await promisify(execFile)(
      process.execPath,
      ['--max-old-space-size=96', '--import=tsx', 'test/node/gate-heap-fixture.ts', '3000000'],
      { cwd: TILES_DIR },
    );
    const result = JSON.parse(stdout) as { complete: boolean; passes: number; describes: number; heapUsed: number };
    expect(result).toMatchObject({ complete: true, passes: 2, describes: 4_500_000 });
    expect(result.heapUsed).toBeLessThan(96 * 2 ** 20);
  }, 120_000);
});

describe('normalizeRegions', () => {
  it('upper-cases, trims and lists each region once, in order', () => {
    expect(normalizeRegions([' uk', 'SYD', 'UK', '', 'syd ', 'ny'])).toEqual(['UK', 'SYD', 'NY']);
  });
});

describe('waitForReplication', () => {
  it('follows each region until it holds every file, re-checking only what is still missing', async () => {
    const { fake, storage } = await setup();
    const items = await upload(storage, 60);
    const hold = (paths: string[], region: string) =>
      paths.forEach((path) => fake.files.get(path)!.replicatedZones.add(region));
    // A third of the files are in every region from the start; the rest gain UK, NY and SYD, one after each pass.
    const ready = items.slice(0, 20).map((item) => item.path);
    const late = items.slice(20).map((item) => item.path);
    REGIONS.forEach((region) => hold(ready, region));
    const describesBefore = fake.requests.DESCRIBE ?? 0;
    const passes: Array<{ pending: number; describes: number; missingByRegion: Record<string, number> }> = [];
    let describesSoFar = describesBefore;
    const result = await waitForReplication(storage, items, REGIONS, {
      timeoutMs: 10_000,
      intervalMs: 1,
      onPass: ({ pass, pending, missingByRegion }) => {
        passes.push({ pending, describes: fake.requests.DESCRIBE - describesSoFar, missingByRegion });
        describesSoFar = fake.requests.DESCRIBE;
        if (pass <= REGIONS.length) {
          hold(late, REGIONS[pass - 1]);
        }
      },
    });

    expect(result).toMatchObject({
      complete: true,
      missing: [],
      mismatched: [],
      incomplete: 0,
      unchecked: 0,
      passes: 4,
    });
    expect(gateExitCode(result)).toBe(0);
    expect(result.missingByRegion).toEqual({ UK: 0, NY: 0, SYD: 0 });
    // Every file once, then only the 40 that some region still lacked.
    expect(passes).toEqual([
      { pending: 40, describes: 60, missingByRegion: { UK: 40, NY: 40, SYD: 40 } },
      { pending: 40, describes: 40, missingByRegion: { UK: 0, NY: 40, SYD: 40 } },
      { pending: 40, describes: 40, missingByRegion: { UK: 0, NY: 0, SYD: 40 } },
      { pending: 0, describes: 40, missingByRegion: { UK: 0, NY: 0, SYD: 0 } },
    ]);
    expect(fake.requests.DESCRIBE - describesBefore).toBe(180);
    expect(result.regionWaitMs.UK!).toBeLessThanOrEqual(result.regionWaitMs.NY!);
    expect(result.regionWaitMs.NY!).toBeLessThanOrEqual(result.regionWaitMs.SYD!);
  });

  it('needs one answer that names every region, not regions gathered over several', async () => {
    // UK on the first answer, then only NY (UK lost the file), then both.
    const answers = [['UK'], ['NY'], ['UK', 'NY']];
    const { storage, describes } = stubStorage((_, call) => ({ replicatedZones: answers[call - 1] ?? [] }));
    const result = await waitForReplication(storage, [{ path: 'r/a.mvt', sha256: 'AB'.repeat(32) }], ['UK', 'NY'], {
      timeoutMs: 10_000,
      intervalMs: 1,
    });
    expect(result).toMatchObject({ complete: true, passes: 3 });
    expect(describes).toHaveLength(3);
  });

  it('counts a region listed twice, in any case, once', async () => {
    const { fake, storage } = await setup();
    const items = await upload(storage, 4);
    fake.replicate();
    const result = await waitForReplication(storage, items, ['UK', 'uk', ' NY', 'SYD'], {
      timeoutMs: 5_000,
      intervalMs: 1,
    });
    expect(result).toMatchObject({ complete: true, passes: 1 });
    expect(result.missingByRegion).toEqual({ UK: 0, NY: 0, SYD: 0 });
    expect(Object.keys(result.regionWaitMs)).toEqual(['UK', 'NY', 'SYD']);
  });

  it('starts no DESCRIBE once the timeout has passed, and keeps the files it did not reach pending', async () => {
    const sha256 = 'AB'.repeat(32);
    const items = Array.from({ length: 200 }, (_, i) => ({ path: `r/${i}.mvt`, sha256 }));
    for (const regions of [REGIONS, []]) {
      // Every file is in every region, but a pass over them all takes about 2 s.
      const { storage, describes } = stubStorage(() => ({ replicatedZones: REGIONS }), 10);
      const result = await waitForReplication(storage, items, regions, {
        timeoutMs: 150,
        intervalMs: 1,
        concurrency: 1,
      });
      expect(describes.length).toBeGreaterThan(0);
      expect(describes.length).toBeLessThan(60);
      expect(result.passes).toBe(1);
      expect(result.waitedMs).toBeLessThan(1_000);
      // Unchecked files are not taken as replicated, even when no region is followed (storage only).
      expect(result).toMatchObject({ complete: false, missing: [], mismatched: [], errors: 0 });
      expect(result.incomplete).toBe(items.length - describes.length);
      // None of those was ever answered for: not known to be stored, so the CLI exits 1, not 2.
      expect(result.unchecked).toBe(items.length - describes.length);
      expect(gateExitCode(result)).toBe(1);
      expect(result.stragglers[0]).toEqual({ path: `r/${describes.length}.mvt`, regions });
    }
  });

  it('cancels a DESCRIBE backing off or in flight at the timeout, and keeps its file pending', async () => {
    for (const fault of ['rate-limit', 'server-error', 'hang'] as const) {
      const { fake, storage } = await stuckDescribes(fault);
      const started = performance.now();
      const result = await waitForReplication(storage, [{ path: 'r/0.mvt', sha256: 'AA' }], REGIONS, {
        timeoutMs: 300,
        intervalMs: 20,
      });
      // Not the 10 s back-off (or the 60 s request timeout), nor the 30 min the client would keep retrying 429s.
      expect(performance.now() - started).toBeLessThan(3_000);
      // The file got its fault, and no answer: pending, not an error, and not missing.
      expect(fake.faultsInjected[fault]).toBe(1);
      expect(result).toMatchObject({
        complete: false,
        incomplete: 1,
        unchecked: 1,
        errors: 0,
        missing: [],
        mismatched: [],
        passes: 1,
      });
      // The timeout came before the first answer: no error, but nothing says the file is stored either.
      expect(gateExitCode(result)).toBe(1);
      expect(result.stragglers).toEqual([{ path: 'r/0.mvt', regions: REGIONS }]);
      // Nothing is sent after the timeout.
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(fake.requests.DESCRIBE).toBe(1);
      expect(storage.inFlight).toBe(0);
    }
  });

  it("keeps a file's last outcome, an answer or a failure, when the timeout cancels its next DESCRIBE", async () => {
    // Shown missing on the first pass, then rate limited until the timeout.
    const missing = await stuckDescribes('rate-limit', { from: 2 });
    const shown = await waitForReplication(missing.storage, [{ path: 'r/0.mvt', sha256: 'AA' }], REGIONS, {
      timeoutMs: 300,
      intervalMs: 20,
    });
    expect(shown).toMatchObject({
      complete: false,
      missing: ['r/0.mvt'],
      incomplete: 0,
      unchecked: 0,
      errors: 0,
      passes: 2,
    });
    expect(missing.fake.requests.DESCRIBE).toBe(2);

    // Failed on the first pass (one attempt, so no back-off), then rate limited until the timeout: still an error.
    const failing = await stuckDescribes('rate-limit', { from: 2, before: 'server-error', maxAttempts: 1 });
    const failed = await waitForReplication(failing.storage, [{ path: 'r/0.mvt', sha256: 'AA' }], REGIONS, {
      timeoutMs: 300,
      intervalMs: 20,
    });
    expect(failed).toMatchObject({ complete: false, missing: [], incomplete: 1, unchecked: 1, errors: 1, passes: 2 });
    expect(failing.fake.faultsInjected).toEqual({ 'server-error': 1, 'rate-limit': 1 });
  });

  it('keeps a file no DESCRIBE answered for pending, even when no region is followed', async () => {
    const { fake } = await setup();
    // Every DESCRIBE fails (after its retries) with a server error: the file was never seen stored.
    const storage = new BunnyStorage({ zone: fake.zone, password: fake.password, host: fake.url, retry: fast });
    cleanup.push(() => storage.close());
    fake.options.fault = (request) => (request.method === 'DESCRIBE' ? 'server-error' : undefined);
    const result = await waitForReplication(storage, [{ path: 'r/0.mvt', sha256: 'AA' }], [], {
      timeoutMs: 300,
      intervalMs: 20,
    });
    expect(result).toMatchObject({ complete: false, incomplete: 1, errors: 1 });
  });

  it('with no grace, fails as soon as only missing and mismatched files are left', async () => {
    const { fake, storage } = await setup({ replicationDelayMs: { UK: 1, NY: 1, SYD: 1 } });
    const items = await upload(storage, 5);
    fake.seed('r/other.mvt', new TextEncoder().encode('other'));
    const result = await waitForReplication(
      storage,
      [...items, { path: 'r/none.mvt', sha256: 'AA' }, { path: 'r/other.mvt', sha256: items[0].sha256 }],
      REGIONS,
      { timeoutMs: 10_000, intervalMs: 20, mismatchGraceMs: 0 },
    );
    expect(result.complete).toBe(false);
    expect(result.missing).toEqual(['r/none.mvt']);
    expect(result.mismatched).toEqual(['r/other.mvt']);
    expect(result.incomplete).toBe(0);
    expect(result.waitedMs).toBeLessThan(5_000);
    expect(result.missingByRegion).toEqual({ UK: 2, NY: 2, SYD: 2 });
  });

  it('gives up on files that stay missing or mismatched once the grace has passed, not at the timeout', async () => {
    const { fake, storage } = await setup();
    fake.seed('r/other.mvt', new TextEncoder().encode('other'), REGIONS);
    const result = await waitForReplication(
      storage,
      [
        { path: 'r/none.mvt', sha256: 'AA' },
        { path: 'r/other.mvt', sha256: 'BB' },
      ],
      REGIONS,
      { timeoutMs: 20_000, intervalMs: 20, mismatchGraceMs: 300 },
    );
    expect(result).toMatchObject({ complete: false, missing: ['r/none.mvt'], mismatched: ['r/other.mvt'] });
    expect(result.passes).toBeGreaterThan(3);
    expect(result.waitedMs).toBeGreaterThanOrEqual(300);
    expect(result.waitedMs).toBeLessThan(5_000);
  });

  it('keeps asking about a file whose DESCRIBE lags an overwrite, and one stored late, until they are right', async () => {
    // Measured on Bunny: after two quick writes DESCRIBE can show the first version for a while.
    const { fake, storage } = await setup({
      staleOverwrite: { withinMs: 60_000, forMs: 400 },
      replicationDelayMs: { UK: 10, NY: 10, SYD: 10 },
    });
    const v1 = new TextEncoder().encode('v1');
    const v2 = new TextEncoder().encode('v2');
    fake.seed('r/a.mvt', v1, REGIONS);
    await storage.put('r/a.mvt', v2, sha256Hex(v2));
    expect((await storage.describe('r/a.mvt'))?.checksum).toBe(sha256Hex(v1));
    const late = setTimeout(() => fake.seed('r/late.mvt', v2, REGIONS), 200);
    cleanup.push(async () => clearTimeout(late));

    const result = await waitForReplication(
      storage,
      [
        { path: 'r/a.mvt', sha256: sha256Hex(v2) },
        { path: 'r/late.mvt', sha256: sha256Hex(v2) },
      ],
      REGIONS,
      { timeoutMs: 5_000, intervalMs: 50 },
    );
    expect(result).toMatchObject({ complete: true, missing: [], mismatched: [], incomplete: 0 });
    expect(result.passes).toBeGreaterThan(2);
    expect(result.waitedMs).toBeGreaterThanOrEqual(350);
  });

  it('fails at once on a fatal DESCRIBE error instead of waiting out the timeout', async () => {
    const { fake } = await setup();
    const storage = new BunnyStorage({ zone: fake.zone, password: 'rotated', host: fake.url, retry: fast });
    cleanup.push(() => storage.close());
    const started = performance.now();
    await expect(
      waitForReplication(storage, [{ path: 'r/0.mvt', sha256: 'AA' }], REGIONS, { timeoutMs: 20_000, intervalMs: 20 }),
    ).rejects.toThrow(/HTTP 401/);
    expect(performance.now() - started).toBeLessThan(5_000);
    await expect(checkReplication(storage, [{ path: 'r/0.mvt', sha256: 'AA' }], REGIONS)).rejects.toThrow(/HTTP 401/);
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
    // Shown stored with its bytes, just in no region: stored but not replicated, the CLI's exit 2.
    expect(result.unchecked).toBe(0);
    expect(gateExitCode(result)).toBe(2);
    expect(result.regionWaitMs).toEqual({ UK: null, NY: null, SYD: null });
    // The stuck file is named, with the regions it lacks.
    expect(result.stragglers).toEqual([{ path: items[1].path, regions: REGIONS }]);
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
    expect(fake.requests.DESCRIBE).toBe(items.length);
  });

  it('counts a region listed twice, in any case, once', async () => {
    const { fake, storage } = await setup();
    const items = await upload(storage, 4);
    fake.replicate('r/0', ['UK']);
    const report = await checkReplication(storage, items, ['UK', 'uk', 'SYD'], { concurrency: 2 });
    expect(report).toMatchObject({ checked: 4, replicated: 0, partial: 4 });
    expect(report.regions).toEqual({ UK: 1, SYD: 0 });
    expect(report.coverage).toEqual({ UK: 0.25, SYD: 0 });
  });
});

describe('checkReplication with a signal', () => {
  it('leaves out a DESCRIBE the signal cancelled mid-retry: not checked, not an error', async () => {
    const { fake, storage } = await stuckDescribes('rate-limit');
    const controller = new AbortController();
    const checking = checkReplication(storage, [{ path: tilePath('v1', 0), sha256: 'AA', tileId: 0 }], REGIONS, {
      signal: controller.signal,
    });
    await vi.waitFor(() => expect(storage.counters.rateLimited).toBe(1));
    const aborted = performance.now();
    controller.abort();
    const report = await checking;
    expect(performance.now() - aborted).toBeLessThan(1_000);
    expect(report).toMatchObject({ checked: 0, errors: 0, missing: 0, errorPaths: [], byZoom: {} });
    expect(fake.requests.DESCRIBE).toBe(1);
  });
});

describe('bunny:replication CLI', () => {
  /** Runs the CLI to its end; a nonzero exit is a result, a signal or spawn failure or a report not JSON an error. */
  async function run(
    fake: FakeStorage,
    args: string[],
    env: Record<string, string> = {},
  ): Promise<{ code: number; report: Record<string, any>; stderr: string }> {
    let code = 0;
    let stdout: string;
    let stderr: string;
    try {
      ({ stdout, stderr } = await promisify(execFile)(
        process.execPath,
        ['--import=tsx', 'src/bunny/cli/replication.ts', ...args],
        {
          cwd: TILES_DIR,
          maxBuffer: 64 * 2 ** 20,
          env: {
            ...process.env,
            BUNNY_STORAGE_ZONE: fake.zone,
            BUNNY_STORAGE_PASSWORD: fake.password,
            BUNNY_STORAGE_HOST: fake.url,
            // Pinned addresses exported in the shell would win over the fake's host.
            BUNNY_STORAGE_ADDRESSES: '127.0.0.1',
            ...env,
          },
        },
      ));
    } catch (error) {
      const failed = error as ExecFileException & { stdout?: string; stderr?: string };
      if (typeof failed.code !== 'number') {
        throw error;
      }
      code = failed.code;
      stdout = failed.stdout ?? '';
      stderr = failed.stderr ?? '';
    }
    return { code, report: stdout ? (JSON.parse(stdout) as Record<string, any>) : {}, stderr };
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
      claimsDir: join(scratch.dir, 'claims'),
      firstLoad: true,
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

    const waited = await run(fake, [
      ...firstZooms,
      '--wait',
      '--interval-s',
      '1',
      '--timeout-min',
      '1',
      '--mismatch-grace-s',
      '0',
    ]);
    expect(waited.code, waited.stderr).toBe(1);
    expect(waited.report).toMatchObject({ complete: false, files: 21, missing: [tilePath('itest', 9)] });

    // A range or sample with no position in the archive checks nothing: an error, not a pass, and no report.
    const describes = fake.requests.DESCRIBE;
    const beyond = [...common, '--from-id', String(positions + 100), '--to-id', String(positions + 200)];
    for (const extra of [[], ['--wait', '--timeout-min', '1'], ['--sample', '5']]) {
      const nothing = await run(fake, [...beyond, ...extra]);
      expect(nothing.code, nothing.stderr).toBe(1);
      expect(nothing.stderr).toMatch(/No file to check/);
      expect(nothing.report).toEqual({});
    }
    expect(fake.requests.DESCRIBE).toBe(describes);
  }, 60_000);

  it('checks a sample against the live manifest with --live, and every column with --listing', async () => {
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
      claimsDir: join(scratch.dir, 'claims'),
      firstLoad: true,
      storage: { zone: fake.zone, password: fake.password, host: fake.url, retry: fast },
      log: () => {},
    });
    const server = await startStateServer('state-zone', 'state-password');
    const store = new BunnyStateStore({ zone: 'state-zone', password: 'state-password', host: server.host });
    cleanup.push(
      () => server.close(),
      () => store.close(),
    );
    const manifest = join(scratch.dir, '20261118.tman.zst');
    await buildManifest(fixture.path, { out: manifest, buildKey: '20261118', threads: 2 });
    const live = await publishBuild(store, { buildKey: '20261118', manifestFile: manifest, prefix: 'itest' });
    const env = {
      BUNNY_STATE_ZONE: 'state-zone',
      BUNNY_STATE_PASSWORD: 'state-password',
      BUNNY_STATE_HOST: server.host,
    };
    // The fixture's header says zooms 0-15, as every archive buildArchive writes does; it holds zooms 0-5.
    const args = ['--live', '--regions', 'UK,SYD', '--sample', '30', '--per-zoom', '--seed', '3', '--max-zoom', '5'];

    fake.replicate('', ['UK']);
    const partial = await run(fake, args, env);
    expect(partial.code, partial.stderr).toBe(2);
    const checked = 1 + 4 + 16 + 30 * 3;
    expect(partial.report).toMatchObject({ prefix: 'itest', build: '20261118', checked, missing: 0, mismatched: 0 });
    expect(partial.report).toMatchObject({ notInArchive: 0, lacking: { UK: 0, SYD: checked } });

    // z0-2 are sampled whole, so these two are among the positions checked.
    fake.replicate();
    fake.seed(tilePath('itest', 3), new TextEncoder().encode('wrong'));
    fake.files.delete(tilePath('itest', 5));
    const broken = await run(fake, args, env);
    expect(broken.code, broken.stderr).toBe(1);
    expect(broken.report).toMatchObject({ missing: 1, mismatched: 1, lacking: { UK: 0, SYD: 0 } });
    expect(broken.report.mismatchedPaths).toEqual([tilePath('itest', 3)]);
    expect(broken.report.missingPaths).toEqual([tilePath('itest', 5)]);

    // Every column, the prefix taken from the state.
    const listed = await run(fake, ['--listing', '--regions', 'UK,SYD', '--max-zoom', '5'], env);
    expect(listed.code, listed.stderr).toBe(1);
    expect(listed.report).toMatchObject({ mode: 'listing', prefix: 'itest', expected: positions, missing: 1 });
    expect(listed.report.missingPaths).toEqual([tilePath('itest', 5)]);
    // The file just written over is in no region yet.
    expect(listed.report.held).toEqual({ UK: positions - 2, SYD: positions - 2 });
    expect(listed.report.byZoom['5']).toMatchObject({ columns: 32, expected: 1024, present: 1024, missing: 0 });

    // A pending build may have written anywhere it differs: nothing is checked until a sync finishes. Saying so
    // needs neither the live manifest (here missing) nor the tiles zone (here a host that does not resolve).
    const pending = [{ build: '20261119', manifest: live.manifest, positions, startedAt: live.updatedAt }];
    await writeState(store, { ...live, manifest: 'manifests/20261117.tman.zst', pending });
    const skipped = await run(fake, args, {
      ...env,
      BUNNY_STORAGE_HOST: 'http://tiles-storage.invalid',
      BUNNY_STORAGE_ADDRESSES: '',
    });
    expect(skipped.code, skipped.stderr).toBe(0);
    expect(skipped.report).toMatchObject({ prefix: 'itest', build: '20261118', mode: 'sample-per-zoom', seed: 3 });
    expect(skipped.report.skipped).toMatch(/pending builds 20261119/);
    // Nor the tiles zone's credentials: a job holding only the state zone's still gets its report.
    const noTileCredentials = { ...env, BUNNY_STORAGE_ZONE: '', BUNNY_STORAGE_PASSWORD: '' };
    const withoutCredentials = await run(fake, args, noTileCredentials);
    expect(withoutCredentials.code, withoutCredentials.stderr).toBe(0);
    expect(withoutCredentials.report.skipped).toMatch(/pending builds 20261119/);
    // A bad argument is still refused first, pending builds or not.
    const badAddresses = await run(fake, [...args, '--addresses', 'not-an-ip'], noTileCredentials);
    expect(badAddresses.code).toBe(3);
    expect(badAddresses.stderr).toMatch(/Storage addresses must be IP addresses/);
    // Once there is something to check, they are needed.
    await writeState(store, live);
    const unchecked = await run(fake, args, noTileCredentials);
    expect(unchecked.code).toBe(1);
    expect(unchecked.stderr).toMatch(/BUNNY_STORAGE_ZONE and BUNNY_STORAGE_PASSWORD must be set/);
    expect(unchecked.report).toEqual({});

    const usage = await run(fake, ['--live'], env);
    expect(usage.code).toBe(3);
    expect(usage.stderr).toMatch(/give --sample/);
  }, 60_000);

  it("--live samples the zooms the manifest's header or its positions give, not its position count", async () => {
    // A sparse build of zooms 3-5 only: every seventh position, far fewer positions than its last tile ID.
    const ids: number[] = [];
    for (let tileId = zoomStart(3); tileId < zoomEnd(5); tileId++) {
      if (tileId % 7 === 0) {
        ids.push(tileId);
      }
    }
    const { fake } = await setup({ regions: ['UK'] });
    const entries = ids.map((tileId) => {
      const bytes = new TextEncoder().encode(`sparse ${tileId}`);
      fake.seed(tilePath('sparse', tileId), bytes, ['UK']);
      return { tileId, runLength: 1, hash: sha256Hex(bytes).slice(0, 16) };
    });
    const scratch = await scratchDir();
    const server = await startStateServer('state-zone', 'state-password');
    const store = new BunnyStateStore({ zone: 'state-zone', password: 'state-password', host: server.host });
    cleanup.push(scratch.remove, server.close, () => store.close());
    const env = {
      BUNNY_STATE_ZONE: 'state-zone',
      BUNNY_STATE_PASSWORD: 'state-password',
      BUNNY_STATE_HOST: server.host,
    };
    const args = ['--live', '--regions', 'UK', '--sample', '2000', '--per-zoom', '--seed', '1'];
    // 2,000 per zoom takes every position of zooms 3-5: each one the build has is checked, the rest are not in it.
    const perZoom = (z: number) => ids.filter((tileId) => tileId >= zoomStart(z) && tileId < zoomEnd(z)).length;

    // With the zooms in the header (as the manifest CLI writes them), and without (only the positions say).
    for (const zooms of [{ minZoom: 3, maxZoom: 5 }, {}]) {
      const manifest = join(scratch.dir, `sparse-${Object.keys(zooms).length}.tman.zst`);
      await writeManifest(manifest, { buildKey: '20261118', ...zooms }, entries);
      await publishBuild(store, { buildKey: '20261118', manifestFile: manifest, prefix: 'sparse' });
      const result = await run(fake, args, env);
      expect(result.code, result.stderr).toBe(0);
      expect(result.report).toMatchObject({
        range: { from: zoomStart(3), to: zoomEnd(5) },
        checked: ids.length,
        replicated: ids.length,
        notInArchive: zoomEnd(5) - zoomStart(3) - ids.length,
      });
      expect(Object.keys(result.report.byZoom)).toEqual(['3', '4', '5']);
      expect(result.report.byZoom['5'].checked).toBe(perZoom(5));
    }

    // A manifest without zooms over no position says nothing to check.
    const empty = join(scratch.dir, 'empty.tman.zst');
    await writeManifest(empty, { buildKey: '20261118' }, []);
    await publishBuild(store, { buildKey: '20261118', manifestFile: empty, prefix: 'sparse' });
    const nothing = await run(fake, args, env);
    expect(nothing.code, nothing.stderr).toBe(1);
    expect(nothing.stderr).toMatch(/No file to check: the live build 20261118's manifest holds no position/);
  }, 60_000);

  it('refuses a --listing zoom range that would list nothing', async () => {
    const { fake } = await setup();
    for (const [args, message] of [
      // The maximum defaults to 15.
      [['--min-zoom', '16'], /--max-zoom 15 \(the default\) is below --min-zoom 16/],
      [['--min-zoom', '5', '--max-zoom', '3'], /--max-zoom 3 is below --min-zoom 5/],
      [['--max-zoom', '27'], /at most 26/],
    ] as const) {
      // With and without --prefix: the zooms are checked before state.json is read.
      for (const prefix of [['--prefix', 'itest'], []]) {
        const result = await run(fake, ['--listing', ...prefix, ...args]);
        expect(result.code, result.stderr).toBe(3);
        expect(result.stderr).toMatch(message);
        expect(result.report).toEqual({});
      }
    }
    expect(fake.requests).toEqual({});
  }, 30_000);

  it('with --listing counts only canonical file names, and a region listed twice once', async () => {
    const { fake } = await setup({ regions: ['UK', 'SYD'] });
    // z0 and z1: 5 positions, each in Sydney and all but one in the UK.
    for (let tileId = 0; tileId < zoomEnd(1); tileId++) {
      const regions = tileId === 3 ? ['SYD'] : ['UK', 'SYD'];
      fake.seed(tilePath('lt', tileId), new TextEncoder().encode(`t${tileId}`), regions);
    }
    const args = ['--listing', '--prefix', 'lt', '--max-zoom', '1', '--regions', 'UK,uk,SYD'];
    const lagging = await run(fake, args);
    expect(lagging.code, lagging.stderr).toBe(2);
    expect(lagging.report).toMatchObject({ regions: ['UK', 'SYD'], expected: 5, present: 5, missing: 0 });
    expect(lagging.report.held).toEqual({ UK: 4, SYD: 5 });
    expect(lagging.report.coverage).toEqual({ UK: 0.8, SYD: 1 });

    // Clients ask for the name tilePath() gives a position: a leading zero names another object.
    const canonical = tilePath('lt', 2);
    const alias = canonical.replace(/\/(\d+)\.mvt$/, '/0$1.mvt');
    fake.files.delete(canonical);
    fake.seed(alias, new TextEncoder().encode('t2'), ['UK', 'SYD']);
    const aliased = await run(fake, args);
    expect(aliased.code, aliased.stderr).toBe(1);
    expect(aliased.report).toMatchObject({ present: 4, missing: 1, missingPaths: [canonical] });
  });

  it('refuses arguments that would check nothing, or something other than asked', async () => {
    const { fake } = await setup();
    for (const [args, message] of [
      [['--listing', '--prefix', 'itest', '--from-id', '0', '--to-id', '5'], /--listing walks whole zooms/],
      [['--listing', '--prefix', 'itest', '--regions', ' , '], /--regions needs region codes/],
      [['--archive', 'none.pmtiles', '--sample', '0'], /--sample must be at least 1/],
      [['--archive', 'none.pmtiles', '--wait', '--timeout-min', '0'], /--timeout-min must be at least 1/],
      // Before the state zone is read (none is configured here), so a pending-build skip cannot hide it.
      [['--live', '--sample', '5', '--from-id', '9', '--to-id', '3'], /Empty range \[9, 3\)/],
    ] as const) {
      const result = await run(fake, [...args]);
      expect(result.code, result.stderr).toBe(3);
      expect(result.stderr).toMatch(message);
      expect(result.report).toEqual({});
    }
    expect(fake.requests).toEqual({});
  }, 30_000);

  it('with --wait exits 1 at once when the zone refuses the password', async () => {
    const scratch = await scratchDir();
    cleanup.push(scratch.remove);
    const fixture = await writeFixtureArchive(scratch.dir, 21, 3, 10);
    const { fake } = await setup();
    const args = ['--archive', fixture.path, '--from-id', '0', '--to-id', '5', '--wait', '--timeout-min', '1'];
    const result = await run(fake, args, { BUNNY_STORAGE_PASSWORD: 'rotated' });
    expect(result.code).toBe(1);
    expect(result.stderr).toMatch(/HTTP 401/);
  }, 20_000);
});
