import { ChildProcess, spawn } from 'node:child_process';
import { mkdirSync, PathLike, readFileSync, rmSync, utimesSync } from 'node:fs';
import { mkdir, readFile, rename, stat } from 'node:fs/promises';
import { join } from 'node:path';
import { Worker } from 'node:worker_threads';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { archiveIdentity, identifyArchive } from '../../src/bunny/archive-identity';
import { ArchiveRanges } from '../../src/bunny/archive-walk';
import { BunnyStateStore, writeState } from '../../src/bunny/state';
import { tilePath } from '../../src/bunny/tile-id';
import {
  Checkpoint,
  chunkProbeIds,
  MainMessage,
  preflightIds,
  runSeed,
  SeedOptions,
  SeedStatus,
} from '../../src/bunny/upload';
import { FakeStorage, FakeStorageOptions, Fault } from './fake-storage';
import { startStateServer } from './state-server';
import { FixtureArchive, random, scratchDir, withOtherTileBytes, writeFixtureArchive } from './upload-fixture';

/** Set by a test to hold back chosen renames (status.json is written to a temporary file and renamed into place). */
const renames = vi.hoisted(() => ({
  hook: undefined as ((from: string, to: string) => Promise<void> | undefined) | undefined,
}));
vi.mock('node:fs/promises', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs/promises')>();
  return {
    ...actual,
    rename: async (from: PathLike, to: PathLike) => {
      await renames.hook?.(String(from), String(to));
      return actual.rename(from, to);
    },
  };
});

const TILES_DIR = join(import.meta.dirname, '../..');
const fast = { baseDelayMs: 1, maxDelayMs: 5, settleMs: 5, maxAttempts: 5, maxRateLimitWaitMs: 5_000 };
let cleanup: (() => Promise<void> | void)[] = [];

afterEach(async () => {
  renames.hook = undefined;
  for (const step of cleanup.reverse()) {
    await step();
  }
  cleanup = [];
});

async function setup(positions: number, fakeOptions: FakeStorageOptions = {}) {
  const scratch = await scratchDir();
  cleanup.push(scratch.remove);
  const fixture = await writeFixtureArchive(scratch.dir, positions, 11, 300);
  const fake = await FakeStorage.start(fakeOptions);
  cleanup.push(() => fake.close());
  const options = (overrides: Partial<SeedOptions> = {}): SeedOptions => ({
    archivePath: fixture.path,
    prefix: 'itest',
    from: 0,
    to: positions,
    chunkSize: 512,
    workers: 3,
    concurrency: 24,
    stateDir: join(scratch.dir, 'state'),
    // Most tests here are of a first load's checks; those of the default say firstLoad: false.
    firstLoad: true,
    storage: { zone: fake.zone, password: fake.password, host: fake.url, retry: fast, timeoutMs: 300 },
    progressIntervalMs: 200,
    claimsDir: join(scratch.dir, 'claims'),
    log: () => {},
    ...overrides,
  });
  return { fixture, fake, options, dir: scratch.dir };
}

function expectStored(fake: FakeStorage, fixture: FixtureArchive, from: number, to: number, prefix = 'itest') {
  let wrong = 0;
  for (let id = from; id < to; id++) {
    if (fake.checksum(tilePath(prefix, id)) !== fixture.sha256(id)) {
      wrong++;
    }
  }
  expect(wrong).toBe(0);
  expect(fake.files.size).toBe(to - from);
}

const sleep = (ms: number) => new Promise((done) => setTimeout(done, Math.max(0, ms)));

async function until(condition: () => boolean, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) {
      throw new Error('timed out waiting');
    }
    await sleep(5);
  }
}

/** Faults on a share of first PUT attempts: every kind, stored or not. */
function faults(seed: number, share: number) {
  const next = random(seed);
  const kinds: Fault[] = ['rate-limit', 'server-error', 'stored-server-error', 'reset', 'stored-reset', 'stored-hang'];
  return ({ method, attempt }: { method: string; attempt: number }) =>
    method === 'PUT' && attempt === 1 && next() < share ? kinds[Math.floor(next() * kinds.length)] : undefined;
}

describe('runSeed', () => {
  it('uploads every position through faults, then has nothing left to do', async () => {
    const { fixture, fake, options } = await setup(5_461, {
      fault: faults(3, 0.05),
      latencyMs: () => Math.random() * 2,
    });
    const { status } = await runSeed(options());

    expect(status).toMatchObject({
      state: 'complete',
      error: null,
      chunks: { total: 11, done: 11, failed: 0 },
      files: { total: 5_461, done: 5_461, uploaded: 5_461, failed: 0 },
    });
    expectStored(fake, fixture, 0, 5_461);
    expect(fake.identicalReputs).toEqual([]);
    expect(Object.keys(fake.faultsInjected).length).toBe(6);
    expect(status.retries.ambiguousStored).toBeGreaterThan(0);
    expect(status.rateLimited).toBe(fake.faultsInjected['rate-limit']);
    expect(JSON.parse(await readFile(join(options().stateDir, 'status.json'), 'utf8'))).toMatchObject({
      state: 'complete',
    });

    const puts = fake.requests.PUT;
    const again = await runSeed(options());
    expect(again.status).toMatchObject({ state: 'complete', chunks: { previouslyDone: 11 }, workers: 0 });
    expect(fake.requests.PUT).toBe(puts);
    expect(fake.identicalReputs).toEqual([]);
  });

  it('keeps total PUTs in flight within --concurrency', async () => {
    const { fake, options } = await setup(5_461, { latencyMs: 3 });
    await runSeed(options({ workers: 4, concurrency: 20 }));
    expect(fake.maxInFlight).toBeLessThanOrEqual(20);
    expect(fake.maxInFlight).toBeGreaterThan(10);
  });

  it('keeps total PUTs in flight within --concurrency when it is below --workers', async () => {
    const { fake, options } = await setup(1_365, { latencyMs: 1 });
    const { status } = await runSeed(options({ workers: 6, concurrency: 2 }));
    expect(status.state).toBe('complete');
    expect(fake.maxInFlight).toBeLessThanOrEqual(2);
  });

  it('keeps connections per storage address within the per-address cap', async () => {
    const addresses = ['127.0.0.1', '127.0.0.2'];
    const { fake, options } = await setup(1_365, { latencyMs: 1, addresses });
    const { status } = await runSeed(
      options({
        workers: 3,
        concurrency: 100,
        maxConnectionsPerAddress: 4,
        // No preflight client: its connections would overlap the workers' on the server side.
        firstLoad: false,
        storage: { ...options().storage, addresses },
      }),
    );
    expect(status.state).toBe('complete');
    expect(status.concurrency).toBe(8);
    for (const address of addresses) {
      expect(fake.maxOpenByAddress.get(address)).toBeLessThanOrEqual(4);
    }
    expect(fake.maxInFlight).toBeLessThanOrEqual(8);
  });

  it('refuses to resume a state dir into another zone', async () => {
    const { options } = await setup(1_365);
    await runSeed(options());
    const other = await FakeStorage.start({ zone: 'other-zone' });
    cleanup.push(() => other.close());
    await expect(
      runSeed(options({ storage: { zone: other.zone, password: other.password, host: other.url, retry: fast } })),
    ).rejects.toThrow(/zone, host differ/);
    expect(other.files.size).toBe(0);
  });

  it('refuses a fresh state dir when an earlier run stored a narrow slice of the range', async () => {
    const { fake, options, dir } = await setup(5_461);
    await runSeed(options({ from: 3_000, to: 3_100, stateDir: join(dir, 'slice') }));
    const puts = fake.requests.PUT;
    await expect(runSeed(options())).rejects.toThrow(/already stored under itest/);
    expect(fake.requests.PUT).toBe(puts);
    expect(fake.identicalReputs).toEqual([]);
  });

  it('refuses a fresh state dir over a slice that holds only a chunk start, between the spread samples', async () => {
    // 21,845 positions: the spread samples are every 2nd id from 0. With 1,023 per chunk, chunk 3 starts at the odd
    // id 3,069, so only the chunk-start samples see a one-position slice there.
    const { fake, options, dir } = await setup(21_845);
    await runSeed(options({ from: 3_069, to: 3_070, chunkSize: 1_023, stateDir: join(dir, 'slice') }));
    expect(fake.files.size).toBe(1);
    await expect(runSeed(options({ chunkSize: 1_023 }))).rejects.toThrow(/itest\/\S+ is already stored/);
    expect(fake.requests.PUT).toBe(1);
  });

  it('DESCRIBEs every position unless told it is a first load, so a file outside every sample is not PUT again', async () => {
    const positions = 21_845;
    const { fixture, fake, options, dir } = await setup(positions);
    const chunkSize = 512;
    // A position neither a first load's fresh-seed check (every 2nd id here, and each chunk's first) nor its chunk's
    // probe looks at.
    const preflight = new Set(preflightIds(0, positions, chunkSize));
    const probed = (id: number) => {
      const start = Math.floor(id / chunkSize) * chunkSize;
      return chunkProbeIds(start, Math.min(positions, start + chunkSize)).includes(id);
    };
    const lone = [...Array(positions).keys()].find((id) => !preflight.has(id) && !probed(id))!;
    expect(lone).toBeGreaterThan(0);
    // Stored by an earlier seed, under two prefixes.
    for (const prefix of ['itest', 'first']) {
      await runSeed(options({ prefix, from: lone, to: lone + 1, stateDir: join(dir, `lone-${prefix}`) }));
    }
    expect(fake.files.size).toBe(2);

    const describes = fake.requests.DESCRIBE ?? 0;
    const logs: string[] = [];
    const { status } = await runSeed(
      options({ firstLoad: false, stateDir: join(dir, 'full'), log: (line) => logs.push(line) }),
    );
    expect(status).toMatchObject({ state: 'complete', files: { uploaded: positions - 1, skippedExisting: 1 } });
    expect(fake.requests.DESCRIBE - describes).toBe(positions);
    expect(fake.putsByPath.get(tilePath('itest', lone))).toBe(1);
    expect(fake.identicalReputs).toEqual([]);
    expect(logs.some((line) => /DESCRIBE before every PUT$/.test(line))).toBe(true);
    for (let id = 0; id < positions; id++) {
      expect(fake.checksum(tilePath('itest', id))).toBe(fixture.sha256(id));
    }

    // What --first-load trades for half the requests: its samples miss that file, so it is PUT again.
    const first = await runSeed(options({ prefix: 'first', stateDir: join(dir, 'first') }));
    expect(first.status).toMatchObject({ state: 'complete', chunks: { overlapping: 0 } });
    expect(fake.identicalReputs).toEqual([tilePath('first', lone)]);
  }, 60_000);

  it('verifies the chunks another seed stored part of when an interrupted seed resumes after it', async () => {
    const controller = new AbortController();
    let puts = 0;
    const { fixture, fake, options, dir } = await setup(5_461, {
      latencyMs: 2,
      onRequest: ({ method }) => {
        if (method === 'PUT' && ++puts === 1_000) {
          controller.abort();
        }
      },
    });
    const first = await runSeed(options({ signal: controller.signal, stateDir: join(dir, 'full') }));
    expect(first.status.state).toBe('interrupted');
    // Another seed (fresh state dir, so its preflight passes) uploads the tail, from inside chunk 5 of the first.
    const tail = await runSeed(options({ from: 3_000, stateDir: join(dir, 'tail') }));
    expect(tail.status.state).toBe('complete');

    const logs: string[] = [];
    const resumed = await runSeed(options({ stateDir: join(dir, 'full'), log: (line) => logs.push(line) }));
    expect(resumed.status.state).toBe('complete');
    expect(fake.identicalReputs).toEqual([]);
    expectStored(fake, fixture, 0, 5_461);
    // Chunks 6-10 were never started by the first run: each is found stored and verified instead of PUT blind.
    expect(resumed.status.chunks.overlapping).toBeGreaterThanOrEqual(5);
    expect(resumed.status.files.skippedExisting).toBeGreaterThanOrEqual(5_461 - 3_072);
    expect(logs.some((line) => /already stored with the same bytes/.test(line))).toBe(true);
  });

  it('refuses to start while another seed on this machine covers an overlapping range', async () => {
    let release!: () => void;
    const held = new Promise<void>((resolve) => (release = resolve));
    const { fixture, fake, options, dir } = await setup(5_461, {
      hold: ({ method }) => (method === 'PUT' ? held : undefined),
    });
    // Every seed started here is released and run out before the fake storage closes, whatever fails below.
    const started: Promise<unknown>[] = [];
    const seed = (overrides: Partial<SeedOptions>) => {
      const run = runSeed(options(overrides));
      started.push(run.catch(() => {}));
      return run;
    };
    const first = seed({ stateDir: join(dir, 'a') });
    let other: ReturnType<typeof runSeed> | undefined;
    let deadline: NodeJS.Timeout | undefined;
    try {
      await until(() => (fake.requests.PUT ?? 0) > 0);
      // Refused before any request. A seed that ran instead would wait on the held PUTs: 10 s, then it counts as not
      // refused.
      const overlapping = seed({ from: 3_000, stateDir: join(dir, 'b') });
      const stillRunning = new Promise((resolve) => (deadline = setTimeout(() => resolve('still running'), 10_000)));
      await expect(Promise.race([overlapping, stillRunning])).rejects.toThrow(
        /is seeding \[0, 5461\) of test-zone\/itest\/, which overlaps/,
      );
      // Another prefix does not overlap.
      other = seed({ prefix: 'other', to: 100, stateDir: join(dir, 'c') });
    } finally {
      clearTimeout(deadline);
      release();
      await Promise.all(started);
    }
    expect((await first).status.state).toBe('complete');
    expect((await other!).status.state).toBe('complete');
    expect(fake.files.size).toBe(5_461 + 100);
    for (let id = 0; id < 5_461; id++) {
      expect(fake.checksum(tilePath('itest', id))).toBe(fixture.sha256(id));
    }
    // Its claim is gone once it is done: the next seed runs (and finds everything stored).
    const again = await runSeed(options({ stateDir: join(dir, 'd'), firstLoad: false, to: 600 }));
    expect(again.status.files.skippedExisting).toBe(600);
  });

  it('waits for PUTs a stopped run left pending before verifying the chunks it left unfinished', async () => {
    const late = tilePath('itest', 100);
    const lateStoreMs = 1_500;
    const controller = new AbortController();
    let arrived = 0;
    const { fixture, fake, options } = await setup(1_365, {
      lateStoreMs,
      fault: ({ method, path, attempt }) => {
        if (method === 'PUT' && path === late && attempt === 1) {
          arrived = Date.now();
          // Stopped while put() waits out its settle (the PUT timed out after 300 ms; it DESCRIBEs at 1.8 s).
          setTimeout(() => controller.abort(), 600);
          return 'late-store';
        }
        return undefined;
      },
    });
    const storage = { ...options().storage, retry: { ...fast, settleMs: 300 } };
    const first = await runSeed(options({ storage, signal: controller.signal }));
    expect(first.status.state).toBe('interrupted');
    expect(fake.files.has(late)).toBe(false);

    const second = await runSeed(options({ storage }));
    expect(second.status.state).toBe('complete');
    await sleep(arrived + lateStoreMs + 300 - Date.now());
    expect(fake.putsByPath.get(late)).toBe(1);
    expect(fake.identicalReputs).toEqual([]);
    expectStored(fake, fixture, 0, 1_365);
  });

  it('stops cleanly, with the reason, when failures.jsonl cannot be written', async () => {
    const bad = tilePath('itest', 100);
    const { options, dir } = await setup(1_365, {
      fault: ({ method, path }) => (method === 'PUT' && path === bad ? 'server-error' : undefined),
    });
    const stateDir = join(dir, 'state');
    // A directory where the file goes: every append fails.
    await mkdir(join(stateDir, 'failures.jsonl'), { recursive: true });
    const { status, failures } = await runSeed(options({ stateDir }));
    expect(status.state).toBe('failed');
    expect(status.error).toMatch(/could not write .*failures\.jsonl/);
    expect(failures.map((failure) => failure.path)).toEqual([bad]);
    expect(JSON.parse(await readFile(join(stateDir, 'status.json'), 'utf8'))).toMatchObject({ state: 'failed' });
  });

  it('moves to the storage addresses a later lookup returns', async () => {
    const addresses = ['127.0.0.1', '127.0.0.2'];
    let moved = false;
    let puts = 0;
    const { fixture, fake, options } = await setup(5_461, {
      addresses,
      latencyMs: 1,
      // Partway through, the old address leaves DNS and stops answering.
      onRequest: ({ method }) => {
        if (method === 'PUT' && ++puts === 500) {
          moved = true;
        }
      },
      fault: ({ address }) => (moved && address === '127.0.0.1' ? 'hang' : undefined),
    });
    const logs: string[] = [];
    const { status } = await runSeed(
      options({
        addressRefreshMs: 50,
        resolveAddresses: async () => (moved ? ['127.0.0.2'] : ['127.0.0.1']),
        log: (line) => logs.push(line),
      }),
    );
    expect(status.state).toBe('complete');
    expect(status.addresses).toEqual(['127.0.0.2']);
    expect(fake.requestsByAddress.get('127.0.0.2')).toBeGreaterThan(1_000);
    expect(logs.some((line) => /Storage addresses changed/.test(line))).toBe(true);
    expectStored(fake, fixture, 0, 5_461);
    expect(fake.identicalReputs).toEqual([]);
  });

  it('keeps a worker busy while two of its chunks wait on one slow file each', async () => {
    const slow = new Set([
      tilePath('itest', 10),
      tilePath('itest', 70),
      tilePath('itest', 130),
      tilePath('itest', 190),
    ]);
    // The slow files are held until 2,000 others went out; with the workers stalled that never happens, and a
    // fallback releases them after 8 s instead.
    let releasedBy: string | undefined;
    let resolve!: () => void;
    const released = new Promise<void>((done) => (resolve = done));
    const release = (by: string) => {
      releasedBy ??= by;
      resolve();
    };
    let others = 0;
    const fallback = setTimeout(() => release('timeout'), 8_000);
    cleanup.push(() => clearTimeout(fallback));
    const { fake, fixture, options } = await setup(5_461, {
      hold: ({ method, path }) => (method === 'PUT' && slow.has(path) ? released : undefined),
      onRequest: ({ method, path }) => {
        if (method === 'PUT' && !slow.has(path) && ++others === 2_000) {
          release('progress');
        }
      },
    });
    const { status } = await runSeed(
      options({ chunkSize: 60, workers: 2, concurrency: 16, storage: { ...options().storage, timeoutMs: 20_000 } }),
    );
    expect(status.state).toBe('complete');
    expect(releasedBy).toBe('progress');
    expectStored(fake, fixture, 0, 5_461);
  });

  it('uploads only the requested id range', async () => {
    const { fixture, fake, options } = await setup(5_461);
    const { status } = await runSeed(options({ from: 1_000, to: 3_333, chunkSize: 700 }));
    expect(status.state).toBe('complete');
    expectStored(fake, fixture, 1_000, 3_333);
  });

  it('refuses a first load from a fresh state dir over files already stored, and verifies them without it', async () => {
    const { fixture, fake, options, dir } = await setup(1_365);
    await runSeed(options());
    const puts = fake.requests.PUT;

    await expect(runSeed(options({ stateDir: join(dir, 'other') }))).rejects.toThrow(
      /already stored under itest.*leave out --first-load/,
    );
    const verified = await runSeed(options({ stateDir: join(dir, 'third'), firstLoad: false }));
    expect(verified.status.files).toMatchObject({ uploaded: 0, skippedExisting: 1_365 });
    expect(fake.requests.PUT).toBe(puts);
    expect(fake.identicalReputs).toEqual([]);
    expectStored(fake, fixture, 0, 1_365);
  });

  it('resumes an interrupted seed by verifying the chunks it had started', async () => {
    const controller = new AbortController();
    let puts = 0;
    const { fixture, fake, options } = await setup(5_461, {
      latencyMs: 2,
      onRequest: ({ method }) => {
        if (method === 'PUT' && ++puts === 2_000) {
          controller.abort();
        }
      },
    });
    const first = await runSeed(options({ signal: controller.signal }));
    expect(first.status.state).toBe('interrupted');
    expect(first.status.chunks.done).toBeLessThan(11);
    const describesBefore = fake.requests.DESCRIBE ?? 0;

    const second = await runSeed(options());
    expect(second.status.state).toBe('complete');
    expect(second.status.files.skippedExisting).toBeGreaterThan(0);
    expect(fake.requests.DESCRIBE - describesBefore).toBeGreaterThan(0);
    expectStored(fake, fixture, 0, 5_461);
    expect(fake.identicalReputs).toEqual([]);
  });

  it('verifies a chunk it had started on resume, even where the per-chunk probe sees nothing stored', async () => {
    // One chunk; its first position never answers and positions 1-20 are stored, all between the probe's samples
    // (0, 22, 43, ...). Only the checkpoint says the chunk was started, so only it keeps the resume from PUTting
    // those 20 again.
    const controller = new AbortController();
    let blocked = true;
    const allowed = new Set(Array.from({ length: 20 }, (_, i) => tilePath('itest', i + 1)));
    const { fixture, fake, options } = await setup(1_365, {
      fault: ({ method, path }) => (blocked && method === 'PUT' && !allowed.has(path) ? 'hang' : undefined),
      onRequest: () => {
        if ([...allowed].every((path) => fake.files.has(path))) {
          controller.abort();
        }
      },
    });
    const first = await runSeed(options({ chunkSize: 2_048, workers: 1, concurrency: 32, signal: controller.signal }));
    expect(first.status.state).toBe('interrupted');
    expect(fake.files.size).toBe(20);

    blocked = false;
    const logs: string[] = [];
    const resumed = await runSeed(options({ chunkSize: 2_048, workers: 1, log: (line) => logs.push(line) }));
    expect(resumed.status.state).toBe('complete');
    expect(resumed.status.chunks.overlapping).toBe(0);
    expect(logs.some((line) => /already stored with the same bytes/.test(line))).toBe(false);
    expect(resumed.status.files.skippedExisting).toBe(20);
    expect(fake.identicalReputs).toEqual([]);
    expectStored(fake, fixture, 0, 1_365);
  });

  it('lists the failed files of a chunk a worker stopped on partway', async () => {
    const bad = (path: string) => Number(path.split('/').pop()!.split('.')[0]) < 64 && path.startsWith('itest/');
    const { options } = await setup(1_365, {
      fault: ({ method, path }) => (method === 'PUT' && bad(path) ? 'server-error' : undefined),
    });
    // Every position of z0-z5 fails; chunk 0 holds the first 512. The worker stops after 10 failures, before the chunk
    // is done: the 10th stops new work, and at most the 3 others of its 4 in flight fail after it.
    const { status, failures } = await runSeed(options({ workers: 1, concurrency: 4, maxFailures: 10 }));
    expect(status.state).toBe('failed');
    expect(status.files.failed).toBeGreaterThanOrEqual(10);
    expect(status.files.failed).toBeLessThanOrEqual(13);
    const lines = (await readFile(status.failuresFile, 'utf8')).trim().split('\n');
    expect(lines).toHaveLength(status.files.failed);
    expect(failures).toHaveLength(status.files.failed);
    expect(failures.every((failure) => bad(failure.path))).toBe(true);
    expect(lines.map((line) => (JSON.parse(line) as { path: string }).path).sort()).toEqual(
      failures.map((failure) => failure.path).sort(),
    );
    expect(status.chunks.failed).toBe(1);
  });

  it('stops, with the reason, once status.json can no longer be written', async () => {
    let puts = 0;
    let statusPath = '';
    const { fake, options } = await setup(5_461, {
      latencyMs: 1,
      onRequest: ({ method }) => {
        if (method === 'PUT' && ++puts === 500) {
          // A directory where the file goes: every later status write fails.
          for (;;) {
            rmSync(statusPath, { recursive: true, force: true });
            try {
              mkdirSync(statusPath);
              break;
            } catch {
              // A status write landed in between; again.
            }
          }
        }
      },
    });
    const opts = options({ progressIntervalMs: 50 });
    statusPath = join(opts.stateDir, 'status.json');
    const { status } = await runSeed(opts);
    expect(status.state).toBe('failed');
    expect(status.error).toMatch(/could not write .*status\.json/);
    expect(fake.files.size).toBeLessThan(5_461);
    expect(fake.identicalReputs).toEqual([]);
  });

  it('does not report complete when the last done line fails to reach the disk', async () => {
    const { fake, options } = await setup(1_365);
    const markDone = Checkpoint.prototype.markDone;
    const spy = vi.spyOn(Checkpoint.prototype, 'markDone').mockImplementation(function (this: Checkpoint, id: number) {
      if (id === 2) {
        // Fails after everything else is done, as a final fsync might.
        return new Promise<void>((_, reject) => setTimeout(() => reject(new Error('EIO: i/o error, fsync')), 300));
      }
      return markDone.call(this, id);
    });
    cleanup.push(() => spy.mockRestore());
    const { status } = await runSeed(options());
    expect(fake.files.size).toBe(1_365);
    expect(status.state).toBe('failed');
    expect(status.error).toMatch(/checkpoint write failed: EIO/);
  });

  it('checks nothing and uploads nothing when stopped before the fresh-seed check', async () => {
    const { fake, options } = await setup(5_461);
    const controller = new AbortController();
    controller.abort();
    const { status } = await runSeed(options({ signal: controller.signal }));
    expect(status.state).toBe('interrupted');
    expect(fake.requests).toEqual({});
  });

  it('fails files that keep failing, lists them, and repairs them on the next run', async () => {
    let broken = true;
    const bad = new Set([tilePath('itest', 100), tilePath('itest', 2_000)]);
    const { fixture, fake, options } = await setup(5_461, {
      fault: ({ method, path }) => (broken && method === 'PUT' && bad.has(path) ? 'server-error' : undefined),
    });
    const first = await runSeed(options());
    expect(first.status).toMatchObject({ state: 'failed', files: { failed: 2 }, chunks: { failed: 2, done: 9 } });
    expect(first.failures.map((failure) => failure.path).sort()).toEqual([...bad].sort());
    const lines = (await readFile(first.status.failuresFile, 'utf8')).trim().split('\n');
    expect(lines.map((line) => JSON.parse(line).path).sort()).toEqual([...bad].sort());

    broken = false;
    const second = await runSeed(options());
    expect(second.status).toMatchObject({ state: 'complete', chunks: { done: 11, previouslyDone: 9 } });
    expectStored(fake, fixture, 0, 5_461);
    expect(fake.identicalReputs).toEqual([]);
  });

  it('leaves the final status on disk when a progress write is still going as the seed ends', async () => {
    const { options } = await setup(1_365, { latencyMs: 1 });
    const opts = options({ progressIntervalMs: 20 });
    const statusPath = join(opts.stateDir, 'status.json');
    let running = 0;
    let heldUntil = 0;
    renames.hook = (from, to) => {
      // The first progress write (the second "running" status) lands only after the seed is long done.
      if (to === statusPath && readFileSync(from, 'utf8').includes('"state": "running"') && ++running === 2) {
        heldUntil = Date.now() + 3_000;
        return sleep(3_000);
      }
      return undefined;
    };
    const { status } = await runSeed(opts);
    expect(status.state).toBe('complete');
    expect(heldUntil).toBeGreaterThan(0);
    await sleep(heldUntil + 200 - Date.now());
    expect(JSON.parse(await readFile(statusPath, 'utf8'))).toMatchObject({ state: 'complete', error: null });
  });

  it('refuses an archive put at its path after its identity was computed, writing and uploading nothing', async () => {
    const { fixture, fake, options, dir } = await setup(1_365);
    const identified = await identifyArchive(fixture.path);
    // Same size, header and root directory; other tile bytes.
    const other = join(dir, 'other.pmtiles');
    await withOtherTileBytes(fixture.path, other);
    await rename(other, fixture.path);
    await expect(runSeed(options({ archiveIdentity: identified }))).rejects.toThrow(
      /no longer the file its identity was computed from/,
    );
    expect(fake.requests).toEqual({});
    await expect(stat(options().stateDir)).rejects.toThrow(/ENOENT/);
  });

  it('uploads nothing when the archive is replaced after the seed opened it, before its workers do', async () => {
    const { fixture, fake, options, dir } = await setup(1_365);
    const other = join(dir, 'other.pmtiles');
    await withOtherTileBytes(fixture.path, other);
    const { status } = await runSeed(
      options({
        // Called once the checkpoint is open and before any worker starts.
        resolveAddresses: async () => {
          await rename(other, fixture.path);
          return ['127.0.0.1'];
        },
      }),
    );
    expect(status.state).toBe('failed');
    expect(status.error).toMatch(/no longer the file its identity was computed from/);
    expect(fake.requests.PUT).toBeUndefined();
    expect(status.chunks.done).toBe(0);
    expect(await readFile(join(options().stateDir, 'checkpoint.log'), 'utf8')).not.toMatch(/^done /m);
  });

  it('records no chunk done once the archive is written to mid-seed, and a resume finishes it', async () => {
    let puts = 0;
    let path = '';
    const { fixture, fake, options } = await setup(5_461, {
      latencyMs: 1,
      onRequest: ({ method }) => {
        if (method === 'PUT' && ++puts === 1_500) {
          // Its bytes stay the same; its modification and change times don't.
          utimesSync(path, new Date(), new Date());
        }
      },
    });
    path = fixture.path;
    const first = await runSeed(options());
    expect(first.status.state).toBe('failed');
    expect(first.status.error).toMatch(/no longer the file its identity was computed from/);
    expect(first.status.chunks.done).toBeLessThan(11);
    const log = await readFile(join(options().stateDir, 'checkpoint.log'), 'utf8');
    expect(log.match(/^done /gm)?.length ?? 0).toBe(first.status.chunks.done);

    const resumed = await runSeed(options());
    expect(resumed.status.state).toBe('complete');
    expectStored(fake, fixture, 0, 5_461);
    expect(fake.identicalReputs).toEqual([]);
  });

  it('refuses a range that reaches past the tiles of the archive', async () => {
    const { fake, options } = await setup(1_365);
    await expect(runSeed(options({ from: 1_000, to: 1_400 }))).rejects.toThrow(
      /\[1000, 1400\) reaches past the tiles of .*, which has tiles in \[0, 1365\) only/,
    );
    expect(fake.requests).toEqual({});
    await expect(stat(options().stateDir)).rejects.toThrow(/ENOENT/);
  });

  it('does not record a chunk done when its worker stored fewer positions than it was handed out with', async () => {
    const { fixture, fake, options } = await setup(1_365);
    const post = Worker.prototype.postMessage;
    let shortened = 0;
    const spy = vi.spyOn(Worker.prototype, 'postMessage').mockImplementation(function (
      this: Worker,
      message: unknown,
      transfer?: readonly Transferable[],
    ) {
      const sent = message as MainMessage;
      if (sent.type === 'chunk' && sent.chunk.chunkId === 1) {
        // As a worker that lost a run would: the last run of chunk 1 never reaches it.
        shortened = sent.chunk.runs.at(-3)!;
        sent.chunk.runs = sent.chunk.runs.slice(0, -4);
      }
      return post.call(this, message, transfer as Transferable[]);
    });
    cleanup.push(() => spy.mockRestore());
    const first = await runSeed(options());
    spy.mockRestore();
    expect(shortened).toBeGreaterThan(0);
    expect(first.status.state).toBe('failed');
    expect(first.status.error).toMatch(/chunk 1 was reported done with \d+ positions stored, not the 512/);
    expect(await readFile(join(options().stateDir, 'checkpoint.log'), 'utf8')).not.toMatch(/^done 1$/m);

    const resumed = await runSeed(options());
    expect(resumed.status.state).toBe('complete');
    expectStored(fake, fixture, 0, 1_365);
    expect(fake.identicalReputs).toEqual([]);
  });

  it('stops on a wrong password before starting workers', async () => {
    const { fake, options } = await setup(1_365);
    await expect(
      runSeed(options({ storage: { zone: fake.zone, password: 'nope', host: fake.url, retry: fast } })),
    ).rejects.toThrow(/HTTP 401/);
    expect(fake.requests.PUT).toBeUndefined();
  });
});

interface ChildResult {
  code: number | null;
  signal: NodeJS.Signals | null;
  stdout: string;
  stderr: string;
}

function startSeed(
  fake: FakeStorage,
  args: string[],
  env: Record<string, string> = {},
): { child: ChildProcess; done: Promise<ChildResult> } {
  // These seeds write a zone no sync uses, unless a test gives the state zone.
  const stateCheck = 'BUNNY_STATE_ZONE' in env ? [] : ['--no-state-check'];
  const child = spawn(process.execPath, ['--import=tsx', 'src/bunny/cli/seed.ts', ...args, ...stateCheck], {
    cwd: TILES_DIR,
    env: {
      ...process.env,
      BUNNY_STORAGE_ZONE: fake.zone,
      BUNNY_STORAGE_PASSWORD: fake.password,
      BUNNY_STORAGE_HOST: fake.url,
      // An address exported where the tests run would send the PUTs elsewhere.
      BUNNY_STORAGE_ADDRESSES: '127.0.0.1',
      ...env,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '';
  let stderr = '';
  child.stdout!.on('data', (chunk) => (stdout += chunk));
  child.stderr!.on('data', (chunk) => (stderr += chunk));
  // On close, not exit: by then stdout and stderr have been read to the end.
  const done = new Promise<ChildResult>((resolve) =>
    child.once('close', (code, signal) => resolve({ code, signal, stdout, stderr })),
  );
  cleanup.push(() => {
    child.kill('SIGKILL');
  });
  return { child, done };
}

describe('bunny:seed CLI', () => {
  async function childSetup(stopAt: number, signal: NodeJS.Signals) {
    let child: ChildProcess | undefined;
    let puts = 0;
    const context = await setup(21_845, {
      latencyMs: () => 1 + Math.random() * 4,
      fault: faults(9, 0.02),
      onRequest: ({ method }) => {
        if (method === 'PUT' && ++puts === stopAt) {
          child?.kill(signal);
        }
      },
    });
    const stateDir = join(context.dir, 'state');
    const args = [
      '--archive',
      context.fixture.path,
      '--prefix',
      'itest',
      '--max-zoom',
      '7',
      '--chunk-size',
      '1024',
      '--workers',
      '3',
      '--concurrency',
      '48',
      '--state-dir',
      stateDir,
      '--request-timeout-s',
      '1',
      '--retry-base-ms',
      '5',
      '--settle-ms',
      '20',
      '--progress-interval-s',
      '1',
      // The first load as documented: chunks it has not started go out without a DESCRIBE per file.
      '--first-load',
    ];
    const run = () => {
      const started = startSeed(context.fake, args, { XDG_STATE_HOME: context.dir });
      child = started.child;
      return started.done;
    };
    return { ...context, stateDir, run };
  }

  it('survives kill -9 mid-upload and resumes without re-PUTting identical bytes', async () => {
    const { fake, fixture, stateDir, run } = await childSetup(8_000, 'SIGKILL');
    const killed = await run();
    expect(killed.signal).toBe('SIGKILL');
    const log = await readFile(join(stateDir, 'checkpoint.log'), 'utf8');
    expect(log).toMatch(/^start \d+$/m);
    expect(fake.files.size).toBeGreaterThanOrEqual(7_000);
    expect(fake.files.size).toBeLessThan(21_845);
    expect(fake.identicalReputs).toEqual([]);

    const describesBefore = fake.requests.DESCRIBE ?? 0;
    const resumed = await run();
    expect(resumed.code, resumed.stderr).toBe(0);
    const status = JSON.parse(resumed.stdout) as SeedStatus;
    expect(status).toMatchObject({ state: 'complete', files: { total: 21_845, done: 21_845, failed: 0 } });
    expect(status.files.skippedExisting).toBeGreaterThan(0);
    expect(fake.requests.DESCRIBE - describesBefore).toBeGreaterThan(0);
    expectStored(fake, fixture, 0, 21_845);
    expect(fake.identicalReputs).toEqual([]);
  }, 60_000);

  it('shuts down cleanly on SIGTERM and resumes', async () => {
    const { fake, fixture, stateDir, run } = await childSetup(6_000, 'SIGTERM');
    const stopped = await run();
    expect(stopped.code, stopped.stderr).toBe(130);
    expect(stopped.stderr).toMatch(/SIGTERM: finishing the PUTs in flight/);
    const status = JSON.parse(await readFile(join(stateDir, 'status.json'), 'utf8')) as SeedStatus;
    expect(status.state).toBe('interrupted');
    expect(status.inFlight).toBe(0);
    expect(fake.identicalReputs).toEqual([]);

    const resumed = await run();
    expect(resumed.code, resumed.stderr).toBe(0);
    expectStored(fake, fixture, 0, 21_845);
    expect(fake.identicalReputs).toEqual([]);
  }, 60_000);

  it('keeps the settle wait when --retry-base-ms is given, so a late commit is not PUT again', async () => {
    const scratch = await scratchDir();
    cleanup.push(scratch.remove);
    const fixture = await writeFixtureArchive(scratch.dir, 85, 5, 50);
    const late = tilePath('itest', 3);
    const lateStoreMs = 3_000;
    let arrived = 0;
    // Stored 3 s after it arrived: after the 1 s timeout (which fires within 1-1.5 s) plus a short back-off, but well
    // before the 10 s settle wait that follows a timeout.
    const fake = await FakeStorage.start({
      lateStoreMs,
      fault: ({ method, path, attempt }) => {
        if (method === 'PUT' && path === late && attempt === 1) {
          arrived = Date.now();
          return 'late-store';
        }
        return undefined;
      },
    });
    cleanup.push(() => fake.close());
    const result = await startSeed(
      fake,
      [
        '--archive',
        fixture.path,
        '--prefix',
        'itest',
        '--max-zoom',
        '3',
        '--state-dir',
        join(scratch.dir, 'state'),
        '--request-timeout-s',
        '1',
        '--retry-base-ms',
        '500',
        '--workers',
        '1',
        '--concurrency',
        '8',
      ],
      { XDG_STATE_HOME: scratch.dir },
    ).done;
    expect(result.code, result.stderr).toBe(0);
    // A second PUT would be seen as an identical re-PUT only once the late commit lands.
    await new Promise((done) => setTimeout(done, Math.max(0, arrived + lateStoreMs + 300 - Date.now())));
    expect(fake.putsByPath.get(late)).toBe(1);
    expect(fake.identicalReputs).toEqual([]);
    expectStored(fake, fixture, 0, 85);
  }, 40_000);

  it('after kill -9, waits for the PUTs that were in flight before verifying them', async () => {
    const scratch = await scratchDir();
    cleanup.push(scratch.remove);
    const fixture = await writeFixtureArchive(scratch.dir, 85, 5, 50);
    const late = tilePath('itest', 3);
    // Stored 3.5 s after it arrived, within the 2 s timeout + 5 x 400 ms settle that put() allows a server.
    const lateStoreMs = 3_500;
    let arrived = 0;
    const first: { child?: ChildProcess } = {};
    const fake = await FakeStorage.start({
      lateStoreMs,
      fault: ({ method, path, attempt }) => {
        if (method === 'PUT' && path === late && attempt === 1) {
          arrived = Date.now();
          setTimeout(() => first.child?.kill('SIGKILL'), 500);
          return 'late-store';
        }
        return undefined;
      },
    });
    cleanup.push(() => fake.close());
    const args = [
      '--archive',
      fixture.path,
      '--prefix',
      'itest',
      '--max-zoom',
      '3',
      '--state-dir',
      join(scratch.dir, 'state'),
      '--request-timeout-s',
      '2',
      '--settle-ms',
      '400',
      '--progress-interval-s',
      '1',
      '--workers',
      '1',
      '--concurrency',
      '8',
    ];
    const env = { XDG_STATE_HOME: scratch.dir };
    const killed = startSeed(fake, args, env);
    first.child = killed.child;
    expect((await killed.done).signal).toBe('SIGKILL');

    const resumed = await startSeed(fake, args, env).done;
    expect(resumed.code, resumed.stderr).toBe(0);
    expect(resumed.stderr).toMatch(/Waiting \d+ s before verifying/);
    await sleep(arrived + lateStoreMs + 300 - Date.now());
    expect(fake.putsByPath.get(late)).toBe(1);
    expect(fake.identicalReputs).toEqual([]);
    expectStored(fake, fixture, 0, 85);
  }, 40_000);

  it('after kill -9, waits for a PUT sent late in a status interval, past the window of the status before it', async () => {
    const scratch = await scratchDir();
    cleanup.push(scratch.remove);
    const fixture = await writeFixtureArchive(scratch.dir, 1_365, 5, 50);
    const stateDir = join(scratch.dir, 'state');
    const statusPath = join(stateDir, 'status.json');
    // Commit window: 2 s timeout + 5 x 400 ms settle. Status every 3 s.
    const commitWindowMs = 4_000;
    const intervalMs = 3_000;
    let late: string | undefined;
    let arrived = 0;
    const first: { child?: ChildProcess } = {};
    const fake = await FakeStorage.start({
      // Slow PUTs keep the upload going for a few seconds; DESCRIBEs answer at once.
      hold: ({ method }) => (method === 'PUT' ? sleep(20).then(() => {}) : undefined),
      fault: ({ method, path }) => {
        if (late !== undefined || method !== 'PUT' || !first.child) {
          return undefined;
        }
        let since: number;
        try {
          since = Date.now() - Date.parse((JSON.parse(readFileSync(statusPath, 'utf8')) as SeedStatus).updatedAt);
        } catch {
          return undefined;
        }
        // Over half an interval after the last status write, killed well before the next one.
        if (since < 0.55 * intervalMs || since > 0.75 * intervalMs) {
          return undefined;
        }
        late = path;
        arrived = Date.now();
        first.child.kill('SIGKILL');
        // Committed just inside the window counted from the PUT, well after the window counted from the status.
        fake.options.lateStoreMs = commitWindowMs - 100;
        return 'late-store';
      },
    });
    cleanup.push(() => fake.close());
    const args = [
      '--archive',
      fixture.path,
      '--prefix',
      'itest',
      '--max-zoom',
      '5',
      '--chunk-size',
      '128',
      '--state-dir',
      stateDir,
      '--request-timeout-s',
      '2',
      '--settle-ms',
      '400',
      '--progress-interval-s',
      String(intervalMs / 1000),
      '--workers',
      '1',
      '--concurrency',
      '8',
    ];
    const env = { XDG_STATE_HOME: scratch.dir };
    const killed = startSeed(fake, args, env);
    first.child = killed.child;
    expect((await killed.done).signal).toBe('SIGKILL');
    expect(late).toBeDefined();
    const last = JSON.parse(await readFile(statusPath, 'utf8')) as SeedStatus;
    expect(Date.parse(last.updatedAt) + commitWindowMs).toBeLessThan(arrived + commitWindowMs - 100);

    const resumed = await startSeed(fake, args, env).done;
    expect(resumed.code, resumed.stderr).toBe(0);
    await sleep(arrived + commitWindowMs + 300 - Date.now());
    expect(fake.putsByPath.get(late!)).toBe(1);
    expect(fake.identicalReputs).toEqual([]);
    expectStored(fake, fixture, 0, 1_365);
  }, 60_000);

  it('refuses to seed the live prefix with another archive than the live build', async () => {
    const { fake, fixture, dir } = await setup(85);
    const server = await startStateServer('state-zone', 'state-password');
    cleanup.push(() => server.close());
    const store = new BunnyStateStore({ zone: 'state-zone', password: 'state-password', host: server.host });
    cleanup.push(() => store.close());
    const identity = await archiveIdentity(fixture.path);
    // The live build's archive with one tile byte changed: same header, root directory and size.
    const other = join(dir, 'other.pmtiles');
    await withOtherTileBytes(fixture.path, other);
    const otherIdentity = await archiveIdentity(other);
    const live = {
      liveBuild: '20261118',
      manifest: 'manifests/20261118.tman.zst',
      prefix: 'itest',
      updatedAt: '2026-11-18T10:40:00.000Z',
      osmDate: null,
      positions: 85,
      archive: otherIdentity,
      pending: [],
    };
    await writeState(store, live);
    const env = {
      BUNNY_STATE_ZONE: 'state-zone',
      BUNNY_STATE_PASSWORD: 'state-password',
      BUNNY_STATE_HOST: server.host,
      XDG_STATE_HOME: dir,
    };
    const args = (prefix: string) => ['--archive', fixture.path, '--prefix', prefix, '--state-dir', join(dir, prefix)];

    const refused = await startSeed(fake, args('itest'), env).done;
    expect(refused.code).toBe(1);
    expect(refused.stderr).toMatch(/20261118 is live under itest\/ and this archive is not the one it came from/);
    expect(fake.requests.PUT ?? 0).toBe(0);
    // A v1 fingerprint in state.json (header, root directory and size) vouches for no archive, even a matching one.
    const ranges = await ArchiveRanges.open(fixture.path);
    await ranges.close();
    const legacyState = { ...live, archive: { size: ranges.size, fingerprint: ranges.legacyFingerprint } };
    server.objects.set('state.json', Buffer.from(JSON.stringify(legacyState)));
    const legacy = await startSeed(fake, args('itest'), env).done;
    expect(legacy.code).toBe(1);
    expect(legacy.stderr).toMatch(/records no v2 identity of its archive/);
    expect(fake.requests.PUT ?? 0).toBe(0);
    const missing = await startSeed(fake, args('itest'), { ...env, BUNNY_STATE_PASSWORD: '' }).done;
    expect(missing.code).toBe(2);
    expect(missing.stderr).toMatch(/--no-state-check/);

    // Another prefix (a full rewrite, loaded as a first load) is fine, and so is the live build's own archive (a
    // repair), but not as a first load: the live build's files are there.
    const rewrite = await startSeed(fake, [...args('itest/e2'), '--first-load'], env).done;
    expect(rewrite.code, rewrite.stderr).toBe(0);
    await writeState(store, { ...live, archive: identity });
    const puts = fake.requests.PUT;
    const firstLoad = await startSeed(fake, [...args('itest'), '--first-load'], env).done;
    expect(firstLoad.code).toBe(1);
    expect(firstLoad.stderr).toMatch(/20261118 is live under itest\/, so itest\/ is no first load/);
    expect(fake.requests.PUT).toBe(puts);
    const repair = await startSeed(fake, args('itest'), env).done;
    expect(repair.code, repair.stderr).toBe(0);
    for (const prefix of ['itest', 'itest/e2']) {
      expect([...Array(85).keys()].filter((id) => fake.checksum(tilePath(prefix, id)) !== fixture.sha256(id))).toEqual(
        [],
      );
    }
    expect(fake.files.size).toBe(170);
  }, 30_000);

  it('refuses ids past the archive, and cuts a zoom range to its tiles', async () => {
    const scratch = await scratchDir();
    cleanup.push(scratch.remove);
    // 100 positions: z0-z3 and the first 15 of z4's 256, which the header's zooms (0-15) reach far past.
    const fixture = await writeFixtureArchive(scratch.dir, 100, 5, 30);
    const fake = await FakeStorage.start();
    cleanup.push(() => fake.close());
    const stateDir = join(scratch.dir, 'state');
    const env = { XDG_STATE_HOME: scratch.dir };
    const base = ['--archive', fixture.path, '--prefix', 'itest', '--state-dir', stateDir, '--workers', '1'];

    const past = await startSeed(fake, [...base, '--from-id', '0', '--to-id', '101'], env).done;
    expect(past.code).toBe(2);
    expect(past.stderr).toMatch(/\[0, 101\) reaches past the tiles of .*, which has tiles in \[0, 100\) only/);
    expect(fake.requests).toEqual({});
    await expect(stat(stateDir)).rejects.toThrow(/ENOENT/);

    const zooms = await startSeed(fake, base, env).done;
    expect(zooms.code, zooms.stderr).toBe(0);
    expect(zooms.stderr).toMatch(/zooms' ids \[0, 1431655765\) cut to the archive's tiles: \[0, 100\)/);
    expect(JSON.parse(zooms.stdout)).toMatchObject({ state: 'complete', range: { from: 0, to: 100 } });
    expectStored(fake, fixture, 0, 100);
  }, 30_000);

  it('rejects bad arguments with exit code 2', async () => {
    const fake = await FakeStorage.start();
    cleanup.push(() => fake.close());
    const result = await startSeed(fake, ['--archive', 'x.pmtiles', '--max-zoom', '3', '--to-id', '9']).done;
    expect(result.code).toBe(2);
    expect(result.stderr).toMatch(/not both/);
    const both = await startSeed(fake, ['--archive', 'x.pmtiles', '--first-load', '--verify-existing']).done;
    expect(both.code).toBe(2);
    expect(both.stderr).toMatch(/either --first-load or --verify-existing \(the default\), not both/);
    // 0 would retry 429s in a tight loop, or turn off every request timeout.
    for (const flag of ['--retry-base-ms', '--request-timeout-s']) {
      const zero = await startSeed(fake, ['--archive', 'x.pmtiles', flag, '0']).done;
      expect(zero.code).toBe(2);
      expect(zero.stderr).toMatch(new RegExp(`${flag} must be an integer of at least 1, got 0`));
    }
  });
});
