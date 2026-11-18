import { ChildProcess, spawn } from 'node:child_process';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { tilePath } from '../../src/bunny/tile-id';
import { runSeed, SeedOptions, SeedStatus } from '../../src/bunny/upload';
import { FakeStorage, FakeStorageOptions, Fault } from './fake-storage';
import { FixtureArchive, random, scratchDir, writeFixtureArchive } from './upload-fixture';

const TILES_DIR = join(import.meta.dirname, '../..');
const fast = { baseDelayMs: 1, maxDelayMs: 5, settleMs: 5, maxAttempts: 5, maxRateLimitWaitMs: 5_000 };
let cleanup: (() => Promise<void> | void)[] = [];

afterEach(async () => {
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
    verifyExisting: false,
    storage: { zone: fake.zone, password: fake.password, host: fake.url, retry: fast, timeoutMs: 300 },
    progressIntervalMs: 200,
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
    // The ocean tile and the other repeats are read and hashed once per worker, not per position.
    expect(fixture.entries).toBeLessThan(5_461);

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

  it('uploads only the requested id range', async () => {
    const { fixture, fake, options } = await setup(5_461);
    const { status } = await runSeed(options({ from: 1_000, to: 3_333, chunkSize: 700 }));
    expect(status.state).toBe('complete');
    expectStored(fake, fixture, 1_000, 3_333);
  });

  it('refuses a fresh state dir over files already stored, unless told to verify', async () => {
    const { fixture, fake, options, dir } = await setup(1_365);
    await runSeed(options());
    const puts = fake.requests.PUT;

    await expect(runSeed(options({ stateDir: join(dir, 'other') }))).rejects.toThrow(/already stored under itest/);
    const verified = await runSeed(options({ stateDir: join(dir, 'third'), verifyExisting: true }));
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

function startSeed(fake: FakeStorage, args: string[]): { child: ChildProcess; done: Promise<ChildResult> } {
  const child = spawn(process.execPath, ['--import=tsx', 'src/bunny/cli/seed.ts', ...args], {
    cwd: TILES_DIR,
    env: {
      ...process.env,
      BUNNY_STORAGE_ZONE: fake.zone,
      BUNNY_STORAGE_PASSWORD: fake.password,
      BUNNY_STORAGE_HOST: fake.url,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '';
  let stderr = '';
  child.stdout!.on('data', (chunk) => (stdout += chunk));
  child.stderr!.on('data', (chunk) => (stderr += chunk));
  const done = new Promise<ChildResult>((resolve) =>
    child.once('exit', (code, signal) => resolve({ code, signal, stdout, stderr })),
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
      '--progress-interval-s',
      '1',
    ];
    const run = () => {
      const started = startSeed(context.fake, args);
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

  it('rejects bad arguments with exit code 2', async () => {
    const fake = await FakeStorage.start();
    cleanup.push(() => fake.close());
    const result = await startSeed(fake, ['--archive', 'x.pmtiles', '--max-zoom', '3', '--to-id', '9']).done;
    expect(result.code).toBe(2);
    expect(result.stderr).toMatch(/not both/);
  });
});
