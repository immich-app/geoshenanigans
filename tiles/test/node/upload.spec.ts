import { appendFile, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ArchiveRanges } from '../../src/bunny/archive-walk';
import { BunnyStorage } from '../../src/bunny/storage';
import { tilePath } from '../../src/bunny/tile-id';
import { Checkpoint, planChunks, SeedParams, UploadItem, uploadTiles } from '../../src/bunny/upload';
import { FakeStorage, FakeStorageOptions, Fault, sha256Hex } from './fake-storage';
import { random, scratchDir, writeFixtureArchive } from './upload-fixture';

const fast = { baseDelayMs: 1, maxDelayMs: 5, settleMs: 5, maxAttempts: 5, maxRateLimitWaitMs: 5_000 };
let cleanup: (() => Promise<void>)[] = [];

afterEach(async () => {
  for (const step of cleanup.reverse()) {
    await step();
  }
  cleanup = [];
});

async function setup(fakeOptions: FakeStorageOptions = {}, password?: string) {
  const fake = await FakeStorage.start(fakeOptions);
  const storage = new BunnyStorage({
    zone: fake.zone,
    password: password ?? fake.password,
    host: fake.url,
    retry: fast,
    timeoutMs: 200,
    concurrency: 32,
  });
  cleanup.push(
    () => fake.close(),
    () => storage.close(),
  );
  return { fake, storage };
}

function items(count: number, salt = ''): UploadItem[] {
  return Array.from({ length: count }, (_, tileId) => {
    const bytes = new TextEncoder().encode(`tile ${tileId}${salt}`);
    return { tileId, bytes, sha256: sha256Hex(bytes) };
  });
}

/** Random faults on a share of first attempts, of every kind; later attempts mostly succeed. */
function randomFaults(seed: number, share = 0.3) {
  const next = random(seed);
  const kinds: Fault[] = ['rate-limit', 'server-error', 'stored-server-error', 'reset', 'stored-reset', 'stored-hang'];
  return ({ method, attempt }: { method: string; attempt: number }) =>
    method === 'PUT' && next() < share / attempt ? kinds[Math.floor(next() * kinds.length)] : undefined;
}

describe('uploadTiles', () => {
  it('stores every item through every kind of fault without re-PUTting identical bytes', async () => {
    const { fake, storage } = await setup({ fault: randomFaults(7), latencyMs: () => Math.random() * 3 });
    const all = items(1_500);
    const stats = await uploadTiles(storage, all, { prefix: 'p', concurrency: 32 });

    expect(stats).toMatchObject({ uploaded: 1_500, skippedExisting: 0, failed: 0, stopped: false });
    expect(stats.bytes).toBe(all.reduce((sum, item) => sum + item.bytes.length, 0));
    for (const item of all) {
      expect(fake.checksum(tilePath('p', item.tileId))).toBe(item.sha256);
    }
    for (const kind of ['rate-limit', 'server-error', 'stored-server-error', 'reset', 'stored-reset', 'stored-hang']) {
      expect(fake.faultsInjected[kind as Fault]).toBeGreaterThan(5);
    }
    expect(stats.rateLimited).toBe(fake.faultsInjected['rate-limit']);
    expect(storage.counters.timeouts).toBeGreaterThan(0);
    expect(storage.counters.ambiguousStored).toBeGreaterThan(0);
    expect(fake.identicalReputs).toEqual([]);
  });

  it('in verify mode skips files already stored with the same bytes and replaces the rest', async () => {
    const { fake, storage } = await setup();
    const all = items(30);
    all.slice(0, 10).forEach((item) => fake.seed(tilePath('v1', item.tileId), item.bytes));
    all.slice(10, 20).forEach((item) => fake.seed(tilePath('v1', item.tileId), new TextEncoder().encode('stale')));

    const stats = await uploadTiles(storage, all, { verify: true });
    expect(stats).toMatchObject({ uploaded: 20, skippedExisting: 10, failed: 0 });
    expect(fake.requests).toEqual({ DESCRIBE: 30, PUT: 20 });
    for (const item of all) {
      expect(fake.checksum(tilePath('v1', item.tileId))).toBe(item.sha256);
    }
    expect(fake.identicalReputs).toEqual([]);
  });

  it('verifies only the items that ask for it', async () => {
    const { fake, storage } = await setup();
    const all = items(4).map((item, i) => ({ ...item, verify: i % 2 === 0 }));
    await uploadTiles(storage, all);
    expect(fake.requests).toEqual({ DESCRIBE: 2, PUT: 4 });
  });

  it('records a file that keeps failing and carries on with the rest', async () => {
    const { fake, storage } = await setup({
      fault: ({ method, path }) => (method === 'PUT' && path === tilePath('v1', 3) ? 'server-error' : undefined),
    });
    const settled: string[] = [];
    const stats = await uploadTiles(storage, items(10), {
      onSettled: (item, outcome) => settled.push(`${item.tileId}:${outcome}`),
    });
    expect(stats).toMatchObject({ uploaded: 9, failed: 1, stopped: false });
    expect(stats.failures).toEqual([
      { tileId: 3, path: tilePath('v1', 3), error: expect.stringContaining('failed 5 times') },
    ]);
    expect(settled).toContain('3:failed');
    expect(fake.files.size).toBe(9);
  });

  it('stops after maxFailures', async () => {
    const { storage } = await setup({ fault: ({ method }) => (method === 'PUT' ? 'server-error' : undefined) });
    const stats = await uploadTiles(storage, items(1_000), { maxFailures: 3, concurrency: 1 });
    expect(stats.failed).toBe(3);
    expect(stats.stopped).toBe(true);
    expect(stats.error).toMatch(/3 files failed/);
  });

  it('stops at once on a fatal error', async () => {
    const { fake, storage } = await setup({}, 'wrong');
    const stats = await uploadTiles(storage, items(1_000), { concurrency: 4 });
    expect(stats.stopped).toBe(true);
    expect(stats.failed).toBeLessThanOrEqual(4);
    expect(stats.error).toMatch(/HTTP 401/);
    expect(fake.files.size).toBe(0);
  });

  it('stops taking items when the signal aborts and lets the ones in flight finish', async () => {
    const { fake, storage } = await setup({ latencyMs: 5 });
    const controller = new AbortController();
    let settled = 0;
    const stats = await uploadTiles(storage, items(10_000), {
      concurrency: 8,
      signal: controller.signal,
      onSettled: () => {
        if (++settled === 50) {
          controller.abort();
        }
      },
    });
    expect(stats.stopped).toBe(true);
    expect(stats.uploaded).toBeGreaterThanOrEqual(50);
    expect(stats.uploaded).toBeLessThan(70);
    expect(fake.files.size).toBe(stats.uploaded);
  });

  it('takes async sources', async () => {
    const { fake, storage } = await setup();
    async function* source() {
      yield* items(5);
    }
    expect((await uploadTiles(storage, source())).uploaded).toBe(5);
    expect(fake.files.size).toBe(5);
  });
});

describe('Checkpoint', () => {
  const params: SeedParams = { prefix: 'v1', from: 0, to: 100, chunkSize: 10, archive: { size: 1, fingerprint: 'f' } };
  let dir: string;
  beforeEach(async () => {
    const scratch = await scratchDir();
    dir = scratch.dir;
    cleanup.push(scratch.remove);
  });

  it('remembers started and done chunks', async () => {
    const first = await Checkpoint.open(dir, params);
    expect(first.fresh).toBe(true);
    await first.markStarted([0, 1, 2]);
    await first.markDone(1);
    await first.close();

    const second = await Checkpoint.open(dir, params);
    expect(second.fresh).toBe(false);
    expect([...second.started].sort()).toEqual([0, 1, 2]);
    expect([...second.done]).toEqual([1]);
    await second.close();
    expect(await readFile(join(dir, 'checkpoint.log'), 'utf8')).toBe('start 0\nstart 1\nstart 2\ndone 1\n');
  });

  it('drops a torn last line and keeps appending cleanly', async () => {
    const first = await Checkpoint.open(dir, params);
    await first.markStarted([4]);
    await first.close();
    await appendFile(join(dir, 'checkpoint.log'), 'done 4');

    const second = await Checkpoint.open(dir, params);
    expect(second.done.size).toBe(0);
    await second.markStarted([12]);
    await second.close();
    expect(await readFile(join(dir, 'checkpoint.log'), 'utf8')).toBe('start 4\nstart 12\n');
  });

  it('refuses a state dir another live process holds, and takes over a stale lock', async () => {
    await writeFile(join(dir, 'seed.lock'), `${process.ppid}\n`);
    await expect(Checkpoint.open(dir, params)).rejects.toThrow(/in use by process/);
    await writeFile(join(dir, 'seed.lock'), '2147483646\n');
    const checkpoint = await Checkpoint.open(dir, params);
    expect(await readFile(join(dir, 'seed.lock'), 'utf8')).toBe(`${process.pid}\n`);
    await checkpoint.close();
    await expect(readFile(join(dir, 'seed.lock'))).rejects.toThrow(/ENOENT/);
  });

  it('refuses a state dir of another seed or a damaged log', async () => {
    await (await Checkpoint.open(dir, params)).close();
    await expect(Checkpoint.open(dir, { ...params, to: 200 })).rejects.toThrow(/to differ/);
    await expect(Checkpoint.open(dir, { ...params, archive: { size: 1, fingerprint: 'g' } })).rejects.toThrow(
      /archive differ/,
    );
    await writeFile(join(dir, 'checkpoint.log'), 'start 1\nbogus\n');
    await expect(Checkpoint.open(dir, params)).rejects.toThrow(/unreadable line/);
  });
});

describe('planChunks', () => {
  it('covers every position of a range exactly once, in order, clipped to chunks', async () => {
    const scratch = await scratchDir();
    cleanup.push(scratch.remove);
    const fixture = await writeFixtureArchive(scratch.dir, 5_461, 3, 50);
    const ranges = await ArchiveRanges.open(fixture.path);
    cleanup.push(() => ranges.close());

    for (const [from, to, size] of [
      [0, 5_461, 256],
      [1_000, 4_321, 1_000],
      [5_000, 5_461, 7],
    ]) {
      const seen: number[] = [];
      let lastChunk = -1;
      for await (const chunk of planChunks(ranges, from, to, size)) {
        expect(chunk.chunkId).toBe(lastChunk === -1 ? Math.floor(from / size) : lastChunk + 1);
        lastChunk = chunk.chunkId;
        expect(chunk.start).toBe(Math.max(from, chunk.chunkId * size));
        for (let i = 0; i < chunk.runs.length; i += 4) {
          const [tileId, runLength, offset, length] = chunk.runs.subarray(i, i + 4);
          expect(tileId).toBeGreaterThanOrEqual(chunk.start);
          expect(tileId + runLength).toBeLessThanOrEqual(chunk.end);
          const bytes = new Uint8Array(await ranges.archive.readTileData(offset, length));
          for (let id = tileId; id < tileId + runLength; id++) {
            seen.push(id);
            expect(sha256Hex(bytes)).toBe(fixture.sha256(id));
          }
        }
      }
      expect(seen).toEqual(Array.from({ length: to - from }, (_, i) => from + i));
    }
  });

  it('skips the chunks it is told to', async () => {
    const scratch = await scratchDir();
    cleanup.push(scratch.remove);
    const fixture = await writeFixtureArchive(scratch.dir, 1_365, 4, 50);
    const ranges = await ArchiveRanges.open(fixture.path);
    cleanup.push(() => ranges.close());
    const ids: number[] = [];
    for await (const chunk of planChunks(ranges, 0, 1_365, 100, (id) => id % 2 === 1)) {
      ids.push(chunk.chunkId);
    }
    expect(ids).toEqual([0, 2, 4, 6, 8, 10, 12]);
  });

  it('finds the run of single ids', async () => {
    const scratch = await scratchDir();
    cleanup.push(scratch.remove);
    const fixture = await writeFixtureArchive(scratch.dir, 1_365, 5, 20);
    const ranges = await ArchiveRanges.open(fixture.path);
    cleanup.push(() => ranges.close());
    for (const id of [0, 1, 77, 500, 1_364]) {
      const run = (await ranges.find(id))!;
      expect(id).toBeGreaterThanOrEqual(run.tileId);
      expect(id).toBeLessThan(run.tileId + run.runLength);
      expect(sha256Hex(new Uint8Array(await ranges.archive.readTileData(run.offset, run.length)))).toBe(
        fixture.sha256(id),
      );
    }
    expect(await ranges.find(1_365)).toBeNull();
  });
});
