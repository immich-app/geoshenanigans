import { appendFile, FileHandle, open, readFile, utimes, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ArchiveRanges } from '../../src/bunny/archive-walk';
import { BunnyStorage } from '../../src/bunny/storage';
import { tilePath } from '../../src/bunny/tile-id';
import {
  Checkpoint,
  chunkProbeIds,
  LOCK_SETTLE_MS,
  planChunks,
  preflightIds,
  SeedParams,
  UploadItem,
  uploadTiles,
} from '../../src/bunny/upload';
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
    // A file with another checksum is fetched and hashed before it is replaced (DESCRIBE can lag an overwrite).
    expect(fake.requests).toEqual({ DESCRIBE: 30, GET: 10, PUT: 20 });
    for (const item of all) {
      expect(fake.checksum(tilePath('v1', item.tileId))).toBe(item.sha256);
    }
    expect(fake.identicalReputs).toEqual([]);
  });

  it('in verify mode does not PUT bytes already stored when DESCRIBE still shows the version they overwrote', async () => {
    const { fake, storage } = await setup({ staleOverwrite: { withinMs: 60_000, forMs: Infinity } });
    const all = items(10);
    for (const item of all) {
      fake.seed(tilePath('v1', item.tileId), new TextEncoder().encode(`older ${item.tileId}`));
    }
    await uploadTiles(storage, all.slice(0, 5), { prefix: 'v1' });
    const puts = fake.requests.PUT;

    const stats = await uploadTiles(storage, all, { verify: true });
    expect(stats).toMatchObject({ uploaded: 5, skippedExisting: 5, failed: 0 });
    expect(fake.requests.PUT - puts).toBe(5);
    expect(storage.counters.staleDescribes).toBe(5);
    expect(fake.identicalReputs).toEqual([]);
  });

  it('verifies only the items that ask for it', async () => {
    const { fake, storage } = await setup();
    const all = items(4).map((item, i) => ({ ...item, verify: i % 2 === 0 }));
    await uploadTiles(storage, all);
    expect(fake.requests).toEqual({ DESCRIBE: 2, PUT: 4 });
  });

  it('verifies every item when told to, even items that say verify: false', async () => {
    const { fake, storage } = await setup();
    const all = items(4).map((item) => ({ ...item, verify: false }));
    fake.seed(tilePath('v1', 0), all[0].bytes);
    const stats = await uploadTiles(storage, all, { verify: true });
    expect(stats).toMatchObject({ uploaded: 3, skippedExisting: 1 });
    expect(fake.identicalReputs).toEqual([]);
  });

  it('does not count a file the storage client stopped on as failed', async () => {
    const controller = new AbortController();
    const fake = await FakeStorage.start({ fault: ({ method }) => (method === 'PUT' ? 'rate-limit' : undefined) });
    const storage = new BunnyStorage({
      zone: fake.zone,
      password: fake.password,
      host: fake.url,
      retry: { ...fast, baseDelayMs: 5_000, maxDelayMs: 5_000, maxRateLimitWaitMs: 60_000 },
      signal: controller.signal,
    });
    cleanup.push(
      () => fake.close(),
      () => storage.close(),
    );
    setTimeout(() => controller.abort(), 50);
    const settled: string[] = [];
    const stats = await uploadTiles(storage, items(10), {
      concurrency: 4,
      signal: controller.signal,
      onSettled: (item, outcome) => settled.push(`${item.tileId}:${outcome}`),
    });
    expect(stats).toMatchObject({ uploaded: 0, failed: 0, stopped: true });
    expect(settled).toEqual([]);
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
  const params: SeedParams = {
    zone: 'zone-a',
    host: 'https://storage.example.test',
    prefix: 'v1',
    from: 0,
    to: 100,
    chunkSize: 10,
    archive: { size: 1, fingerprint: 'f' },
  };
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

  /** Makes FileHandle.write take at most `limit(call)` bytes per call (a string counts as its UTF-8 bytes). */
  async function shortWrites(limit: (call: number) => number | Error) {
    const handle = await open(join(dir, 'probe'), 'w');
    await handle.close();
    const prototype = Object.getPrototypeOf(handle) as FileHandle;
    const write = prototype.write as (...args: unknown[]) => ReturnType<FileHandle['write']>;
    let calls = 0;
    const spy = vi.spyOn(prototype, 'write').mockImplementation(function (this: FileHandle, ...args: unknown[]) {
      const data = typeof args[0] === 'string' ? Buffer.from(args[0]) : (args[0] as Uint8Array);
      const offset = typeof args[0] === 'string' ? 0 : ((args[1] as number | undefined) ?? 0);
      const length =
        typeof args[0] === 'string' ? data.length : ((args[2] as number | undefined) ?? data.length - offset);
      const take = limit(++calls);
      if (take instanceof Error) {
        return Promise.reject(take);
      }
      return write.call(this, data, offset, Math.min(take, length));
    });
    cleanup.push(async () => spy.mockRestore());
    return spy;
  }

  it('finishes a line the disk took only part of', async () => {
    const checkpoint = await Checkpoint.open(dir, params);
    // The first write(2) takes 7 bytes only, as on a nearly full disk; the rest goes in later calls.
    const spy = await shortWrites((call) => (call === 1 ? 7 : Infinity));
    await checkpoint.markStarted([123]);
    spy.mockRestore();
    await checkpoint.markDone(98);
    await checkpoint.close();
    expect(await readFile(join(dir, 'checkpoint.log'), 'utf8')).toBe('start 123\ndone 98\n');
  });

  it('rejects an append that only got part of its line out, and writes nothing after it', async () => {
    const checkpoint = await Checkpoint.open(dir, params);
    await checkpoint.markStarted([1]);
    const spy = await shortWrites((call) =>
      call === 1 ? 7 : Object.assign(new Error('ENOSPC: no space left on device, write'), { code: 'ENOSPC' }),
    );
    await expect(checkpoint.markStarted([123])).rejects.toThrow(/ENOSPC/);
    spy.mockRestore();
    // The disk has room again, but a line after the torn one would join it into one unreadable line.
    await expect(checkpoint.markDone(1)).rejects.toThrow(/earlier write failed/);
    await checkpoint.close();
    expect(await readFile(join(dir, 'checkpoint.log'), 'utf8')).toBe('start 1\nstart 1');

    const reopened = await Checkpoint.open(dir, params);
    expect([...reopened.started]).toEqual([1]);
    expect(reopened.done.size).toBe(0);
    await reopened.close();
  });

  it('refuses a state dir another live process holds, and takes over a stale lock', async () => {
    const lockPath = join(dir, 'seed.lock');
    await writeFile(lockPath, `${process.ppid} 0123abcd\n`);
    await expect(Checkpoint.open(dir, params)).rejects.toThrow(/in use by process/);
    await writeFile(lockPath, '2147483646 0123abcd\n');
    const checkpoint = await Checkpoint.open(dir, params);
    expect(await readFile(lockPath, 'utf8')).toMatch(new RegExp(`^${process.pid} [0-9a-f]{16}\n$`));
    // A second open in the same process is refused too.
    await expect(Checkpoint.open(dir, params)).rejects.toThrow(/in use by process/);
    await checkpoint.close();
    await expect(readFile(lockPath)).rejects.toThrow(/ENOENT/);
  });

  it('takes an unreadable lock as held unless it is old', async () => {
    // Locks are linked into place whole; an empty one is only taken over once it is clearly a leftover.
    const lockPath = join(dir, 'seed.lock');
    await writeFile(lockPath, '');
    await expect(Checkpoint.open(dir, params)).rejects.toThrow(/seed\.lock is unreadable/);
    const old = new Date(Date.now() - 10 * 60_000);
    await utimes(lockPath, old, old);
    await (await Checkpoint.open(dir, params)).close();
  });

  it('gives up a stale lock it took over when another process took it over at the same moment', async () => {
    const lockPath = join(dir, 'seed.lock');
    await writeFile(lockPath, '2147483646 0123abcd\n');
    const opening = Checkpoint.open(dir, params);
    const outcome = opening.then(
      () => 'opened',
      (error: Error) => error.message,
    );
    // While it waits to confirm the takeover, a second process removes its lock and links its own.
    await new Promise((resolve) => setTimeout(resolve, LOCK_SETTLE_MS / 2));
    await writeFile(lockPath, `${process.ppid} 4567cdef\n`);
    expect(await outcome).toMatch(new RegExp(`seed\\.lock now names process ${process.ppid}: another seed took`));
    expect(await readFile(lockPath, 'utf8')).toBe(`${process.ppid} 4567cdef\n`);
  });

  it('starts no chunk once another process took the state dir over, and leaves its lock alone', async () => {
    const checkpoint = await Checkpoint.open(dir, params);
    await checkpoint.markStarted([1]);
    const lockPath = join(dir, 'seed.lock');
    await writeFile(lockPath, `${process.ppid} 4567cdef\n`);
    await expect(checkpoint.markStarted([2])).rejects.toThrow(/another seed took this state dir over/);
    expect([...checkpoint.started]).toEqual([1]);
    await checkpoint.close();
    expect(await readFile(join(dir, 'checkpoint.log'), 'utf8')).toBe('start 1\n');
    expect(await readFile(lockPath, 'utf8')).toBe(`${process.ppid} 4567cdef\n`);
  });

  it('flush() rejects once a line failed to reach the disk', async () => {
    const checkpoint = await Checkpoint.open(dir, params);
    await checkpoint.markStarted([1]);
    await checkpoint.flush();
    const spy = await shortWrites(() => Object.assign(new Error('EIO: i/o error, write'), { code: 'EIO' }));
    const done = checkpoint.markDone(1).catch(() => {});
    await expect(checkpoint.flush()).rejects.toThrow(/EIO/);
    await done;
    spy.mockRestore();
    await checkpoint.close();
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

  it('refuses a state dir of the same seed into another zone or storage host', async () => {
    const first = await Checkpoint.open(dir, params);
    await first.markStarted([0]);
    await first.markDone(0);
    await first.close();
    await expect(Checkpoint.open(dir, { ...params, zone: 'zone-b' })).rejects.toThrow(/zone differ/);
    await expect(Checkpoint.open(dir, { ...params, host: 'http://127.0.0.1:9' })).rejects.toThrow(/host differ/);
  });

  it('refuses a checkpoint log without the seed.json that says what it is for', async () => {
    await writeFile(join(dir, 'checkpoint.log'), 'start 0\ndone 0\n');
    await expect(Checkpoint.open(dir, params)).rejects.toThrow(/seed\.json is missing/);
  });
});

describe('preflightIds', () => {
  const PLANET = 1_431_655_765;

  function check(from: number, to: number, chunkSize: number) {
    const ids = preflightIds(from, to, chunkSize);
    expect(ids).toEqual([...new Set(ids)].sort((a, b) => a - b));
    expect(ids[0]).toBe(from);
    expect(ids.at(-1)).toBe(to - 1);
    const set = new Set(ids);
    let missingStarts = 0;
    for (let id = Math.ceil(from / chunkSize) * chunkSize; id < to; id += chunkSize) {
      missingStarts += set.has(id) ? 0 : 1;
    }
    expect(missingStarts).toBe(0);
    for (let id = from; id < Math.min(to, from + 24); id++) {
      expect(set.has(id)).toBe(true);
    }
    const stride = Math.ceil((to - from) / 16_384);
    let widest = 0;
    for (let i = 1; i < ids.length; i++) {
      widest = Math.max(widest, ids[i] - ids[i - 1]);
    }
    expect(widest).toBeLessThanOrEqual(stride);
    expect(ids.length).toBeLessThanOrEqual(24 + Math.ceil((to - from) / chunkSize) + 16_384 + 2);
    return ids;
  }

  it('checks every chunk start, the head, the last id and an even spread at planet scale', () => {
    const ids = check(0, PLANET, 65_536);
    // 21,846 chunk starts and 16,384 spread samples (stride 87,382), a few shared.
    expect(ids.length).toBeGreaterThan(21_846 + 16_000);
  });

  it('checks the chunk starts of a range that begins and ends inside chunks', () => {
    // z15 alone: 357,913,941 is not a multiple of 65,536.
    check(357_913_941, PLANET, 65_536);
    check(1_000_000, 1_001_000, 65_536);
  });

  it('falls back to the spread samples when there are too many chunks to check each start', () => {
    const ids = preflightIds(0, PLANET, 1_000);
    expect(ids.length).toBeLessThanOrEqual(24 + 16_384 + 2);
  });
});

describe('chunkProbeIds', () => {
  it('samples the first and last position and an even spread between', () => {
    const ids = chunkProbeIds(65_536, 131_072, 64);
    expect(ids).toHaveLength(64);
    expect(ids[0]).toBe(65_536);
    expect(ids.at(-1)).toBe(131_071);
    expect(ids).toEqual([...new Set(ids)].sort((a, b) => a - b));
    for (let i = 1; i < ids.length; i++) {
      expect(ids[i] - ids[i - 1]).toBeLessThanOrEqual(Math.ceil(65_535 / 63));
    }
  });

  it('catches a 1,000-position slice inside a planet chunk that no preflight sample reaches', () => {
    // --from-id 1000000 --to-id 1001000 lies inside chunk 15 = [983,040, 1,048,576): no chunk start, and between
    // the preflight's spread samples (stride 87,382).
    const slice = (id: number) => id >= 1_000_000 && id < 1_001_000;
    expect(preflightIds(0, 1_431_655_765, 65_536).some(slice)).toBe(false);
    expect(chunkProbeIds(983_040, 1_048_576).some(slice)).toBe(true);
  });

  it('takes every position of a chunk smaller than the sample', () => {
    expect(chunkProbeIds(10, 15, 64)).toEqual([10, 11, 12, 13, 14]);
    expect(chunkProbeIds(7, 8, 64)).toEqual([7]);
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

  it('reads and decodes a leaf once for concurrent lookups in it', async () => {
    const scratch = await scratchDir();
    cleanup.push(scratch.remove);
    const fixture = await writeFixtureArchive(scratch.dir, 1_365, 5, 300);
    const ranges = await ArchiveRanges.open(fixture.path);
    cleanup.push(() => ranges.close());
    const handle = await open(fixture.path);
    const read = vi.spyOn(Object.getPrototypeOf(handle) as FileHandle, 'read');
    await handle.close();
    cleanup.push(async () => read.mockRestore());
    // Ids 0-127 all sit in the first leaf (300 entries of at least one position each).
    const runs = await Promise.all(Array.from({ length: 128 }, (_, id) => ranges.find(id)));
    expect(runs.every((run, id) => run && run.tileId <= id && id < run.tileId + run.runLength)).toBe(true);
    expect(read).toHaveBeenCalledTimes(1);
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
