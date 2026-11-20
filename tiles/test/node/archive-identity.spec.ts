import { createHash, randomBytes } from 'node:crypto';
import { closeSync, futimesSync, openSync } from 'node:fs';
import { copyFile, readFile, rename, stat, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import type { Worker as NodeWorker } from 'node:worker_threads';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  archiveIdentity,
  checkOpened,
  fileMd5Base64,
  fileSnapshot,
  identifyArchive,
  IDENTITY_BLOCK_BYTES,
  IDENTITY_PATTERN,
  parseMd5Base64,
} from '../../src/bunny/archive-identity';
import { ArchiveRanges } from '../../src/bunny/archive-walk';
import { scratchDir, withOtherTileBytes, writeFixtureArchive } from './upload-fixture';

/** Set by a test to fail the start of a thread, as Node fails one it can't start; `started` lists those it started. */
const threads = vi.hoisted(() => ({ failAt: undefined as number | undefined, started: [] as NodeWorker[] }));
vi.mock('node:worker_threads', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:worker_threads')>();
  class Worker extends actual.Worker {
    constructor(...args: ConstructorParameters<typeof actual.Worker>) {
      if (threads.failAt !== undefined && threads.started.length === threads.failAt) {
        throw new Error('could not start a thread');
      }
      super(...args);
      threads.started.push(this);
    }
  }
  return { ...actual, Worker };
});

let cleanup: (() => Promise<void>)[] = [];

afterEach(async () => {
  for (const step of cleanup.reverse()) {
    await step();
  }
  cleanup = [];
});

async function scratch() {
  const { dir, remove } = await scratchDir();
  cleanup.push(remove);
  return dir;
}

/** The v2 identity as archive-identity.ts defines it, computed the plain way. */
function reference(bytes: Uint8Array, blockBytes = IDENTITY_BLOCK_BYTES): string {
  const root = createHash('sha256').update(`pmtiles-archive-identity-v2\n${bytes.length}\n`);
  for (let start = 0; start < bytes.length; start += blockBytes) {
    root.update(
      createHash('sha256')
        .update(bytes.subarray(start, start + blockBytes))
        .digest(),
    );
  }
  return `sha256-tree-v2:${root.digest('hex')}`;
}

describe('archiveIdentity', () => {
  it('hashes the whole file in blocks, the same on any number of threads', async () => {
    const dir = await scratch();
    const path = join(dir, 'blocks.bin');
    const bytes = randomBytes(5 * 4096 + 123);
    await writeFile(path, bytes);
    for (const threads of [1, 3, 8]) {
      expect(await archiveIdentity(path, { threads, blockBytes: 4096 }), `${threads} threads`).toEqual({
        size: bytes.length,
        identity: reference(bytes, 4096),
      });
    }
    // One block (any file up to 64 MiB) is hashed without threads, by the same definition.
    const identity = await archiveIdentity(path);
    expect(identity).toEqual({ size: bytes.length, identity: reference(bytes) });
    expect(identity.identity).toMatch(IDENTITY_PATTERN);
    // Exact multiples of the block size and empty files too.
    await writeFile(path, bytes.subarray(0, 3 * 4096));
    expect((await archiveIdentity(path, { threads: 2, blockBytes: 4096 })).identity).toBe(
      reference(bytes.subarray(0, 3 * 4096), 4096),
    );
    await writeFile(path, '');
    expect(await archiveIdentity(path)).toEqual({ size: 0, identity: reference(new Uint8Array(0)) });
  });

  it('tells apart archives that share the header, root directory and size but not the tile bytes', async () => {
    const dir = await scratch();
    const fixture = await writeFixtureArchive(dir, 2_000);
    const other = join(dir, 'other.pmtiles');
    await withOtherTileBytes(fixture.path, other);

    const [a, b] = await Promise.all([ArchiveRanges.open(fixture.path), ArchiveRanges.open(other)]);
    await Promise.all([a.close(), b.close()]);
    // The v1 fingerprint seeds recorded before can't tell them apart...
    expect(b.legacyFingerprint).toBe(a.legacyFingerprint);
    expect(b.size).toBe(a.size);
    expect(b.archive.header).toEqual(a.archive.header);
    // ...the v2 identity can, on threads or not.
    for (const options of [{}, { threads: 4, blockBytes: 1024 }]) {
      const [first, second] = [await archiveIdentity(fixture.path, options), await archiveIdentity(other, options)];
      expect(second.size).toBe(first.size);
      expect(second.identity).not.toBe(first.identity);
    }
    expect((await archiveIdentity(fixture.path)).identity).toBe(reference(await readFile(fixture.path)));
  });
});

describe('identifyArchive', () => {
  it('stops the threads it started when a later one fails to start', async () => {
    const dir = await scratch();
    const path = join(dir, 'blocks.bin');
    const bytes = randomBytes(8 * 4096);
    await writeFile(path, bytes);
    threads.started = [];
    threads.failAt = 2;
    cleanup.push(async () => {
      threads.failAt = undefined;
    });
    await expect(identifyArchive(path, { threads: 4, blockBytes: 4096 })).rejects.toThrow(/could not start a thread/);
    expect(threads.started).toHaveLength(2);
    // Both were stopped before the call failed, not left reading the file: a thread that no longer runs has id -1.
    expect(threads.started.map((worker) => worker.threadId)).toEqual([-1, -1]);

    threads.failAt = undefined;
    expect((await identifyArchive(path, { threads: 4, blockBytes: 4096 })).identity.identity).toBe(
      reference(bytes, 4096),
    );
  });

  it('ties the identity to the file it hashed, which another file put at the path does not match', async () => {
    const dir = await scratch();
    const fixture = await writeFixtureArchive(dir, 2_000);
    for (const options of [{}, { threads: 3, blockBytes: 1024 }]) {
      const identified = await identifyArchive(fixture.path, options);
      expect(identified.identity).toEqual(await archiveIdentity(fixture.path, options));
      expect(identified.source).toBe(fileSnapshot(await stat(fixture.path, { bigint: true })));
    }
    const { source } = await identifyArchive(fixture.path);
    const original = openSync(fixture.path, 'r');
    cleanup.push(async () => closeSync(original));
    checkOpened(fixture.path, original, source);
    await (await ArchiveRanges.open(fixture.path, source)).close();

    // The same size, header and root directory, other tile bytes, put at the path. Nothing is asserted of the
    // original descriptor after this: losing its name moves the file's change time on ext4 (CI's TMPDIR) and tmpfs
    // but not on ZFS, so whether its check still passes depends on the filesystem.
    const other = join(dir, 'other.pmtiles');
    await withOtherTileBytes(fixture.path, other);
    await rename(other, fixture.path);
    const replaced = openSync(fixture.path, 'r');
    cleanup.push(async () => closeSync(replaced));
    expect(() => checkOpened(fixture.path, replaced, source)).toThrow(/no longer the file its identity was computed/);
    await expect(ArchiveRanges.open(fixture.path, source)).rejects.toThrow(/no longer the file its identity/);
  });

  it('no longer matches the file it hashed once its modification time moves, as a write moves it', async () => {
    const dir = await scratch();
    const fixture = await writeFixtureArchive(dir, 2_000);
    const { source } = await identifyArchive(fixture.path);
    const fd = openSync(fixture.path, 'r');
    cleanup.push(async () => closeSync(fd));
    const ranges = await ArchiveRanges.open(fixture.path, source);
    cleanup.push(() => ranges.close());
    checkOpened(fixture.path, fd, source);
    await ranges.checkUnchanged();

    // A modification time the file can't have had (it was written just now), so the snapshot differs however
    // coarse the filesystem's clock is: a write within the same tick could leave both times as they were.
    const past = new Date('2001-02-03T04:05:06Z');
    futimesSync(fd, past, past);
    expect((await stat(fixture.path)).mtime).toEqual(past);
    expect(() => checkOpened(fixture.path, fd, source)).toThrow(/replaced or written to/);
    await expect(ranges.checkUnchanged()).rejects.toThrow(/was written to while it was read/);
    await expect(ArchiveRanges.open(fixture.path, source)).rejects.toThrow(/no longer the file its identity/);
    await expect(fileMd5Base64(fixture.path, undefined, source)).rejects.toThrow(/replaced or written to/);
  });
});

describe('fileMd5Base64', () => {
  it("hashes the whole file as builds.json's md5sum does", async () => {
    const dir = await scratch();
    const path = join(dir, 'file.bin');
    const bytes = randomBytes(20 * 2 ** 20 + 7);
    await writeFile(path, bytes);
    expect(await fileMd5Base64(path)).toBe(createHash('md5').update(bytes).digest('base64'));
    // bunny:sync stops the pass when the identity beside it fails.
    await expect(fileMd5Base64(path, AbortSignal.abort())).rejects.toThrow(/abort/i);
  });

  it('hashes only the file the identity is of, given its snapshot', async () => {
    const dir = await scratch();
    const fixture = await writeFixtureArchive(dir, 2_000);
    const { source } = await identifyArchive(fixture.path);
    const md5 = createHash('md5')
      .update(await readFile(fixture.path))
      .digest('base64');
    expect(await fileMd5Base64(fixture.path, undefined, source)).toBe(md5);
    // The same bytes in another file at the path: not the file the identity was computed from.
    const copy = join(dir, 'copy.pmtiles');
    await copyFile(fixture.path, copy);
    await rename(copy, fixture.path);
    await expect(fileMd5Base64(fixture.path, undefined, source)).rejects.toThrow(/replaced or written to/);
    expect(await fileMd5Base64(fixture.path)).toBe(md5);
  });

  it('takes only a base64 MD5', () => {
    expect(parseMd5Base64('hKPqtB5lzZzHKO5NlYsxDg==')).toBe('hKPqtB5lzZzHKO5NlYsxDg==');
    for (const bad of ['', '84a3eab41e65cd9cc728ee4d958b310e', 'hKPqtB5lzZzHKO5NlYsxDg=', 'hKPqtB5lzZzHKO5NlYsxDh==']) {
      expect(() => parseMd5Base64(bad), bad).toThrow(/--archive-md5 must be a base64 MD5/);
    }
  });
});
