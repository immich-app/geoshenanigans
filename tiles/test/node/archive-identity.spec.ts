import { createHash, randomBytes } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import {
  archiveIdentity,
  fileMd5Base64,
  IDENTITY_BLOCK_BYTES,
  IDENTITY_PATTERN,
  parseMd5Base64,
} from '../../src/bunny/archive-identity';
import { ArchiveRanges } from '../../src/bunny/archive-walk';
import { scratchDir, withOtherTileBytes, writeFixtureArchive } from './upload-fixture';

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

  it('takes only a base64 MD5', () => {
    expect(parseMd5Base64('hKPqtB5lzZzHKO5NlYsxDg==')).toBe('hKPqtB5lzZzHKO5NlYsxDg==');
    for (const bad of ['', '84a3eab41e65cd9cc728ee4d958b310e', 'hKPqtB5lzZzHKO5NlYsxDg=', 'hKPqtB5lzZzHKO5NlYsxDh==']) {
      expect(() => parseMd5Base64(bad), bad).toThrow(/--archive-md5 must be a base64 MD5/);
    }
  });
});
