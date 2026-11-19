import { createHash } from 'node:crypto';
import { utimesSync } from 'node:fs';
import { copyFile, link, readdir, readFile, rename, stat, symlink, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { gzipSync } from 'node:zlib';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { fileSnapshot } from '../../src/bunny/archive-identity';
import {
  buildManifest,
  decodeHeaderFrame,
  MANIFEST_BODY_OFFSET,
  ManifestEntry,
  ManifestWriter,
  readManifest,
  readManifestHeader,
  writeManifest,
} from '../../src/bunny/manifest';
import { decodeDirectory, JobRunner } from '../../src/bunny/manifest-worker';
import { MAX_SAFE_ZOOM, zoomEnd } from '../../src/bunny/tile-id';
import { openArchiveFile } from '../../src/deploy/archive-file';
import {
  BIG_CONTENT,
  Build,
  contentBytes,
  contentHash,
  expectedEntries,
  randomBuild,
  rng,
  scratchDir,
  writeArchiveFile,
} from './manifest-fixtures';

const ASSET = fileURLToPath(new URL('../assets/tiles.pmtiles', import.meta.url));

const collect = async (path: string) => {
  const entries: ManifestEntry[] = [];
  for await (const entry of readManifest(path)) {
    entries.push(entry);
  }
  return entries;
};

/** A gzipped PMTiles v3 directory: count, tile ID deltas, run lengths, lengths, offsets + 1. */
function directoryBytes(entries: { tileId: number; runLength: number; offset: number; length: number }[]): Buffer {
  const out: number[] = [];
  const varint = (value: number) => {
    for (; value >= 0x80; value = Math.floor(value / 0x80)) {
      out.push((value % 0x80) | 0x80);
    }
    out.push(value);
  };
  varint(entries.length);
  entries.forEach((entry, i) => varint(entry.tileId - (i > 0 ? entries[i - 1].tileId : 0)));
  entries.forEach((entry) => varint(entry.runLength));
  entries.forEach((entry) => varint(entry.length));
  entries.forEach((entry) => varint(entry.offset + 1));
  return gzipSync(Buffer.from(out));
}

/** A clustered archive holding one tile over `runLength` positions from tile ID 0, in the root or in a leaf. */
function longRunArchive(tile: Uint8Array, runLength: number, inLeaf: boolean): Buffer {
  const entry = { tileId: 0, runLength, offset: 0, length: tile.length };
  const leaf = inLeaf ? directoryBytes([entry]) : Buffer.alloc(0);
  const root = directoryBytes([inLeaf ? { tileId: 0, runLength: 0, offset: 0, length: leaf.length } : entry]);
  const metadata = gzipSync(Buffer.from('{}'));
  const header = Buffer.alloc(127);
  header.write('PMTiles', 0, 'latin1');
  header[7] = 3;
  const metadataOffset = 127 + root.length;
  const leafOffset = metadataOffset + metadata.length;
  const fields = [127, root.length, metadataOffset, metadata.length, leafOffset, leaf.length];
  [...fields, leafOffset + leaf.length, tile.length, runLength, 1, 1].forEach((value, i) =>
    header.writeBigUInt64LE(BigInt(value), 8 + i * 8),
  );
  header.set([1, 2, 2, 1, 0, 16], 96); // clustered, gzip directories and tiles, MVT, zooms 0-16
  return Buffer.concat([header, root, metadata, leaf, tile]);
}

let scratch: Awaited<ReturnType<typeof scratchDir>>;
beforeEach(async () => {
  scratch = await scratchDir();
});
afterEach(async () => {
  await scratch.cleanup();
});

describe('buildManifest', () => {
  it('records the hash of every position of a real archive', async () => {
    const out = scratch.path('asset.tman.zst');
    const stats = await buildManifest(ASSET, { out, buildKey: '20240726', threads: 2 });

    const { archive, close } = await openArchiveFile(ASSET);
    const expected: ManifestEntry[] = [];
    for await (const run of archive.tileEntries()) {
      for (const entry of run) {
        const bytes = new Uint8Array(await archive.readTileData(entry.offset, entry.length));
        const hash = createHash('sha256').update(bytes).digest('hex').slice(0, 16).toUpperCase();
        expected.push({ tileId: entry.tileId, runLength: entry.runLength, hash });
      }
    }
    await close();

    expect(await collect(out)).toEqual(expected);
    expect(stats.header).toMatchObject({ buildKey: '20240726', positions: 5, entries: 5, minZoom: 0, maxZoom: 1 });
    expect(stats.header.osmDate).toBe('2024-07-26T04:00:00Z');
    expect(await readManifestHeader(out)).toEqual(stats.header);
    expect(stats.fileBytes).toBe((await readFile(out)).length);
  });

  it.each([
    { seed: 1, threads: 1, leavesPerJob: 1, layout: {} },
    { seed: 2, threads: 4, leavesPerJob: 1, layout: { rootTiles: 5, nestChance: 0.3 } },
    { seed: 3, threads: 3, leavesPerJob: 2, layout: { splitChance: 0.6, duplicateChance: 0.05 } },
    { seed: 4, threads: 2, leavesPerJob: 3, layout: { maxLeafEntries: 2, nestChance: 0.5, rootTiles: 1 } },
    { seed: 5, threads: 5, leavesPerJob: 1, layout: { splitChance: 0.05, maxLeafEntries: 30 } },
  ])(
    'matches a per-position hash of a generated archive (seed $seed)',
    async ({ seed, threads, leavesPerJob, layout }) => {
      const r = rng(seed);
      const build = randomBuild(r, 3000, r.int(0, 2) * 21_845, 0.02);
      const archivePath = scratch.path('a.pmtiles');
      await writeArchiveFile(archivePath, build, r, layout);
      const out = scratch.path('a.tman.zst');
      const stats = await buildManifest(archivePath, { out, buildKey: '20261118', threads, leavesPerJob });

      const expected = expectedEntries(build);
      expect(await collect(out)).toEqual(expected);
      expect(stats.header.entries).toBe(expected.length);
      expect(stats.header.positions).toBe(build.contents.filter((content) => content !== null).length);
    },
  );

  it('hashes each distinct tile once, the ocean tile included, across jobs and threads', async () => {
    const r = rng(11);
    const build = randomBuild(r, 6000);
    const archivePath = scratch.path('a.pmtiles');
    const archive = await writeArchiveFile(archivePath, build, r, { maxLeafEntries: 4 });
    const stats = await buildManifest(archivePath, {
      out: scratch.path('a.tman.zst'),
      buildKey: '20261118',
      threads: 4,
      leavesPerJob: 1,
    });

    expect(stats.jobs).toBeGreaterThan(100);
    expect(stats.tileContents).toBe(archive.tileContents);
    expect(stats.tilesHashed).toBe(archive.tileContents);
  });

  it('still hashes repeats correctly when they point further back than it remembers', async () => {
    const r = rng(12);
    const build = randomBuild(r, 4000);
    const archivePath = scratch.path('a.pmtiles');
    const archive = await writeArchiveFile(archivePath, build, r, { maxLeafEntries: 3, duplicateChance: 0.1 });
    const out = scratch.path('a.tman.zst');
    const stats = await buildManifest(archivePath, {
      out,
      buildKey: '20261118',
      threads: 3,
      leavesPerJob: 1,
      windowJobs: 0,
      cacheEntries: 1,
    });

    expect(await collect(out)).toEqual(expectedEntries(build));
    expect(stats.tilesHashed).toBeGreaterThan(archive.tileContents);
  });

  it('reads tiles larger than a read slab and runs of them', async () => {
    const contents = [0, 0, BIG_CONTENT, BIG_CONTENT, 7, BIG_CONTENT + 1, 0, BIG_CONTENT, 8, 9, BIG_CONTENT + 2];
    const build: Build = { base: 0, contents };
    const archivePath = scratch.path('a.pmtiles');
    await writeArchiveFile(archivePath, build, rng(13), { maxLeafEntries: 3 });
    const out = scratch.path('a.tman.zst');
    await buildManifest(archivePath, { out, buildKey: '20261118', threads: 2, leavesPerJob: 1 });
    expect(await collect(out)).toEqual(expectedEntries(build));
  });

  it('writes the same entries for the same content whatever the archive layout and thread count', async () => {
    const r = rng(21);
    const build = randomBuild(r, 5000, 1365);
    const bodies: Buffer[] = [];
    for (const [i, layout] of [
      { splitChance: 0, maxLeafEntries: 50 },
      { splitChance: 0.7, maxLeafEntries: 2, nestChance: 0.4, rootTiles: 3, duplicateChance: 0.1 },
    ].entries()) {
      const archivePath = scratch.path(`${i}.pmtiles`);
      await writeArchiveFile(archivePath, build, r, layout);
      for (const threads of [1, 4]) {
        const out = scratch.path(`${i}-${threads}.tman.zst`);
        await buildManifest(archivePath, { out, buildKey: '20261118', threads, leavesPerJob: 1 });
        bodies.push((await readFile(out)).subarray(MANIFEST_BODY_OFFSET));
      }
    }
    for (const body of bodies.slice(1)) {
      expect(body.equals(bodies[0])).toBe(true);
    }
  });

  it('reads the OSM date from planetiler metadata', async () => {
    const archivePath = scratch.path('a.pmtiles');
    await writeArchiveFile(archivePath, { base: 0, contents: [1, 2, 3, 4, 5] }, rng(1), {
      metadata: { 'planetiler:osm:osmosisreplicationtime': '2026-11-17T23:59:59Z' },
      maxZoom: 1,
    });
    const stats = await buildManifest(archivePath, { out: scratch.path('a.tman.zst'), buildKey: '20261118' });
    expect(stats.header).toMatchObject({ osmDate: '2026-11-17T23:59:59Z', minZoom: 0, maxZoom: 1 });
  });

  it('fails without leaving a file when the archive is damaged', async () => {
    const archivePath = scratch.path('a.pmtiles');
    const archive = await writeArchiveFile(archivePath, randomBuild(rng(5), 2000), rng(5), { maxLeafEntries: 5 });
    // Cut off the end of the tile data: the last job's reads fail in its thread.
    await writeFile(archivePath, archive.bytes.subarray(0, archive.bytes.length - 2000));
    const out = scratch.path('a.tman.zst');
    await expect(
      buildManifest(archivePath, { out, buildKey: '20261118', threads: 3, leavesPerJob: 1 }),
    ).rejects.toThrow(/Unexpected end of archive/);
    expect(await readdir(scratch.dir)).toEqual(['a.pmtiles']);
  });

  it.each([
    { runLength: 2 ** 32, inLeaf: false },
    { runLength: 2 ** 32 + 1, inLeaf: false },
    { runLength: 2 ** 32, inLeaf: true },
    { runLength: 2 ** 32 + 1, inLeaf: true },
  ])('keeps a run of $runLength positions whole (in a leaf: $inLeaf)', async ({ runLength, inLeaf }) => {
    const archivePath = scratch.path('long.pmtiles');
    await writeFile(archivePath, longRunArchive(contentBytes(1), runLength, inLeaf));
    const out = scratch.path('long.tman.zst');
    const stats = await buildManifest(archivePath, { out, buildKey: '20261118', threads: 1 });
    expect(await collect(out)).toEqual([{ tileId: 0, runLength, hash: contentHash(1) }]);
    expect(stats.header).toMatchObject({ entries: 1, positions: runLength });
  });

  it('refuses to write the manifest over its own archive', async () => {
    const archivePath = scratch.path('a.pmtiles');
    const archive = await writeArchiveFile(archivePath, randomBuild(rng(6), 500), rng(6));
    const hardLink = scratch.path('hard.pmtiles');
    const softLink = scratch.path('soft.pmtiles');
    await link(archivePath, hardLink);
    await symlink(archivePath, softLink);
    for (const out of [archivePath, hardLink, softLink]) {
      await expect(buildManifest(archivePath, { out, buildKey: '20261118', threads: 1 }), out).rejects.toThrow(
        /is the archive/,
      );
    }
    expect((await readFile(archivePath)).equals(archive.bytes)).toBe(true);
    expect((await readdir(scratch.dir)).sort()).toEqual(['a.pmtiles', 'hard.pmtiles', 'soft.pmtiles']);
    // Replacing an earlier manifest of it is fine.
    const out = scratch.path('a.tman.zst');
    await writeFile(out, 'stale');
    await buildManifest(archivePath, { out, buildKey: '20261118', threads: 1 });
    expect((await readManifestHeader(out)).buildKey).toBe('20261118');
  });

  it('reads only the file of the snapshot it was given, on every thread, still at the path once all is read', async () => {
    const archivePath = scratch.path('a.pmtiles');
    await writeArchiveFile(archivePath, randomBuild(rng(7), 2000), rng(7), { maxLeafEntries: 5 });
    const before = await stat(archivePath, { bigint: true });
    const source = fileSnapshot(before);
    const options = { buildKey: '20261118', threads: 3, leavesPerJob: 1 };
    const bound = scratch.path('bound.tman.zst');
    const plain = scratch.path('plain.tman.zst');
    await buildManifest(archivePath, { ...options, out: bound, source });
    await buildManifest(archivePath, { ...options, out: plain });
    expect(await collect(bound)).toEqual(await collect(plain));
    // Each hashing thread checks the file it opened too.
    const init = { manifestWorker: true, path: archivePath, leafDirectoryOffset: 0, tileDataOffset: 0 } as const;
    expect(() => new JobRunner({ ...init, internalCompression: 2, source: 'another file' })).toThrow(
      /replaced or written to/,
    );

    // Written to while it is read: the path no longer names the file as it was. Its modification time moves a minute
    // back, which no filesystem's clock granularity hides (a time set to now could land in the same tick). It moves
    // once the last job is merged: every thread has opened the file by then (each had jobs), so only the check after
    // the read can catch it. Moved any earlier, a thread still starting up could catch it first, with its own message.
    const moved = new Date(Number(before.mtimeMs) - 60_000);
    let movedAt: { jobs: number; jobsTotal: number } | undefined;
    await expect(
      buildManifest(archivePath, {
        ...options,
        out: scratch.path('written.tman.zst'),
        source,
        onProgress: ({ jobs, jobsTotal }) => {
          if (jobs === jobsTotal) {
            movedAt = { jobs, jobsTotal };
            utimesSync(archivePath, moved, moved);
          }
        },
      }),
    ).rejects.toThrow(/replaced or written to while its manifest was built/);
    expect(movedAt?.jobsTotal).toBeGreaterThan(options.threads);
    expect((await stat(archivePath)).mtime).toEqual(moved);
    // Another file put at the path: the same bytes, but not the file the snapshot is of.
    const copy = scratch.path('copy.pmtiles');
    await copyFile(archivePath, copy);
    const now = fileSnapshot(await stat(archivePath, { bigint: true }));
    await rename(copy, archivePath);
    await expect(
      buildManifest(archivePath, { ...options, out: scratch.path('replaced.tman.zst'), source: now }),
    ).rejects.toThrow(/replaced or written to/);
    expect((await readdir(scratch.dir)).sort()).toEqual(['a.pmtiles', 'bound.tman.zst', 'plain.tman.zst']);
  });

  it('rejects a build key that is not a date', async () => {
    await expect(buildManifest(ASSET, { out: scratch.path('a.tman.zst'), buildKey: '../x' })).rejects.toThrow(
      /YYYYMMDD/,
    );
  });
});

describe('manifest file', () => {
  const hash = (n: number) => n.toString(16).toUpperCase().padStart(16, '0');

  it('round-trips gaps, large tile ids and long runs, merging equal neighbours', async () => {
    const out = scratch.path('m.tman.zst');
    const entries: ManifestEntry[] = [
      { tileId: 0, runLength: 1, hash: hash(1) },
      { tileId: 1, runLength: 4, hash: hash(1) },
      { tileId: 10, runLength: 1, hash: hash(1) },
      { tileId: 11, runLength: 2 ** 31 + 5, hash: hash(2) },
      { tileId: 2 ** 40, runLength: 3, hash: 'FFFFFFFFFFFFFFFF' },
      { tileId: 2 ** 40 + 3, runLength: 1, hash: '0000000000000000' },
    ];
    const header = await writeManifest(out, { buildKey: '20261118' }, entries);
    expect(await collect(out)).toEqual([
      { tileId: 0, runLength: 5, hash: hash(1) },
      { tileId: 10, runLength: 1, hash: hash(1) },
      { tileId: 11, runLength: 2 ** 31 + 5, hash: hash(2) },
      { tileId: 2 ** 40, runLength: 3, hash: 'FFFFFFFFFFFFFFFF' },
      { tileId: 2 ** 40 + 3, runLength: 1, hash: '0000000000000000' },
    ]);
    expect(header).toMatchObject({ entries: 5, positions: 5 + 1 + 2 ** 31 + 5 + 3 + 1, osmDate: null, minZoom: null });
  });

  it('streams many entries across chunk boundaries', async () => {
    const out = scratch.path('m.tman.zst');
    const r = rng(3);
    const entries: ManifestEntry[] = [];
    let tileId = 0;
    for (let i = 0; i < 300_000; i++) {
      tileId += r.chance(0.1) ? r.int(1, 1000) : 0;
      const runLength = r.chance(0.2) ? r.int(1, 100_000) : 1;
      entries.push({ tileId, runLength, hash: hash(i * 2654435761) });
      tileId += runLength;
    }
    await writeManifest(out, { buildKey: '20261118' }, entries);
    expect(await collect(out)).toEqual(entries);
  });

  it('refuses to read positions past zoom 26, whatever reads them', async () => {
    const end = zoomEnd(MAX_SAFE_ZOOM);
    for (const [i, entry] of [
      { tileId: end - 1, runLength: 2, hash: hash(1) },
      { tileId: end, runLength: 1, hash: hash(1) },
      { tileId: Number.MAX_SAFE_INTEGER - 1, runLength: 1, hash: hash(1) },
    ].entries()) {
      const out = scratch.path(`past-${i}.tman.zst`);
      await writeManifest(out, { buildKey: '20261118' }, [{ tileId: 0, runLength: 1, hash: hash(2) }, entry]);
      await expect(collect(out), `case ${i}`).rejects.toThrow(/go past zoom 26/);
    }
    // The last position of zoom 26 still reads.
    const out = scratch.path('last.tman.zst');
    await writeManifest(out, { buildKey: '20261118' }, [{ tileId: end - 1, runLength: 1, hash: hash(1) }]);
    expect(await collect(out)).toEqual([{ tileId: end - 1, runLength: 1, hash: hash(1) }]);
  });

  it('rejects entries out of order or overlapping', async () => {
    const writer = await ManifestWriter.create(scratch.path('m.tman.zst'), { buildKey: '20261118' });
    writer.add(10, 5, 1, 2);
    expect(() => writer.add(12, 1, 3, 4)).toThrow(/overlaps/);
    expect(() => writer.add(20, 0, 3, 4)).toThrow(/Invalid/);
    await writer.abort();
    expect(await readdir(scratch.dir)).toEqual([]);
  });

  it('gives each writer of the same file its own partial file', async () => {
    const out = scratch.path('m.tman.zst');
    const first = await ManifestWriter.create(out, { buildKey: '20261117' });
    const second = await ManifestWriter.create(out, { buildKey: '20261118' });
    first.add(0, 3, 1, 2);
    second.add(0, 5, 3, 4);
    second.add(7, 1, 5, 6);
    await first.drain();
    await second.drain();
    await first.abort();
    const header = await second.finish();
    expect(header).toMatchObject({ buildKey: '20261118', entries: 2, positions: 6 });
    expect(await collect(out)).toEqual([
      { tileId: 0, runLength: 5, hash: hash(0x300000004) },
      { tileId: 7, runLength: 1, hash: hash(0x500000006) },
    ]);
    expect(await readdir(scratch.dir)).toEqual(['m.tman.zst']);
  });

  it('detects truncation, corruption and unfinished files', async () => {
    const out = scratch.path('m.tman.zst');
    const r = rng(4);
    const entries = Array.from({ length: 50_000 }, (_, i) => ({
      tileId: i,
      runLength: 1,
      hash: hash(r.int(0, 2 ** 30)),
    }));
    await writeManifest(out, { buildKey: '20261118' }, entries);
    const bytes = await readFile(out);

    const truncated = scratch.path('truncated.tman.zst');
    await writeFile(truncated, bytes.subarray(0, bytes.length - 100));
    await expect(collect(truncated)).rejects.toThrow();

    const corrupt = scratch.path('corrupt.tman.zst');
    const flipped = Buffer.from(bytes);
    flipped[Math.floor(flipped.length * 0.7)] ^= 0x40;
    await writeFile(corrupt, flipped);
    await expect(collect(corrupt)).rejects.toThrow();

    const unfinished = scratch.path('unfinished.tman.zst');
    await writeFile(
      unfinished,
      Buffer.concat([Buffer.alloc(MANIFEST_BODY_OFFSET), bytes.subarray(MANIFEST_BODY_OFFSET)]),
    );
    await expect(readManifestHeader(unfinished)).rejects.toThrow(/never finished/);

    // A header that disagrees with the entries (e.g. a body from another file).
    const header = decodeHeaderFrame(bytes);
    const json = JSON.stringify({ ...header, version: undefined, entries: header.entries + 1 });
    const mismatched = Buffer.from(bytes);
    mismatched.writeUInt16LE(Buffer.byteLength(json), 14);
    mismatched.fill(0, 16, MANIFEST_BODY_OFFSET);
    mismatched.write(json, 16);
    const mismatchedPath = scratch.path('mismatched.tman.zst');
    await writeFile(mismatchedPath, mismatched);
    await expect(collect(mismatchedPath)).rejects.toThrow(/header says/);
  });
});

describe('decodeDirectory', () => {
  it('decodes run lengths beyond 32 bits', () => {
    // 1 entry: id 0, run 2^32 + 1 (5-byte varint), length 7, offset 0 (+1 encoded).
    const directory = decodeDirectory(new Uint8Array([1, 0, 0x81, 0x80, 0x80, 0x80, 0x10, 7, 1]));
    expect([...directory.runLengths]).toEqual([2 ** 32 + 1]);
  });

  it('decodes offsets that continue from the previous entry', () => {
    // 2 entries: ids 5, 7; runs 1, 3; lengths 10, 20; offsets 0 (+1 encoded) then "right after".
    const directory = decodeDirectory(new Uint8Array([2, 5, 2, 1, 3, 10, 20, 1, 0]));
    expect([...directory.tileIds]).toEqual([5, 7]);
    expect([...directory.runLengths]).toEqual([1, 3]);
    expect([...directory.offsets]).toEqual([0, 10]);
    expect([...directory.lengths]).toEqual([10, 20]);
  });
});
