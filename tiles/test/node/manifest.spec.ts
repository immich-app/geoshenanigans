import { createHash } from 'node:crypto';
import { readdir, readFile, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
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
import { decodeDirectory } from '../../src/bunny/manifest-worker';
import { openArchiveFile } from '../../src/deploy/archive-file';
import {
  BIG_CONTENT,
  Build,
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

  it('rejects entries out of order or overlapping', async () => {
    const writer = await ManifestWriter.create(scratch.path('m.tman.zst'), { buildKey: '20261118' });
    writer.add(10, 5, 1, 2);
    expect(() => writer.add(12, 1, 3, 4)).toThrow(/overlaps/);
    expect(() => writer.add(20, 0, 3, 4)).toThrow(/Invalid/);
    await writer.abort();
    expect(await readdir(scratch.dir)).toEqual([]);
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
  it('decodes offsets that continue from the previous entry', () => {
    // 2 entries: ids 5, 7; runs 1, 3; lengths 10, 20; offsets 0 (+1 encoded) then "right after".
    const directory = decodeDirectory(new Uint8Array([2, 5, 2, 1, 3, 10, 20, 1, 0]));
    expect([...directory.tileIds]).toEqual([5, 7]);
    expect([...directory.runLengths]).toEqual([1, 3]);
    expect([...directory.offsets]).toEqual([0, 10]);
    expect([...directory.lengths]).toEqual([10, 20]);
  });
});
