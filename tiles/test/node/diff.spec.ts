import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { changedTiles, DiffKind, diffManifests, DiffRange, DiffTotals, zoomOf } from '../../src/bunny/diff';
import { buildManifest, writeManifest } from '../../src/bunny/manifest';
import { openArchiveFile } from '../../src/deploy/archive-file';
import { zxyToTileId } from '../../src/pmtiles/utils';
import {
  Build,
  contentBytes,
  contentHash,
  expectedEntries,
  LayoutOptions,
  mutateBuild,
  randomBuild,
  Rng,
  rng,
  scratchDir,
  writeArchiveFile,
} from './manifest-fixtures';

/** The diff by brute force: every position compared on its own, then grouped into ranges. */
function bruteForce(oldBuild: Build, newBuild: Build) {
  const hashAt = (build: Build, tileId: number) => {
    const content = build.contents[tileId - build.base];
    return content === null || content === undefined ? null : contentHash(content);
  };
  const first = Math.min(oldBuild.base, newBuild.base);
  const last = Math.max(oldBuild.base + oldBuild.contents.length, newBuild.base + newBuild.contents.length);
  const ranges: DiffRange[] = [];
  const counts = { unchanged: 0, changed: 0, added: 0, removed: 0 };
  const byZoom: Record<number, Record<DiffKind, number>> = {};
  for (let tileId = first; tileId < last; tileId++) {
    const before = hashAt(oldBuild, tileId);
    const after = hashAt(newBuild, tileId);
    let kind: DiffKind | undefined;
    if (before !== null && after !== null) {
      kind = before === after ? undefined : 'changed';
    } else if (before !== null) {
      kind = 'removed';
    } else if (after !== null) {
      kind = 'added';
    } else {
      continue;
    }
    if (!kind) {
      counts.unchanged++;
      continue;
    }
    counts[kind]++;
    const z = zoomOf(tileId);
    byZoom[z] ??= { changed: 0, added: 0, removed: 0 };
    byZoom[z][kind]++;
    const previous = ranges[ranges.length - 1];
    if (previous && previous.kind === kind && previous.start + previous.count === tileId) {
      previous.count++;
    } else {
      ranges.push({ start: tileId, count: 1, kind });
    }
  }
  return { ranges, counts, byZoom };
}

async function runDiff(oldPath: string, newPath: string) {
  const diff = await diffManifests(oldPath, newPath);
  const ranges: DiffRange[] = [];
  for await (const range of diff) {
    ranges.push(range);
  }
  return { ranges, totals: diff.totals };
}

function expectMatches(actual: { ranges: DiffRange[]; totals: DiffTotals }, oldBuild: Build, newBuild: Build) {
  const expected = bruteForce(oldBuild, newBuild);
  expect(actual.ranges).toEqual(expected.ranges);
  const { totals } = actual;
  expect({
    unchanged: totals.unchanged,
    changed: totals.changed,
    added: totals.added,
    removed: totals.removed,
  }).toEqual(expected.counts);
  expect(totals.ranges).toBe(expected.ranges.length);
  const byZoom = Object.fromEntries(
    totals.byZoom.flatMap((counts, z) => (counts.changed + counts.added + counts.removed > 0 ? [[z, counts]] : [])),
  );
  expect(byZoom).toEqual(expected.byZoom);
}

/** A build shifted so it straddles a zoom boundary, to exercise per-zoom counting. */
function straddling(r: Rng, length: number, gapChance: number) {
  const boundaries = [0, 5, 21, 85, 341, 1365, 5461, 21_845, 1_431_655_765 - length];
  const base = Math.max(0, boundaries[r.int(0, boundaries.length - 1)] - r.int(0, length));
  return randomBuild(r, length, base, gapChance);
}

let scratch: Awaited<ReturnType<typeof scratchDir>>;
beforeEach(async () => {
  scratch = await scratchDir();
});
afterEach(async () => {
  await scratch.cleanup();
});

describe('diffManifests', () => {
  it('matches a per-position comparison on random manifests (property test)', async () => {
    for (let seed = 1; seed <= 300; seed++) {
      const r = rng(seed);
      const gapChance = r.chance(0.3) ? 0.05 : 0;
      const oldBuild = straddling(r, r.int(1, 1500), gapChance);
      let newBuild = mutateBuild(r, oldBuild, gapChance);
      if (r.chance(0.15)) {
        // Different extents: the new build starts later or ends earlier/later.
        const trimStart = r.int(0, 20);
        const extra = Array.from({ length: r.int(0, 30) }, () => (r.chance(0.5) ? 0 : r.int(1, 5)));
        newBuild = { base: newBuild.base + trimStart, contents: [...newBuild.contents.slice(trimStart), ...extra] };
      }
      const oldPath = scratch.path(`${seed}-old.tman.zst`);
      const newPath = scratch.path(`${seed}-new.tman.zst`);
      await writeManifest(oldPath, { buildKey: '20261117' }, expectedEntries(oldBuild));
      await writeManifest(newPath, { buildKey: '20261118' }, expectedEntries(newBuild));
      const result = await runDiff(oldPath, newPath);
      try {
        expectMatches(result, oldBuild, newBuild);
      } catch (error) {
        throw new Error(`seed ${seed}: ${error instanceof Error ? error.message : String(error)}`, { cause: error });
      }
    }
  });

  it('matches a per-position comparison of real builds with different archive layouts (property test)', async () => {
    const layouts: LayoutOptions[] = [
      { splitChance: 0, maxLeafEntries: 40 },
      { splitChance: 0.6, maxLeafEntries: 3, nestChance: 0.3, rootTiles: 4 },
      { splitChance: 0.2, maxLeafEntries: 8, duplicateChance: 0.1 },
      { splitChance: 0.9, maxLeafEntries: 1 },
    ];
    for (let seed = 1; seed <= 16; seed++) {
      const r = rng(1000 + seed);
      const gapChance = seed % 4 === 0 ? 0.03 : 0;
      const oldBuild = straddling(r, r.int(200, 2500), gapChance);
      const newBuild = mutateBuild(r, oldBuild, gapChance);
      const paths: string[] = [];
      for (const [name, build] of [
        ['old', oldBuild],
        ['new', newBuild],
      ] as const) {
        const archivePath = scratch.path(`${seed}-${name}.pmtiles`);
        await writeArchiveFile(archivePath, build, r, layouts[r.int(0, layouts.length - 1)]);
        const out = scratch.path(`${seed}-${name}.tman.zst`);
        await buildManifest(archivePath, {
          out,
          buildKey: name === 'old' ? '20261117' : '20261118',
          threads: r.int(1, 3),
          leavesPerJob: r.int(1, 3),
        });
        paths.push(out);
      }
      const result = await runDiff(paths[0], paths[1]);
      try {
        expectMatches(result, oldBuild, newBuild);
      } catch (error) {
        throw new Error(`seed ${seed}: ${error instanceof Error ? error.message : String(error)}`, { cause: error });
      }
    }
  });

  it('finds nothing between two layouts of the same build', async () => {
    const r = rng(77);
    const build = randomBuild(r, 3000, 341);
    const paths: string[] = [];
    for (const [i, layout] of [
      { splitChance: 0 },
      { splitChance: 0.8, maxLeafEntries: 2, duplicateChance: 0.2 },
    ].entries()) {
      const archivePath = scratch.path(`${i}.pmtiles`);
      await writeArchiveFile(archivePath, build, r, layout);
      paths.push(scratch.path(`${i}.tman.zst`));
      await buildManifest(archivePath, { out: paths[i], buildKey: '20261118', threads: 2, leavesPerJob: 1 });
    }
    const { ranges, totals } = await runDiff(paths[0], paths[1]);
    expect(ranges).toEqual([]);
    expect(totals).toMatchObject({ unchanged: 3000, changed: 0, added: 0, removed: 0, positionsOld: 3000 });
  });

  it('handles a run in one build split across entries of the other and moved ocean boundaries', async () => {
    const h = (n: number) => n.toString(16).toUpperCase().padStart(16, '0');
    const oldPath = scratch.path('old.tman.zst');
    const newPath = scratch.path('new.tman.zst');
    // Old: ocean 0-99, land 100-109, ocean 110-199. New: ocean 0-94, land 95-104 (same bytes from 100), ocean
    // 105-149, a changed tile at 150, ocean to 199.
    await writeManifest(oldPath, { buildKey: '20261117' }, [
      { tileId: 0, runLength: 100, hash: h(0) },
      { tileId: 100, runLength: 10, hash: h(1) },
      { tileId: 110, runLength: 90, hash: h(0) },
    ]);
    await writeManifest(newPath, { buildKey: '20261118' }, [
      { tileId: 0, runLength: 95, hash: h(0) },
      { tileId: 95, runLength: 10, hash: h(1) },
      { tileId: 105, runLength: 45, hash: h(0) },
      { tileId: 150, runLength: 1, hash: h(2) },
      { tileId: 151, runLength: 49, hash: h(0) },
    ]);
    const { ranges, totals } = await runDiff(oldPath, newPath);
    expect(ranges).toEqual([
      { start: 95, count: 5, kind: 'changed' },
      { start: 105, count: 5, kind: 'changed' },
      { start: 150, count: 1, kind: 'changed' },
    ]);
    expect(totals).toMatchObject({ oldBuild: '20261117', newBuild: '20261118', changed: 11, unchanged: 189 });
  });

  it('can only be iterated once', async () => {
    const path = scratch.path('m.tman.zst');
    await writeManifest(path, { buildKey: '20261118' }, [{ tileId: 0, runLength: 1, hash: '0'.repeat(16) }]);
    const diff = await diffManifests(path, path);
    for await (const range of diff) {
      expect(range).toBeUndefined();
    }
    await expect(diff[Symbol.asyncIterator]().next()).rejects.toThrow(/once/);
  });
});

describe('zoomOf', () => {
  it('matches the Hilbert tile numbering', () => {
    for (const [z, x, y] of [
      [0, 0, 0],
      [1, 1, 1],
      [5, 31, 0],
      [12, 4095, 4095],
      [15, 0, 0],
      [15, 32767, 32767],
      [18, 1, 2],
    ]) {
      const tileId = zxyToTileId(z, x, y);
      expect(zoomOf(tileId)).toBe(z);
    }
    expect(zoomOf(zxyToTileId(15, 0, 0) - 1)).toBe(14);
  });
});

describe('changedTiles', () => {
  it('finds the archive tile of every changed and added position', async () => {
    const r = rng(5);
    const oldBuild = randomBuild(r, 2000, 85, 0.02);
    const newBuild = mutateBuild(r, oldBuild, 0.02);
    const oldPath = scratch.path('old.tman.zst');
    const newPath = scratch.path('new.tman.zst');
    await writeManifest(oldPath, { buildKey: '20261117' }, expectedEntries(oldBuild));
    await writeManifest(newPath, { buildKey: '20261118' }, expectedEntries(newBuild));
    const archivePath = scratch.path('new.pmtiles');
    const archive = await writeArchiveFile(archivePath, newBuild, r, { splitChance: 0.5, maxLeafEntries: 3 });

    const { archive: reader, close } = await openArchiveFile(archivePath);
    const tiles = [];
    for await (const tile of changedTiles(reader.tileEntries(), await diffManifests(oldPath, newPath))) {
      tiles.push(tile);
    }
    await close();

    const expected = bruteForce(oldBuild, newBuild)
      .ranges.filter((range) => range.kind !== 'removed')
      .flatMap((range) => Array.from({ length: range.count }, (_, i) => range.start + i))
      .map((tileId) => {
        const content = newBuild.contents[tileId - newBuild.base]!;
        return { tileId, offset: archive.offsets[tileId - newBuild.base], length: contentBytes(content).length };
      });
    expect(tiles.length).toBeGreaterThan(0);
    expect(tiles).toEqual(expected);
  });

  it('rejects positions the archive does not have', async () => {
    const entries = (async function* () {
      yield [{ tileId: 0, offset: 0, length: 5, runLength: 2, chunkId: 0 }];
    })();
    const iterate = async () => {
      for await (const tile of changedTiles(entries, [{ start: 1, count: 3, kind: 'changed' }])) {
        expect(tile.tileId).toBe(1);
      }
    };
    await expect(iterate()).rejects.toThrow(/Tile 2 is not in the archive/);
  });
});
