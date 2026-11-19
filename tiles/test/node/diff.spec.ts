import { execFile } from 'node:child_process';
import { link, mkdir, readdir, readFile, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ArchiveRanges } from '../../src/bunny/archive-walk';
import { DiffKind, diffManifests, DiffRange, DiffTotals, planSync, SyncRange, zoomOf } from '../../src/bunny/diff';
import { buildManifest, ManifestEntry, writeManifest } from '../../src/bunny/manifest';
import { sha256Hex } from '../../src/bunny/state';
import { planItems } from '../../src/bunny/sync';
import { MAX_SAFE_ZOOM, zoomEnd } from '../../src/bunny/tile-id';
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

  it('rejects changed, added, removed and unchanged positions past zoom 26 instead of hanging', async () => {
    const end = zoomEnd(MAX_SAFE_ZOOM);
    const cases: [ManifestEntry[], ManifestEntry[]][] = [
      // A changed run crossing into zoom 27, one changed position at its first ID, one added, one removed.
      [
        [{ tileId: end - 1, runLength: 2, hash: 'A'.repeat(16) }],
        [{ tileId: end - 1, runLength: 2, hash: 'B'.repeat(16) }],
      ],
      [[{ tileId: end, runLength: 1, hash: 'A'.repeat(16) }], [{ tileId: end, runLength: 1, hash: 'B'.repeat(16) }]],
      [[], [{ tileId: end + 5, runLength: 1, hash: 'A'.repeat(16) }]],
      [[{ tileId: end + 5, runLength: 1, hash: 'A'.repeat(16) }], []],
      // The same bytes in both builds: no range to count, but still no position a manifest can describe.
      [
        [{ tileId: end - 1, runLength: 2, hash: 'A'.repeat(16) }],
        [{ tileId: end - 1, runLength: 2, hash: 'A'.repeat(16) }],
      ],
      [
        [{ tileId: end + 5, runLength: 1, hash: 'A'.repeat(16) }],
        [{ tileId: end + 5, runLength: 1, hash: 'A'.repeat(16) }],
      ],
    ];
    for (const [i, [before, after]] of cases.entries()) {
      const oldPath = scratch.path(`old-${i}.tman.zst`);
      const newPath = scratch.path(`new-${i}.tman.zst`);
      await writeManifest(oldPath, { buildKey: '20261117' }, before);
      await writeManifest(newPath, { buildKey: '20261118' }, after);
      await expect(runDiff(oldPath, newPath), `case ${i}`).rejects.toThrow(/past zoom 26/);
    }
    // The last position of zoom 26 still counts.
    const oldPath = scratch.path('old-last.tman.zst');
    const newPath = scratch.path('new-last.tman.zst');
    await writeManifest(oldPath, { buildKey: '20261117' }, [{ tileId: end - 1, runLength: 1, hash: 'A'.repeat(16) }]);
    await writeManifest(newPath, { buildKey: '20261118' }, [{ tileId: end - 1, runLength: 1, hash: 'B'.repeat(16) }]);
    const { ranges, totals } = await runDiff(oldPath, newPath);
    expect(ranges).toEqual([{ start: end - 1, count: 1, kind: 'changed' }]);
    expect(totals.byZoom[26]).toEqual({ changed: 1, added: 0, removed: 0 });
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

/** The upload items planItems (the sync's uploader input) makes of a plan over an archive. */
async function itemsOf(archivePath: string, ranges: AsyncIterable<SyncRange> | Iterable<SyncRange>) {
  const archive = await ArchiveRanges.open(archivePath);
  try {
    const items = [];
    for await (const item of planItems(archive, ranges)) {
      items.push({ tileId: item.tileId, sha256: item.sha256, verify: item.verify });
    }
    return items;
  } finally {
    await archive.close();
  }
}

describe('planItems over a diff', () => {
  it('finds the archive tile of every changed and added position', async () => {
    const r = rng(5);
    const oldBuild = randomBuild(r, 2000, 85, 0.02);
    const newBuild = mutateBuild(r, oldBuild, 0.02);
    const oldPath = scratch.path('old.tman.zst');
    const newPath = scratch.path('new.tman.zst');
    await writeManifest(oldPath, { buildKey: '20261117' }, expectedEntries(oldBuild));
    await writeManifest(newPath, { buildKey: '20261118' }, expectedEntries(newBuild));
    const archivePath = scratch.path('new.pmtiles');
    await writeArchiveFile(archivePath, newBuild, r, { splitChance: 0.5, maxLeafEntries: 3 });

    const items = await itemsOf(archivePath, await planSync(oldPath, newPath));
    const expected = bruteForce(oldBuild, newBuild)
      .ranges.filter((range) => range.kind !== 'removed')
      .flatMap((range) => Array.from({ length: range.count }, (_, i) => range.start + i))
      .map((tileId) => ({
        tileId,
        sha256: sha256Hex(contentBytes(newBuild.contents[tileId - newBuild.base]!)),
        verify: false,
      }));
    expect(items.length).toBeGreaterThan(0);
    expect(items).toEqual(expected);
  });

  it('marks the positions a plan must check and rejects positions the archive does not have', async () => {
    const r = rng(6);
    const build: Build = { base: 0, contents: [1, 1, 2, 3, 3, 3, 4, 5, 6, 7] };
    const archivePath = scratch.path('small.pmtiles');
    await writeArchiveFile(archivePath, build, r, { splitChance: 0.5, maxLeafEntries: 2 });
    const items = await itemsOf(archivePath, [
      { start: 1, count: 2, kind: 'changed', verify: true },
      { start: 3, count: 1, kind: 'removed', verify: true },
      { start: 4, count: 1, kind: 'added', verify: false },
    ]);
    expect(items.map((item) => [item.tileId, item.verify])).toEqual([
      [1, true],
      [2, true],
      [4, false],
    ]);
    await expect(itemsOf(archivePath, [{ start: 8, count: 3, kind: 'changed', verify: false }])).rejects.toThrow(
      /Tile 10 is not in/,
    );
  });
});

const hashAt = (build: Build, tileId: number) => {
  const content = build.contents[tileId - build.base];
  return content === null || content === undefined ? null : contentHash(content);
};

/** The plan by brute force, position by position (see SyncPlan). */
function bruteForcePlan(live: Build, next: Build, pending: Build[]) {
  const builds = [live, next, ...pending];
  const first = Math.min(...builds.map((b) => b.base));
  const last = Math.max(...builds.map((b) => b.base + b.contents.length));
  const ranges: SyncRange[] = [];
  let verify = 0;
  let verifyOnly = 0;
  for (let tileId = first; tileId < last; tileId++) {
    const before = hashAt(live, tileId);
    const after = hashAt(next, tileId);
    const listed = before !== after;
    const touched = pending.some((build) => hashAt(build, tileId) !== before);
    if (!listed && !touched) {
      continue;
    }
    let kind: DiffKind;
    if (listed) {
      kind = before === null ? 'added' : after === null ? 'removed' : 'changed';
    } else {
      kind = before === null ? 'removed' : 'changed';
      verifyOnly++;
    }
    if (touched) {
      verify++;
    }
    const previous = ranges[ranges.length - 1];
    if (
      previous &&
      previous.kind === kind &&
      previous.verify === touched &&
      previous.start + previous.count === tileId
    ) {
      previous.count++;
    } else {
      ranges.push({ start: tileId, count: 1, kind, verify: touched });
    }
  }
  return { ranges, verify, verifyOnly };
}

async function runPlan(livePath: string, newPath: string, pendingPaths: string[]) {
  const plan = await planSync(livePath, newPath, pendingPaths);
  const ranges: SyncRange[] = [];
  for await (const range of plan) {
    ranges.push(range);
  }
  return { ranges, totals: plan.totals };
}

/** Writes a build's manifest under a fresh name. */
let manifestCount = 0;
async function manifestOf(build: Build, buildKey: string) {
  const path = scratch.path(`m${manifestCount++}.tman.zst`);
  await writeManifest(path, { buildKey }, expectedEntries(build));
  return path;
}

/**
 * A tiles zone: the content hash per position. Writes follow the uploader's rules: a `verify` position is DESCRIBEd
 * first and only written when it holds other bytes; any PUT of the bytes already there is counted, since on Bunny it
 * clears the file's replica list for good.
 */
class Zone {
  files = new Map<number, string>();
  identicalPuts = 0;
  writes = 0;

  static of(build: Build) {
    const zone = new Zone();
    build.contents.forEach((_, i) => {
      const hash = hashAt(build, build.base + i);
      if (hash !== null) {
        zone.files.set(build.base + i, hash);
      }
    });
    return zone;
  }

  apply(range: SyncRange, tileId: number, build: Build) {
    const want = range.kind === 'removed' ? null : hashAt(build, tileId);
    const have = this.files.get(tileId) ?? null;
    if (range.verify && have === want) {
      return;
    }
    if (want === null) {
      this.files.delete(tileId);
    } else {
      if (have === want) {
        this.identicalPuts++;
      }
      this.files.set(tileId, want);
    }
    this.writes++;
  }

  /** Positions where the zone doesn't hold the build. */
  stale(build: Build) {
    const expected = Zone.of(build).files;
    const positions = new Set([...expected.keys(), ...this.files.keys()]);
    return [...positions].filter((p) => expected.get(p) !== this.files.get(p)).length;
  }
}

describe('planSync', () => {
  it('is the plain diff without pending builds', async () => {
    const r = rng(11);
    const live = randomBuild(r, 1200, 85, 0.02);
    const next = mutateBuild(r, live, 0.02);
    const [livePath, newPath] = [await manifestOf(live, '20261117'), await manifestOf(next, '20261118')];
    const plan = await runPlan(livePath, newPath, []);
    const diff = await runDiff(livePath, newPath);
    expect(plan.ranges).toEqual(diff.ranges.map((range) => ({ ...range, verify: false })));
    expect(plan.totals).toMatchObject({ diff: diff.totals, pendingBuilds: [], verify: 0, verifyOnly: 0 });
  });

  it('matches a per-position plan with pending builds (property test)', async () => {
    for (let seed = 1; seed <= 150; seed++) {
      const r = rng(5000 + seed);
      const gapChance = r.chance(0.3) ? 0.05 : 0;
      const live = straddling(r, r.int(1, 1200), gapChance);
      const pending = Array.from({ length: r.int(1, 3) }, () => mutateBuild(r, live, gapChance));
      // A re-run plans with the new build among the pending ones.
      const next = r.chance(0.2) ? pending[pending.length - 1] : mutateBuild(r, pending[0], gapChance);
      if (r.chance(0.15)) {
        pending[0] = { base: pending[0].base + r.int(0, 20), contents: [...pending[0].contents, 0, 3, 3] };
      }
      const livePath = await manifestOf(live, '20261117');
      const newPath = await manifestOf(next, '20261121');
      const pendingPaths = [];
      for (const [i, build] of pending.entries()) {
        pendingPaths.push(await manifestOf(build, `202611${18 + i}`));
      }
      const result = await runPlan(livePath, newPath, pendingPaths);
      const expected = bruteForcePlan(live, next, pending);
      try {
        expect(result.ranges).toEqual(expected.ranges);
        expect(result.totals).toMatchObject({
          verify: expected.verify,
          verifyOnly: expected.verifyOnly,
          ranges: expected.ranges.length,
          pendingBuilds: pending.map((_, i) => `202611${18 + i}`),
        });
        expectMatches({ ranges: (await runDiff(livePath, newPath)).ranges, totals: result.totals.diff }, live, next);
      } catch (error) {
        throw new Error(`seed ${seed}: ${error instanceof Error ? error.message : String(error)}`, { cause: error });
      }
    }
  });

  /**
   * Days of syncs, some dying part-way (after a random share of their writes, or with random writes lost), against
   * a model of the zone. `track` is what state.json does: false plans every day with the plain live-to-new diff.
   */
  async function simulate(seed: number, track: boolean) {
    const r = rng(seed);
    let live = randomBuild(r, r.int(100, 700), 341, 0.02);
    let livePath = await manifestOf(live, '20261101');
    const zone = Zone.of(live);
    let pending: { build: Build; path: string }[] = [];
    let stale = 0;
    let previous = live;
    for (let day = 2; day <= 9; day++) {
      // Edits revert: a day's build often goes back to an earlier day's bytes in places.
      const build = r.chance(0.4) ? mutateBuild(r, live, 0.02) : mutateBuild(r, previous, 0.02);
      previous = build;
      const path = await manifestOf(build, `202611${String(day).padStart(2, '0')}`);
      const plan = await runPlan(livePath, path, track ? pending.map((p) => p.path) : []);
      if (track) {
        pending.push({ build, path });
      }
      const positions = plan.ranges.flatMap((range) =>
        Array.from({ length: range.count }, (_, i) => ({ range, tileId: range.start + i })),
      );
      const fails = day < 9 && r.chance(0.6);
      const stopAt = fails && r.chance(0.5) ? r.int(0, positions.length) : positions.length;
      const lose = fails && stopAt === positions.length ? 0.3 : 0;
      for (const { range, tileId } of positions.slice(0, stopAt)) {
        if (!r.chance(lose)) {
          zone.apply(range, tileId, build);
        }
      }
      if (!fails) {
        live = build;
        livePath = path;
        pending = [];
        stale += zone.stale(live);
      }
    }
    return { stale, identicalPuts: zone.identicalPuts };
  }

  it('brings the zone to the live build after failed syncs without re-sending identical bytes (simulation)', async () => {
    let plainStale = 0;
    let plainIdentical = 0;
    for (let seed = 1; seed <= 40; seed++) {
      const tracked = await simulate(9000 + seed, true);
      expect(tracked, `seed ${seed}`).toEqual({ stale: 0, identicalPuts: 0 });
      const plain = await simulate(9000 + seed, false);
      plainStale += plain.stale;
      plainIdentical += plain.identicalPuts;
    }
    // Planning with the live manifest alone, as before pending builds were recorded, gets both wrong.
    expect(plainStale).toBeGreaterThan(0);
    expect(plainIdentical).toBeGreaterThan(0);
  });
});

describe('bunny:diff', () => {
  const TILES_DIR = join(import.meta.dirname, '../..');
  const cli = (...args: string[]) =>
    promisify(execFile)(process.execPath, ['--import=tsx', 'src/bunny/cli/diff.ts', ...args], { cwd: TILES_DIR }).then(
      (result) => ({ code: 0, ...result }),
      (error: { code: number; stdout: string; stderr: string }) => error,
    );
  const ndjson = async (path: string) =>
    (await readFile(path, 'utf8'))
      .split('\n')
      .filter(Boolean)
      .map((line) => JSON.parse(line) as SyncRange);

  async function builds() {
    const r = rng(21);
    const live = randomBuild(r, 30_000, 341, 0.02);
    const next = mutateBuild(r, live, 0.02);
    await mkdir(scratch.path('out'));
    return { live, next, livePath: await manifestOf(live, '20261117'), newPath: await manifestOf(next, '20261118') };
  }

  it('writes the ranges to a file of its own and puts it in place once complete', async () => {
    const { live, next, livePath, newPath } = await builds();
    const ranges = scratch.path('out/ranges.ndjson');
    await writeFile(ranges, 'from an earlier run\n');
    const result = await cli('--old', livePath, '--new', newPath, '--ranges', ranges);
    expect(result).toMatchObject({ code: 0 });
    expect(JSON.parse(result.stdout)).toMatchObject({ oldBuild: '20261117', newBuild: '20261118' });
    expect(await ndjson(ranges)).toEqual(bruteForce(live, next).ranges);
    expect(await readdir(scratch.path('out'))).toEqual(['ranges.ndjson']);

    const pendingPath = await manifestOf(mutateBuild(rng(22), live, 0.02), '20261119');
    const planned = await cli('--old', livePath, '--new', newPath, '--pending', pendingPath, '--ranges', ranges);
    expect(planned).toMatchObject({ code: 0 });
    expect((await ndjson(ranges)).every((range) => typeof range.verify === 'boolean')).toBe(true);
  });

  it('refuses a ranges file that is one of the manifests, also through a link', async () => {
    const { livePath, newPath } = await builds();
    const pendingPath = await manifestOf(randomBuild(rng(23), 100, 0), '20261119');
    await link(newPath, scratch.path('out/hard.ndjson'));
    await symlink(pendingPath, scratch.path('out/soft.ndjson'));
    const before = await Promise.all([livePath, newPath, pendingPath].map((path) => readFile(path)));
    for (const ranges of [livePath, scratch.path('out/hard.ndjson'), scratch.path('out/soft.ndjson')]) {
      const refused = await cli('--old', livePath, '--new', newPath, '--pending', pendingPath, '--ranges', ranges);
      expect(refused, ranges).toMatchObject({ code: 1 });
      expect(refused.stderr).toMatch(/is the manifest .*: writing it would destroy the input/);
    }
    const after = await Promise.all([livePath, newPath, pendingPath].map((path) => readFile(path)));
    expect(after).toEqual(before);
    expect(await readdir(scratch.path('out'))).toEqual(['hard.ndjson', 'soft.ndjson']);
  });

  it('fails through its own error path when the ranges file cannot be written or the plan fails', async () => {
    const { livePath, newPath } = await builds();
    const unwritable = await cli('--old', livePath, '--new', newPath, '--ranges', scratch.path('missing/ranges'));
    expect(unwritable).toMatchObject({ code: 1 });
    expect(unwritable.stderr).toMatch(/ENOENT/);
    expect(unwritable.stderr).not.toMatch(/Unhandled 'error' event/);

    // A plan that fails part-way leaves an earlier ranges file as it was, and no partial file.
    const ranges = scratch.path('out/ranges.ndjson');
    await writeFile(ranges, 'from an earlier run\n');
    const end = zoomEnd(MAX_SAFE_ZOOM);
    const broken = scratch.path('broken.tman.zst');
    await writeManifest(broken, { buildKey: '20261118' }, [
      ...expectedEntries(randomBuild(rng(24), 30_000, 341, 0.02)),
      { tileId: end, runLength: 1, hash: 'A'.repeat(16) },
    ]);
    const failed = await cli('--old', livePath, '--new', broken, '--ranges', ranges);
    expect(failed).toMatchObject({ code: 1 });
    expect(failed.stderr).toMatch(/go past zoom 26/);
    expect(await readFile(ranges, 'utf8')).toBe('from an earlier run\n');
    expect(await readdir(scratch.path('out'))).toEqual(['ranges.ndjson']);
  });
});
