import { createHash } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { readStyles, rewriteStyle, staticFiles, StyleSource } from '../../src/bunny/static-files';
import { openArchiveFile } from '../../src/deploy/archive-file';
import { IDatabaseRepository, IMemCacheRepository, IMetricsRepository, IStorageRepository } from '../../src/interface';
import { PMTilesService } from '../../src/pmtiles.service';
import { Header, Metadata } from '../../src/pmtiles/types';
import { tileJSON } from '../../src/pmtiles/utils';
import { scratchDir } from './manifest-fixtures';

const asset = (name: string) => fileURLToPath(new URL(`../assets/${name}`, import.meta.url));
const PROD = 'https://tiles.immich.cloud';
const text = (body: Uint8Array) => new TextDecoder().decode(body);

let header: Header;
let metadata: Metadata;
let styles: StyleSource[];
beforeEach(async () => {
  const { archive, close } = await openArchiveFile(asset('tiles.pmtiles'));
  header = archive.header;
  metadata = archive.metadata;
  await close();
  styles = await Promise.all(
    ['dark', 'light'].map(async (name) => ({
      name,
      body: new Uint8Array(await readFile(asset(`styles/${name}.json`))),
    })),
  );
});

/** The TileJSON body the Worker sends today: publish stores tileJSON() in D1, the Worker sets the tile URL. */
async function workerTileJson(url: string, deploymentKey: string) {
  const stored = JSON.parse(JSON.stringify(tileJSON({ header, metadata, version: 'v1' })));
  const memCache = new Map<string, unknown>([[deploymentKey, stored]]);
  const service = await PMTilesService.init(
    { getDeploymentKey: () => deploymentKey } as IStorageRepository,
    {
      get: (key: string) => memCache.get(key),
      set: (key: string, value: unknown) => memCache.set(key, value),
    } as IMemCacheRepository,
    {} as IMetricsRepository,
    {} as IDatabaseRepository,
  );
  return JSON.stringify(await service.getJsonResponse('1', new URL(url)));
}

describe('staticFiles', () => {
  it('serves the TileJSON the Worker serves, with stable tile URLs', async () => {
    const files = staticFiles({ header, metadata, styles, publicUrl: PROD });
    const tileJsonFile = files.find((file) => file.path === 'v1.json')!;

    const worker = await workerTileJson(`${PROD}/v1.json`, '20261101');
    const expected = worker.replace(
      JSON.stringify(`${PROD}/v1/{z}/{x}/{y}.mvt?v=20261101`),
      JSON.stringify(`${PROD}/v1/{z}/{x}/{y}.mvt`),
    );
    expect(expected).not.toBe(worker);
    expect(text(tileJsonFile.body)).toBe(expected);
    expect(JSON.parse(text(tileJsonFile.body))).toMatchObject({
      tilejson: '3.0.0',
      scheme: 'xyz',
      tiles: [`${PROD}/v1/{z}/{x}/{y}.mvt`],
      minzoom: header.minZoom,
      maxzoom: header.maxZoom,
    });
  });

  it('keeps prod styles byte for byte at both paths the Worker serves', () => {
    const files = staticFiles({ header, metadata, styles, publicUrl: `${PROD}/` });
    expect(files.map((file) => file.path)).toEqual([
      'v1.json',
      'v1/style/dark.json',
      'v1/style/dark',
      'v1/style/light.json',
      'v1/style/light',
    ]);
    for (const style of styles) {
      for (const path of [`v1/style/${style.name}.json`, `v1/style/${style.name}`]) {
        expect(files.find((file) => file.path === path)!.body).toEqual(style.body);
      }
    }
    for (const file of files) {
      expect(file.sha256).toBe(createHash('sha256').update(file.body).digest('hex').toUpperCase());
      expect(file.contentType).toBe('application/json');
    }
  });

  it('points everything at another host and prefix', () => {
    const files = staticFiles({
      header,
      metadata,
      styles,
      publicUrl: 'https://tiles-test.example',
      prefix: '/itest-b-1/v1/',
      staticPrefix: 'itest-b-1/v1',
    });
    expect(files.map((file) => file.path)).toEqual([
      'itest-b-1/v1.json',
      'itest-b-1/v1/style/dark.json',
      'itest-b-1/v1/style/dark',
      'itest-b-1/v1/style/light.json',
      'itest-b-1/v1/style/light',
    ]);
    expect(JSON.parse(text(files[0].body)).tiles).toEqual(['https://tiles-test.example/itest-b-1/v1/{z}/{x}/{y}.mvt']);
    const style = JSON.parse(text(files[1].body));
    const original = JSON.parse(text(styles[0].body));
    expect(style.sources.vector.url).toBe('https://tiles-test.example/itest-b-1/v1.json');
    expect({ ...style, sources: original.sources }).toEqual(original);
    // Same two-space form as map-styles writes.
    expect(text(files[1].body)).toBe(JSON.stringify(style, null, 2));
  });

  it('keeps the TileJSON and styles at the static prefix when the tiles move (a full rewrite)', () => {
    const files = staticFiles({ header, metadata, styles, publicUrl: PROD, prefix: 'v1/e2' });
    expect(files.map((file) => file.path)).toEqual([
      'v1.json',
      'v1/style/dark.json',
      'v1/style/dark',
      'v1/style/light.json',
      'v1/style/light',
    ]);
    expect(JSON.parse(text(files[0].body)).tiles).toEqual([`${PROD}/v1/e2/{z}/{x}/{y}.mvt`]);
    expect(JSON.parse(text(files[1].body)).sources.vector.url).toBe(`${PROD}/v1.json`);
  });

  it('rejects what the zone could not serve', () => {
    const options = { header, metadata, styles, publicUrl: PROD };
    expect(() => staticFiles({ ...options, styles: [] })).toThrow(/No styles/);
    expect(() => staticFiles({ ...options, styles: [{ name: 'my-style', body: styles[0].body }] })).toThrow(/address/);
    expect(() => staticFiles({ ...options, prefix: 'a/../b' })).toThrow(/Invalid prefix/);
    expect(() => staticFiles({ ...options, staticPrefix: '/' })).toThrow(/Invalid prefix/);
    expect(() => staticFiles({ ...options, publicUrl: `${PROD}?x=1` })).toThrow(/query/);
    const noSource = { name: 'bare', body: new TextEncoder().encode('{"version":8,"sources":{}}') };
    expect(() => rewriteStyle(noSource, `${PROD}/v1.json`)).toThrow(/no "vector" source/);
  });
});

describe('readStyles', () => {
  let scratch: Awaited<ReturnType<typeof scratchDir>>;
  beforeEach(async () => {
    scratch = await scratchDir();
  });
  afterEach(async () => {
    await scratch.cleanup();
  });

  it('reads a map-styles build by name', async () => {
    const dir = scratch.path('dist');
    await mkdir(dir);
    await writeFile(`${dir}/light.json`, '{"a":1}');
    await writeFile(`${dir}/black.json`, '{"b":2}');
    await writeFile(`${dir}/README.md`, 'not a style');
    const read = await readStyles(dir);
    expect(read.map((style) => [style.name, text(style.body)])).toEqual([
      ['black', '{"b":2}'],
      ['light', '{"a":1}'],
    ]);
  });
});
