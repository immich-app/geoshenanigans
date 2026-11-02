import { createExecutionContext, env, waitOnExecutionContext } from 'cloudflare:test';
import worker, { cachePolicy, etagMatches, parseUrl } from '../src';
import { IDatabaseRepository, IMemCacheRepository, IMetricsRepository, IStorageRepository } from '../src/interface';
import { DirectoryString, PMTilesService } from '../src/pmtiles.service';
import { JsonResponse } from '../src/pmtiles/types';
import { fromRadix64, toRadix64 } from '../src/pmtiles/utils';

// For now, you'll need to do something like this to get a correctly-typed
// `Request` to pass to `worker.fetch()`.
const IncomingRequest = Request<unknown, IncomingRequestCfProperties>;

// describe('integration tests', () => {
//   describe('success - zoom1', () => {
//     beforeAll(async () => {
//       const buckets = [env.BUCKET_WEUR, env.BUCKET_WNAM, env.BUCKET_EEUR, env.BUCKET_APAC, env.BUCKET_ENAM];
//       const files = ['tiles.pmtiles', 'styles/dark.json', 'styles/light.json'];
//       for (const bucket of buckets) {
//         for (const fileName of files) {
//           const file = await fetch(`http://localhost:${inject('port')}/${fileName}`);
//           if (!file.body) {
//             throw new Error('File body is undefined');
//           }
//           const body = file.body as ReadableStream;
//           await bucket.put(`${env.DEPLOYMENT_KEY}/${fileName}`, body);
//           console.log('File uploaded');
//         }
//       }
//     }, 30000);
//     it('responds with correct json file', async () => {
//       const response = await SELF.fetch('https://example.com/v1');
//       await expect(JSON.stringify(await response.json(), null, 2)).toMatchFileSnapshot('./__snapshots__/v1.json');
//     });
//     it('responds with correct style json amended with current URL', async () => {
//       const response = await SELF.fetch('https://example.com/v1/style/dark');
//       await expect(JSON.stringify(await response.json(), null, 2)).toMatchFileSnapshot(
//         './__snapshots__/styles/dark.json',
//       );
//     });
//     // TODO: Come back to these and write up a way to populate local D1 on test setup
//     it.skip('responds with correct tile', async () => {
//       const response = await SELF.fetch('https://example.com/v1/0/0/0.mvt');
//       expect(response.status).toBe(200);
//       await expect(Buffer.from(await response.arrayBuffer()).toString()).toMatchFileSnapshot(
//         './__snapshots__/v1-0-0-0.mvt',
//       );
//     });
//     it.skip('responds with correct tile 2', async () => {
//       const response = await SELF.fetch('https://example.com/v1/1/0/1.mvt');
//       expect(response.status).toBe(200);
//       await expect(Buffer.from(await response.arrayBuffer()).toString()).toMatchFileSnapshot(
//         './__snapshots__/v1-1-0-1.mvt',
//       );
//     });
//     it('responds with error when tile out of bounds', async () => {
//       const response = await SELF.fetch('https://example.com/v1/0/0/1.mvt');
//       expect(response.status).toBe(500);
//     });
//   });
// });

describe.skip('Hello World worker', () => {
  it('responds with Hello World! (unit style)', async () => {
    const request = new IncomingRequest('http://example.com');
    // Create an empty context to pass to `worker.fetch()`.
    const ctx = createExecutionContext();
    const response = await worker.fetch(request, env, ctx);
    // Wait for all `Promise`s passed to `ctx.waitUntil()` to settle before running test assertions
    await waitOnExecutionContext(ctx);
    expect(await response.text()).toMatchInlineSnapshot(`"Hello World!"`);
  });
});

describe('parseUrl', () => {
  it.each`
    url                             | version
    ${'http://example.com/v1'}      | ${'1'}
    ${'http://example.com/v2'}      | ${'2'}
    ${'http://example.com/v1.json'} | ${'1'}
    ${'http://example.com/v2.json'} | ${'2'}
  `('json endpoint $url returns version $version', ({ url, version }) => {
    const result = parseUrl(new Request(url));
    expect(result).toStrictEqual({
      requestType: 'json',
      version,
      url: new URL(url),
    });
  });

  it.each`
    url                                         | style      | version
    ${'http://example.com/v1/style/dark'}       | ${'dark'}  | ${'1'}
    ${'http://example.com/v2/style/light'}      | ${'light'} | ${'2'}
    ${'http://example.com/v2/style/dark.json'}  | ${'dark'}  | ${'2'}
    ${'http://example.com/v1/style/light.json'} | ${'light'} | ${'1'}
  `('style endpoint $url returns style $style', ({ url, style, version }) => {
    const result = parseUrl(new Request(url));
    expect(result).toStrictEqual({
      requestType: 'style',
      style,
      url: new URL(url),
      version,
    });
  });

  it.each`
    url                                  | version | z      | x      | y
    ${'http://example.com/v1/0/0/0.mvt'} | ${'1'}  | ${'0'} | ${'0'} | ${'0'}
    ${'http://example.com/v2/1/2/3.mvt'} | ${'2'}  | ${'1'} | ${'2'} | ${'3'}
  `('tile endpoint $url returns tile $z/$x/$y with version $version', ({ url, version, z, x, y }) => {
    const result = parseUrl(new Request(url));
    expect(result).toStrictEqual({
      requestType: 'tile',
      version,
      z,
      x,
      y,
      url: new URL(url),
    });
  });

  it.each`
    url                                              | key
    ${'http://example.com/v1/0/0/0.mvt?v=20261102'}  | ${'20261102'}
    ${'http://example.com/v1/14/1/2.mvt?v=20250818'} | ${'20250818'}
    ${'http://example.com/v1/14/1/2.mvt?v=2026'}     | ${undefined}
    ${'http://example.com/v1/14/1/2.mvt?v=abcdefgh'} | ${undefined}
  `('tile endpoint $url has deployment version $key', ({ url, key }) => {
    expect((parseUrl(new Request(url)) as { deploymentKey?: string }).deploymentKey).toBe(key);
  });

  it.each`
    url
    ${'http://example.com/v1.jason'}
    ${'http://example.com/v2/1/2/3'}
    ${'http://example.com/v2/1/2/3.mvt/extra'}
    ${'http://example.com/v1/1/2.mvt'}
  `('url $url returns undefined request type', ({ url }) => {
    const result = parseUrl(new Request(url));
    expect(result).toStrictEqual({
      requestType: undefined,
      url: new URL(url),
    });
  });
});

describe('radix64', () => {
  it.each`
    number
    ${1}
    ${0}
    ${-1}
    ${100}
    ${1769420107}
    ${663668044}
    ${-1105752063}
  `('converts $number to radix64 and back correctly', ({ number }) => {
    const radix64 = toRadix64(number);
    expect(fromRadix64(radix64)).toEqual(number);
  });
});

describe('cachePolicy', () => {
  const live = '20261102';
  const policy = (url: string) => cachePolicy(parseUrl(new Request(`https://tiles.example${url}`)), live);

  it.each`
    url                           | cacheControl                                               | etag
    ${'/v1/1/2/3.mvt'}            | ${'public, max-age=86400, stale-while-revalidate=2592000'} | ${'"20261102"'}
    ${'/v1/1/2/3.mvt?v=20261102'} | ${'public, max-age=31536000, immutable'}                   | ${undefined}
    ${'/v1/1/2/3.mvt?v=20250818'} | ${'public, max-age=3600'}                                  | ${undefined}
    ${'/v1.json'}                 | ${'public, max-age=3600'}                                  | ${'"20261102"'}
    ${'/v1/style/dark.json'}      | ${'public, max-age=3600'}                                  | ${'"20261102"'}
    ${'/v1/nonsense'}             | ${'public, max-age=2678400'}                               | ${undefined}
  `('$url gets $cacheControl', ({ url, cacheControl, etag }) => {
    expect(policy(url)).toStrictEqual(etag ? { cacheControl, etag } : { cacheControl });
  });
});

describe('etagMatches', () => {
  it.each`
    header                      | matches
    ${'"20261102"'}             | ${true}
    ${'W/"20261102"'}           | ${true}
    ${'"20250818", "20261102"'} | ${true}
    ${'*'}                      | ${false}
    ${'"20250818"'}             | ${false}
    ${null}                     | ${false}
  `('If-None-Match $header matches: $matches', ({ header, matches }) => {
    expect(etagMatches(header, '"20261102"')).toBe(matches);
  });
});

describe('conditional requests', () => {
  const key = env.DEPLOYMENT_KEY;
  const etag = `"${key}"`;
  const fetchWith = async (url: string, headers: Record<string, string> = {}) => {
    const ctx = createExecutionContext();
    const response = await worker.fetch(new IncomingRequest(url, { headers }), env, ctx);
    await waitOnExecutionContext(ctx);
    return response;
  };
  // A directory whose only tile is tile 0, so other tiles in range are absent without any storage read.
  const directory = DirectoryString.fromDirectory({
    tileIdStart: 0,
    offsetStart: 0,
    entries: [{ tileId: 0, offset: 0, length: 10, runLength: 1, chunkId: 0 }],
  }).toString();

  beforeAll(async () => {
    const tileJson = { tilejson: '3.0.0', tiles: [], vector_layers: [], minzoom: 0, maxzoom: 15 };
    await env.D1_GLOBAL.batch([
      env.D1_GLOBAL.prepare(
        `CREATE TABLE IF NOT EXISTS cache_entries_${key} (startTileId INTEGER PRIMARY KEY, entry TEXT NOT NULL)`,
      ),
      env.D1_GLOBAL.prepare(`INSERT OR REPLACE INTO cache_entries_${key} VALUES (-1, ?), (0, ?)`).bind(
        JSON.stringify(tileJson),
        directory,
      ),
    ]);
  });

  it('does not reuse CDN entries stored before the current cache revision', async () => {
    const url = 'https://tiles.example/v1.json?case=revision';
    await caches.default.put(
      url,
      new Response('{"tiles":["old"]}', {
        headers: { 'PMTiles-Deployment-Key': key, 'Cache-Control': 'public, max-age=2678400' },
      }),
    );
    const response = await fetchWith(url);
    expect(response.status).toBe(200);
    expect(response.headers.get('Cache-Control')).toBe('public, max-age=3600');
    expect(((await response.json()) as { tiles: string[] }).tiles).toEqual([
      `https://tiles.example/v1/{z}/{x}/{y}.mvt?v=${key}`,
    ]);
  });

  it('answers 304 from a CDN hit the client already has', async () => {
    const url = 'https://tiles.example/v1/14/8187/5448.mvt?case=cdn';
    await caches.default.put(
      url,
      new Response('tile', {
        headers: {
          'PMTiles-Deployment-Key': key,
          'Tiles-Cache-Revision': '2',
          'Cache-Control': 'public, max-age=86400',
          'Access-Control-Allow-Origin': '*',
          ETag: etag,
        },
      }),
    );
    const response = await fetchWith(url, { 'If-None-Match': `W/${etag}` });
    expect(response.status).toBe(304);
    expect(response.headers.get('ETag')).toBe(etag);
    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*');
    expect(await response.text()).toBe('');
  });

  it('answers 304 once a fresh response turns out to be what the client has', async () => {
    const response = await fetchWith('https://tiles.example/v1.json?case=fresh', { 'If-None-Match': etag });
    expect(response.status).toBe(304);
    expect(response.headers.get('Cache-Control')).toBe('public, max-age=3600');
  });

  it.each`
    tile        | reason
    ${'1/0/0'}  | ${'absent from the directory'}
    ${'16/0/0'} | ${'beyond maxzoom'}
  `('keeps the 404 for a tile $reason despite a matching ETag', async ({ tile }) => {
    const response = await fetchWith(`https://tiles.example/v1/${tile}.mvt?case=missing`, { 'If-None-Match': etag });
    expect(response.status).toBe(404);
  });
});

describe('TileJSON', () => {
  it("points clients at the live deployment's versioned tile URLs", async () => {
    const tileJson = { tiles: [] as string[], minzoom: 0, maxzoom: 15 } as unknown as JsonResponse;
    const memCache = new Map<string, unknown>([['20261102', tileJson]]);
    const service = await PMTilesService.init(
      { getDeploymentKey: () => '20261102' } as IStorageRepository,
      {
        get: (key: string) => memCache.get(key),
        set: (key: string, value: unknown) => memCache.set(key, value),
      } as IMemCacheRepository,
      {} as IMetricsRepository,
      {} as IDatabaseRepository,
    );
    const response = await service.getJsonResponse('1', new URL('https://tiles.example/v1.json'));
    expect(response.tiles).toEqual(['https://tiles.example/v1/{z}/{x}/{y}.mvt?v=20261102']);
  });
});
