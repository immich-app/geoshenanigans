import { createExecutionContext, env, waitOnExecutionContext } from 'cloudflare:test';
import { exports } from 'cloudflare:workers';
import worker, { cachePolicy, cacheStatus, etagMatches, parseUrl } from '../src';
import { IDatabaseRepository, IMemCacheRepository, IMetricsRepository, IStorageRepository } from '../src/interface';
import { DirectoryString, PMTilesService } from '../src/pmtiles.service';
import { JsonResponse } from '../src/pmtiles/types';
import { fromRadix64, toRadix64 } from '../src/pmtiles/utils';
import {
  InfluxMetricsProvider,
  Metric,
  s3Client,
  TigrisEndpoint,
  tigrisEndpoint,
  TigrisStorageRepository,
} from '../src/repository';
import { CACHE_REVISION, decodeMetricTags, encodeMetricTags, innerUrl, parseInnerUrl } from '../src/request';
import { EDGE_CACHE } from '../src/tiles';

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
    ${'/v1/1/2/3.mvt?v=20250818'} | ${'public, max-age=300'}                                   | ${undefined}
    ${'/v1.json'}                 | ${'public, max-age=300'}                                   | ${'"20261102"'}
    ${'/v1/style/dark.json'}      | ${'public, max-age=300'}                                   | ${'"20261102"'}
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

describe('inner URL', () => {
  const key = '20261102';
  const inner = (url: string) => innerUrl(parseUrl(new Request(`https://tiles.example${url}`)), key);

  it.each`
    url                                      | path
    ${'/v1/14/8187/5448.mvt'}                | ${'/v1/14/8187/5448.mvt'}
    ${'/v1/014/08187/5448.mvt?v=20250818&x'} | ${'/v1/14/8187/5448.mvt'}
    ${'/v1/14/8187/5448.mvt?v=20261102'}     | ${'/v1/14/8187/5448.mvt'}
    ${'/v1'}                                 | ${'/v1.json'}
    ${'/v1.json?callback=x'}                 | ${'/v1.json'}
    ${'/v1/style/dark'}                      | ${'/v1/style/dark.json'}
    ${'/v01/style/dark.json'}                | ${'/v1/style/dark.json'}
  `('$url is cached as $path under the deployment', ({ url, path }) => {
    expect(inner(url)).toBe(`https://tiles.internal/${key}${path}?rev=${CACHE_REVISION}`);
  });

  it('parses back to the deployment and the request', () => {
    const { deploymentKey, params } = parseInnerUrl(inner('/v1/14/8187/5448.mvt?v=20261102'));
    expect(deploymentKey).toBe(key);
    expect(params).toMatchObject({ requestType: 'tile', version: '1', z: '14', x: '8187', y: '5448' });
  });

  it('round-trips metric tags that are not Latin-1', () => {
    const tags = { asOrg: 'Société Générale – 中国电信', apexDomain: 'example.com' };
    expect(decodeMetricTags(encodeMetricTags(tags))).toEqual(tags);
    expect(decodeMetricTags(null)).toEqual({});
    expect(decodeMetricTags('%E0')).toEqual({});
  });
});

describe('tigrisEndpoint', () => {
  it.each`
    continent | endpoint
    ${'EU'}   | ${TigrisEndpoint.T3}
    ${'NA'}   | ${TigrisEndpoint.T3}
    ${'AS'}   | ${TigrisEndpoint.T3}
    ${'SA'}   | ${TigrisEndpoint.FLY}
    ${'OC'}   | ${TigrisEndpoint.FLY}
    ${'AF'}   | ${TigrisEndpoint.FLY}
    ${'AN'}   | ${TigrisEndpoint.FLY}
    ${''}     | ${TigrisEndpoint.FLY}
    ${null}   | ${TigrisEndpoint.FLY}
  `('reads from $endpoint for continent $continent', ({ continent, endpoint }) => {
    expect(tigrisEndpoint(continent)).toBe(endpoint);
  });

  it('keeps one client per endpoint', () => {
    expect(s3Client(TigrisEndpoint.T3, 'id', 'secret')).toBe(s3Client(TigrisEndpoint.T3, 'id', 'secret'));
    expect(s3Client(TigrisEndpoint.T3, 'id', 'secret')).not.toBe(s3Client(TigrisEndpoint.FLY, 'id', 'secret'));
  });
});

/** Collects the metrics the worker pushes, by name. */
function captureMetrics() {
  const pushed: Metric[] = [];
  vi.spyOn(InfluxMetricsProvider.prototype, 'pushMetric').mockImplementation((metric: Metric) => {
    pushed.push(metric);
  });
  vi.spyOn(InfluxMetricsProvider.prototype, 'flush').mockResolvedValue(undefined);
  return (name: string) =>
    pushed.filter((metric) => metric.name === name).map((metric) => Object.fromEntries(metric.tags));
}

afterEach(() => {
  vi.restoreAllMocks();
});

// Workers Cache isn't emulated locally, so these give the gateway a stand-in for the Tiles entrypoint that answers as
// the cache would.
describe('gateway', () => {
  const key = env.DEPLOYMENT_KEY;
  const etag = `"${key}"`;

  type GatewayOptions = {
    headers?: Record<string, string>;
    cf?: Partial<IncomingRequestCfProperties>;
    method?: string;
    tiles?: () => Response;
  };

  // What the cache hands back for a stored tile: the edge's headers plus Age and the cache status.
  const cachedTile = (cacheStatus = 'HIT') =>
    new Response('tile', {
      headers: {
        'Cache-Control': EDGE_CACHE,
        'Content-Type': 'application/x-protobuf',
        'PMTiles-Deployment-Key': key,
        Age: '86400',
        'Cf-Cache-Status': cacheStatus,
      },
    });

  const fetchGateway = async (
    url: string,
    { headers, cf, method, tiles = () => cachedTile() }: GatewayOptions = {},
  ) => {
    const calls: Request[] = [];
    const ctx = createExecutionContext();
    Object.defineProperty(ctx, 'exports', {
      value: {
        Tiles: {
          fetch: async (request: Request) => {
            calls.push(request);
            return tiles();
          },
        },
      },
    });
    const request = new IncomingRequest(`https://tiles.example${url}`, {
      method,
      headers,
      cf: cf as IncomingRequestCfProperties,
    });
    const response = await worker.fetch(request, env, ctx);
    await waitOnExecutionContext(ctx);
    return { response, calls };
  };

  it.each`
    url                                  | inner
    ${'/v1/14/8187/5448.mvt?v=20240101'} | ${'/v1/14/8187/5448.mvt'}
    ${'/v1.json'}                        | ${'/v1.json'}
    ${'/v1/style/light'}                 | ${'/v1/style/light.json'}
  `("asks Tiles for $url under the live deployment's key", async ({ url, inner }) => {
    const { calls } = await fetchGateway(url);
    expect(calls.map((request) => request.url)).toEqual([
      `https://tiles.internal/${key}${inner}?rev=${CACHE_REVISION}`,
    ]);
  });

  it("passes on the client's continent, metric tags and accepted encodings, and nothing else it sent", async () => {
    const { calls } = await fetchGateway('/v1/1/2/3.mvt', {
      headers: {
        Origin: 'https://photos.example.org',
        'If-None-Match': etag,
        Range: 'bytes=0-1',
        'Accept-Encoding': 'gzip, br',
      },
      cf: { continent: 'OC', colo: 'SYD', asOrganization: 'Société Générale' },
    });
    const headers = Object.fromEntries(calls[0].headers);
    expect(headers['x-continent']).toBe('OC');
    expect(decodeMetricTags(headers['x-metric-tags'])).toEqual({
      continent: 'OC',
      colo: 'SYD',
      asOrg: 'Société Générale',
      apexDomain: 'example.org',
      protocol: 'https:',
      url: 'unversioned',
    });
    expect(headers['accept-encoding']).toBe('gzip, br');
    expect(Object.keys(headers).sort()).toEqual(['accept-encoding', 'x-continent', 'x-metric-tags']);
  });

  it('answers other methods and unknown URLs itself', async () => {
    const post = await fetchGateway('/v1/1/2/3.mvt', { method: 'POST' });
    expect(post.response.status).toBe(405);
    const unknown = await fetchGateway('/v1/nonsense');
    expect(unknown.response.status).toBe(404);
    expect(unknown.response.headers.get('Cache-Control')).toBe('public, max-age=2678400');
    expect(unknown.response.headers.get('Access-Control-Allow-Origin')).toBe('*');
    expect([...post.calls, ...unknown.calls]).toEqual([]);
  });

  it.each`
    url                                        | cacheControl                                               | etag
    ${'/v1/1/2/3.mvt'}                         | ${'public, max-age=86400, stale-while-revalidate=2592000'} | ${`"${env.DEPLOYMENT_KEY}"`}
    ${`/v1/1/2/3.mvt?v=${env.DEPLOYMENT_KEY}`} | ${'public, max-age=31536000, immutable'}                   | ${null}
    ${'/v1/1/2/3.mvt?v=20240101'}              | ${'public, max-age=300'}                                   | ${null}
    ${'/v1.json'}                              | ${'public, max-age=300'}                                   | ${`"${env.DEPLOYMENT_KEY}"`}
    ${'/v1/style/light.json'}                  | ${'public, max-age=300'}                                   | ${`"${env.DEPLOYMENT_KEY}"`}
  `("gives $url the client policy, not the edge's", async ({ url, cacheControl, etag }) => {
    const { response } = await fetchGateway(url);
    expect(response.status).toBe(200);
    expect(response.headers.get('Cache-Control')).toBe(cacheControl);
    expect(response.headers.get('ETag')).toBe(etag);
    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*');
    expect(response.headers.get('PMTiles-Deployment-Key')).toBe(key);
    expect(response.headers.get('Cf-Cache-Status')).toBe('HIT');
    expect(response.headers.get('Server-Timing')).toMatch(/tiles_cache_fetch;dur=.*tiles_handle_request;dur=/);
    for (const header of ['Age', 'Vary', 'Cache-Tag']) {
      expect(response.headers.has(header)).toBe(false);
    }
    expect(new TextDecoder().decode(await response.arrayBuffer())).toBe('tile');
  });

  it('answers 304 from a cached response the client already has', async () => {
    const { response } = await fetchGateway('/v1/14/8187/5448.mvt', { headers: { 'If-None-Match': `W/${etag}` } });
    expect(response.status).toBe(304);
    expect(response.headers.get('ETag')).toBe(etag);
    expect(response.headers.get('Cache-Control')).toBe('public, max-age=86400, stale-while-revalidate=2592000');
    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*');
    expect(response.headers.has('Content-Type')).toBe(false);
    expect(await response.text()).toBe('');
  });

  it('keeps a 404 from Tiles uncached by clients, despite a matching ETag', async () => {
    const { response } = await fetchGateway('/v1/1/0/0.mvt', {
      headers: { 'If-None-Match': etag },
      tiles: () => new Response('Tile not found', { status: 404, headers: { 'Cache-Control': 'no-store' } }),
    });
    expect(response.status).toBe(404);
    expect(response.headers.get('Cache-Control')).toBe('no-store');
    expect(response.headers.has('ETag')).toBe(false);
    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*');
  });

  it('keeps a failure from Tiles uncached by clients', async () => {
    const { response } = await fetchGateway('/v1/1/0/0.mvt', {
      tiles: () => new Response('Internal Server Error', { status: 500, headers: { 'Cache-Control': 'no-store' } }),
    });
    expect(response.status).toBe(500);
    expect(response.headers.get('Cache-Control')).toBe('no-store');
    expect(response.headers.has('ETag')).toBe(false);
  });

  it('counts responses the cache answered as CDN hits, with their cache status', async () => {
    const metrics = captureMetrics();
    await fetchGateway(`/v1/14/8187/5448.mvt?v=${key}`, { cf: { colo: 'LHR' } });
    await fetchGateway('/v1/style/dark', { tiles: () => cachedTile('UPDATING') });
    await fetchGateway('/v1.json', { tiles: () => cachedTile('MISS') });

    expect(metrics('tiles_cdn_hit')).toEqual([
      expect.objectContaining({
        request_type: 'tile',
        z: '14',
        url: 'versioned',
        deployment_key: key,
        cache_status: 'hit',
        colo: 'LHR',
      }),
      expect.objectContaining({ request_type: 'style', style: 'dark', cache_status: 'updating' }),
    ]);
    expect(metrics('tiles_handle_request').map((tags) => tags.cache_status)).toEqual(['hit', 'updating', 'miss']);
    expect(metrics('tiles_cache_fetch').map((tags) => tags.request_type)).toEqual(['tile', 'style', 'json']);
  });

  it('counts 304s', async () => {
    const metrics = captureMetrics();
    await fetchGateway('/v1.json', { headers: { 'If-None-Match': etag } });
    expect(metrics('tiles_not_modified')).toEqual([expect.objectContaining({ request_type: 'json' })]);
  });

  it.each`
    header       | status
    ${'HIT'}     | ${'hit'}
    ${'EXPIRED'} | ${'expired'}
    ${null}      | ${'none'}
  `('reads cache status $header as $status', ({ header, status }) => {
    expect(cacheStatus(new Response(null, { headers: header ? { 'Cf-Cache-Status': header } : {} }))).toBe(status);
  });
});

describe('Tiles', () => {
  const key = env.DEPLOYMENT_KEY;
  const etag = `"${key}"`;
  const tileJson = { tilejson: '3.0.0', tiles: [], vector_layers: [], minzoom: 0, maxzoom: 15 };
  // A directory whose only tile is tile 0, so other tiles in range are absent without any storage read.
  const directory = DirectoryString.fromDirectory({
    tileIdStart: 0,
    offsetStart: 0,
    entries: [{ tileId: 0, offset: 0, length: 10, runLength: 1, chunkId: 0 }],
  }).toString();

  const tilesRequest = (url: string, headers: Record<string, string> = {}, deploymentKey = key) =>
    new Request(innerUrl(parseUrl(new Request(`https://tiles.example${url}`)), deploymentKey), { headers });

  // Stands in for Tigris: tile 0 is the gzipped bytes of "tile 0", and each read records the endpoint it used.
  const mockStorage = () => {
    const endpoints: string[] = [];
    vi.spyOn(TigrisStorageRepository.prototype, 'getRangeAsStream').mockImplementation(async function (
      this: TigrisStorageRepository,
    ) {
      endpoints.push((this as unknown as { endpoint: string }).endpoint);
      return new Blob(['tile 0']).stream().pipeThrough(new CompressionStream('gzip'));
    });
    return endpoints;
  };

  beforeAll(async () => {
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

  it('serves the TileJSON for the canonical host, with the edge policy and Cache-Tag', async () => {
    const response = await exports.Tiles.fetch(tilesRequest('/v1.json'));
    expect(response.status).toBe(200);
    expect(response.headers.get('Cache-Control')).toBe(EDGE_CACHE);
    expect(response.headers.get('Cache-Tag')).toBe(`tiles,deploy-${key},type-json`);
    for (const header of ['Vary', 'Server-Timing', 'Access-Control-Allow-Origin', 'ETag']) {
      expect(response.headers.has(header)).toBe(false);
    }
    expect(((await response.json()) as { tiles: string[] }).tiles).toEqual([
      `${env.PUBLIC_URL}/v1/{z}/{x}/{y}.mvt?v=${key}`,
    ]);
  });

  it('serves a tile as stored, gzipped, tagged with its deployment', async () => {
    mockStorage();
    const response = await exports.Tiles.fetch(tilesRequest('/v1/0/0/0.mvt'));
    expect(response.status).toBe(200);
    expect(response.headers.get('Content-Encoding')).toBe('gzip');
    expect(response.headers.get('Cache-Tag')).toBe(`tiles,deploy-${key},type-tile`);
    expect(new TextDecoder().decode(await response.arrayBuffer())).toBe('tile 0');
  });

  it.each`
    continent | endpoint
    ${'EU'}   | ${TigrisEndpoint.T3}
    ${'SA'}   | ${TigrisEndpoint.FLY}
    ${''}     | ${TigrisEndpoint.FLY}
  `('reads tiles for $continent from $endpoint', async ({ continent, endpoint }) => {
    const endpoints = mockStorage();
    const response = await exports.Tiles.fetch(tilesRequest('/v1/0/0/0.mvt', { 'X-Continent': continent }));
    await response.body?.cancel();
    expect(endpoints).toEqual([endpoint]);
  });

  it.each`
    tile        | reason
    ${'1/0/0'}  | ${'absent from the directory'}
    ${'16/0/0'} | ${'beyond maxzoom'}
  `('does not let the edge keep a tile $reason', async ({ tile }) => {
    const response = await exports.Tiles.fetch(tilesRequest(`/v1/${tile}.mvt`));
    expect(response.status).toBe(404);
    expect(response.headers.get('Cache-Control')).toBe('no-store');
  });

  it('points a dev style at the canonical host', async () => {
    const stored = { sources: { vector: { type: 'vector', url: 'https://tiles.immich.cloud/v1' } } };
    // Made per call: a stream belongs to the request that created it.
    vi.spyOn(TigrisStorageRepository.prototype, 'getAsStream').mockImplementation(
      async () => new Response(JSON.stringify(stored)).body!,
    );
    const response = await exports.Tiles.fetch(tilesRequest('/v1/style/dark'));
    expect(response.headers.get('Cache-Control')).toBe(EDGE_CACHE);
    expect(response.headers.get('Cache-Tag')).toBe(`tiles,deploy-${key},type-style`);
    expect(await response.json()).toEqual({ sources: { vector: { type: 'vector', url: `${env.PUBLIC_URL}/v1` } } });
  });

  it('does not let the edge keep a failure', async () => {
    vi.spyOn(TigrisStorageRepository.prototype, 'getRangeAsStream').mockRejectedValue(new Error('Tigris is down'));
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const metrics = captureMetrics();
    const response = await exports.Tiles.fetch(tilesRequest('/v1/0/0/0.mvt'));
    expect(response.status).toBe(500);
    expect(response.headers.get('Cache-Control')).toBe('no-store');
    expect(metrics('error')).toEqual([expect.objectContaining({ error: 'Tigris is down' })]);
    expect(metrics('tiles_tile_request')).toEqual([expect.objectContaining({ z: '0' })]);
  });

  it('refuses a deployment other than the live one', async () => {
    const response = await exports.Tiles.fetch(tilesRequest('/v1.json', {}, '20000101'));
    expect(response.status).toBe(404);
    expect(response.headers.get('Cache-Control')).toBe('no-store');
  });

  it("tags its metrics with the client's, as passed on by the gateway", async () => {
    mockStorage();
    const metrics = captureMetrics();
    const tags = { continent: 'EU', colo: 'LHR', asOrg: 'AS', apexDomain: 'example.org', protocol: 'https:' };
    const response = await exports.Tiles.fetch(
      tilesRequest('/v1/0/0/0.mvt', { 'X-Metric-Tags': encodeMetricTags({ ...tags, url: 'versioned' }) }),
    );
    await response.body?.cancel();
    expect(metrics('tiles_tile_request')).toEqual([{ ...tags, url: 'versioned', z: '0', version: '1' }]);
  });

  it('has a purge method for the gateway', async () => {
    // The local runtime has no Workers Cache, so there is nothing to purge.
    expect(await exports.Tiles.purge(['tiles'])).toEqual({
      success: false,
      errors: [{ code: 0, message: 'Workers Cache is not enabled for the Tiles entrypoint' }],
    });
  });

  describe('behind the gateway', () => {
    const fetchWith = async (url: string, headers: Record<string, string> = {}) => {
      const ctx = createExecutionContext();
      const response = await worker.fetch(new IncomingRequest(url, { headers }), env, ctx);
      await waitOnExecutionContext(ctx);
      return response;
    };

    it('points every host at the canonical one', async () => {
      const response = await fetchWith('https://tiles.prod.example/v1');
      expect(response.headers.get('Cache-Control')).toBe('public, max-age=300');
      expect(response.headers.get('ETag')).toBe(etag);
      expect(((await response.json()) as { tiles: string[] }).tiles).toEqual([
        `${env.PUBLIC_URL}/v1/{z}/{x}/{y}.mvt?v=${key}`,
      ]);
    });

    it('answers 304 once a fresh response turns out to be what the client has', async () => {
      const response = await fetchWith('https://tiles.example/v1.json', { 'If-None-Match': etag });
      expect(response.status).toBe(304);
      expect(response.headers.get('Cache-Control')).toBe('public, max-age=300');
    });

    it.each`
      tile        | reason
      ${'1/0/0'}  | ${'absent from the directory'}
      ${'16/0/0'} | ${'beyond maxzoom'}
    `('keeps the 404 for a tile $reason despite a matching ETag', async ({ tile }) => {
      const response = await fetchWith(`https://tiles.example/v1/${tile}.mvt`, { 'If-None-Match': etag });
      expect(response.status).toBe(404);
    });

    it('hands clients the stored tile bytes, encoded once', async () => {
      mockStorage();
      const response = await exports.default.fetch(`https://tiles.example/v1/0/0/0.mvt?v=${key}`);
      expect(response.status).toBe(200);
      expect(response.headers.get('Content-Encoding')).toBe('gzip');
      expect(new TextDecoder().decode(await response.arrayBuffer())).toBe('tile 0');
    });
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
    const response = await service.getJsonResponse('1', 'https://tiles.example');
    expect(response.tiles).toEqual(['https://tiles.example/v1/{z}/{x}/{y}.mvt?v=20261102']);
    // The cached metadata is shared by every request, so it stays as stored.
    expect(tileJson.tiles).toEqual([]);
  });
});
