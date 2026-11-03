import { SignatureV4 } from '@smithy/signature-v4';
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
  tigrisClient,
  TigrisEndpoint,
  tigrisEndpoint,
  TigrisStorageRepository,
} from '../src/repository';
import { CACHE_REVISION, decodeMetricTags, encodeMetricTags, innerUrl, parseInnerUrl } from '../src/request';
import { EDGE_CACHE, inlineTileSource, StyleJson } from '../src/tiles';

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
    url                              | version
    ${'http://example.com/v1'}       | ${'1'}
    ${'http://example.com/v1.json'}  | ${'1'}
    ${'http://example.com/v01.json'} | ${'01'}
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
    ${'http://example.com/v1/style/light'}      | ${'light'} | ${'1'}
    ${'http://example.com/v1/style/dark.json'}  | ${'dark'}  | ${'1'}
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
    url                                          | version | z       | x             | y
    ${'http://example.com/v1/0/0/0.mvt'}         | ${'1'}  | ${'0'}  | ${'0'}        | ${'0'}
    ${'http://example.com/v1/2/3/1.mvt'}         | ${'1'}  | ${'2'}  | ${'3'}        | ${'1'}
    ${'http://example.com/v1/26/67108863/0.mvt'} | ${'1'}  | ${'26'} | ${'67108863'} | ${'0'}
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
    ${'http://example.com/v1/1/0/1'}
    ${'http://example.com/v1/1/0/1.mvt/extra'}
    ${'http://example.com/v1/1/0/1xmvt'}
    ${'http://example.com/v1/1/2.mvt'}
    ${'http://example.com/v1/style/dark-mode'}
  `('url $url returns undefined request type', ({ url }) => {
    const result = parseUrl(new Request(url));
    expect(result).toStrictEqual({
      requestType: undefined,
      url: new URL(url),
    });
  });

  it.each`
    url
    ${'http://example.com/v0.json'}
    ${'http://example.com/v2'}
    ${'http://example.com/v2.json'}
    ${'http://example.com/v2/style/dark'}
    ${'http://example.com/v10/1/0/1.mvt'}
  `('url $url of an unpublished API version returns undefined request type', ({ url }) => {
    expect(parseUrl(new Request(url)).requestType).toBeUndefined();
  });

  it.each`
    tile                          | reason
    ${'0/0/1'}                    | ${'y beyond the grid'}
    ${'1/2/0'}                    | ${'x beyond the grid'}
    ${'14/16384/0'}               | ${'x of 2^z'}
    ${'27/0/0'}                   | ${'z beyond the deepest tile ID'}
    ${'99999999999999999999/0/0'} | ${'z beyond any number'}
    ${'3/99999999999999999999/0'} | ${'x beyond any number'}
  `('tile $tile with $reason returns undefined request type', ({ tile }) => {
    expect(parseUrl(new Request(`http://example.com/v1/${tile}.mvt`)).requestType).toBeUndefined();
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
    ${'/v1/2/3/1.mvt'}            | ${'public, max-age=86400, stale-while-revalidate=2592000'} | ${'"20261102"'}
    ${'/v1/2/3/1.mvt?v=20261102'} | ${'public, max-age=31536000, immutable'}                   | ${undefined}
    ${'/v1/2/3/1.mvt?v=20250818'} | ${'public, max-age=300'}                                   | ${undefined}
    ${'/v1.json'}                 | ${'public, max-age=300'}                                   | ${'"20261102"'}
    ${'/v1/style/dark.json'}      | ${'public, max-age=300'}                                   | ${'"20261102"'}
    ${'/v1/nonsense'}             | ${'no-store'}                                              | ${undefined}
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
    expect(tigrisClient(TigrisEndpoint.T3, 'id', 'secret')).toBe(tigrisClient(TigrisEndpoint.T3, 'id', 'secret'));
    expect(tigrisClient(TigrisEndpoint.T3, 'id', 'secret')).not.toBe(tigrisClient(TigrisEndpoint.FLY, 'id', 'secret'));
  });
});

// The SHA-256 and HMAC-SHA256 the AWS SDK's signer takes, on WebCrypto.
class WebCryptoSha256 {
  private chunks: Uint8Array[] = [];
  constructor(private secret?: Uint8Array | string) {}
  update(data: Uint8Array | string) {
    this.chunks.push(typeof data === 'string' ? new TextEncoder().encode(data) : data);
  }
  reset() {
    this.chunks = [];
  }
  async digest(): Promise<Uint8Array> {
    const data = new Uint8Array(await new Blob(this.chunks as BlobPart[]).arrayBuffer());
    if (this.secret === undefined) {
      return new Uint8Array(await crypto.subtle.digest('SHA-256', data));
    }
    const secret = typeof this.secret === 'string' ? new TextEncoder().encode(this.secret) : this.secret;
    const key = await crypto.subtle.importKey('raw', secret, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
    return new Uint8Array(await crypto.subtle.sign('HMAC', key, data));
  }
}

describe('TigrisStorageRepository', () => {
  const key = '20261102';
  const pushed: Metric[] = [];
  const metrics = { push: (metric: Metric) => pushed.push(metric) } as unknown as IMetricsRepository;
  const repository = (endpoint = TigrisEndpoint.T3) =>
    new TigrisStorageRepository('key-id', 'secret', 'tiles-geo', key, metrics, 'dev/', endpoint);

  // Stands in for Tigris, answering each request with the next of the responses given.
  const mockTigris = (...responses: (() => Response)[]) => {
    const requests: Request[] = [];
    vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
      requests.push(new Request(input as RequestInfo, init));
      return responses[Math.min(requests.length, responses.length) - 1]();
    });
    return requests;
  };
  const partial =
    (body: BodyInit = 'tile 0') =>
    () =>
      new Response(body, { status: 206 });

  beforeEach(() => {
    pushed.length = 0;
    // No backoff between retries.
    vi.spyOn(Math, 'random').mockReturnValue(0);
  });

  it("reads a range from the endpoint's bucket host, signed as the AWS SDK signs", async () => {
    const requests = mockTigris(partial());
    const stream = await repository().getRangeAsStream({ offset: 100, length: 10 }, 'chunk_3.pmtiles');
    expect(await new Response(stream).text()).toBe('tile 0');

    const [request] = requests;
    expect(request.method).toBe('GET');
    expect(request.url).toBe(`https://tiles-geo.t3.storage.dev/dev/${key}/chunk_3.pmtiles`);
    expect(request.headers.get('Range')).toBe('bytes=100-109');
    expect(request.headers.get('X-Amz-Content-Sha256')).toBe('UNSIGNED-PAYLOAD');
    const amzDate = request.headers.get('X-Amz-Date')!;
    const authorization = request.headers.get('Authorization')!;
    expect(authorization).toMatch(
      new RegExp(
        `^AWS4-HMAC-SHA256 Credential=key-id/${amzDate.slice(0, 8)}/auto/s3/aws4_request, ` +
          'SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=[0-9a-f]{64}$',
      ),
    );

    // The signer the AWS SDK used for Tigris comes to the same signature for the same request at the same time.
    const signer = new SignatureV4({
      credentials: { accessKeyId: 'key-id', secretAccessKey: 'secret' },
      region: 'auto',
      service: 's3',
      sha256: WebCryptoSha256,
      uriEscapePath: false,
      applyChecksum: true,
    });
    const url = new URL(request.url);
    const [, year, month, day, hour, minute, second] = /^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/.exec(amzDate)!;
    const signed = await signer.sign(
      {
        method: 'GET',
        protocol: url.protocol,
        hostname: url.hostname,
        path: url.pathname,
        query: {},
        headers: { host: url.host, 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' },
      },
      { signingDate: new Date(`${year}-${month}-${day}T${hour}:${minute}:${second}Z`) },
    );
    expect(signed.headers.authorization).toBe(authorization);
  });

  it.each`
    endpoint              | host
    ${TigrisEndpoint.T3}  | ${'t3.storage.dev'}
    ${TigrisEndpoint.FLY} | ${'fly.storage.tigris.dev'}
  `('reads from $host and tags its metric with it', async ({ endpoint, host }) => {
    const requests = mockTigris(partial());
    await (await repository(endpoint).getRangeAsStream({ offset: 0, length: 6 }, 'chunk_0.pmtiles')).cancel();
    expect(new URL(requests[0].url).hostname).toBe(`tiles-geo.${host}`);
    expect(pushed.map((metric) => [metric.name, Object.fromEntries(metric.tags)])).toEqual([
      ['tigris_storage_get', { endpoint: host }],
    ]);
    expect(pushed[0].fields.get('count')).toEqual({ value: 1, type: 'int' });
    expect(pushed[0].fields.get('duration')?.type).toBe('duration');
  });

  it('hands on the body as it arrives', async () => {
    // A body that sends its first bytes and never finishes: anything that waited for all of it would hang.
    mockTigris(partial(new ReadableStream({ start: (controller) => controller.enqueue(new Uint8Array([1, 2, 3])) })));
    const reader = (await repository().getRangeAsStream({ offset: 0, length: 10 }, 'chunk_0.pmtiles')).getReader();
    expect(await reader.read()).toEqual({ done: false, value: new Uint8Array([1, 2, 3]) });
    await reader.cancel();
  });

  it('reads a whole object', async () => {
    const requests = mockTigris(() => new Response('{"version":8}'));
    expect(await new Response(await repository().getAsStream('styles/light.json')).text()).toBe('{"version":8}');
    expect(requests[0].url).toBe(`https://tiles-geo.t3.storage.dev/dev/${key}/styles/light.json`);
    expect(requests[0].headers.has('Range')).toBe(false);
  });

  it.each`
    statuses
    ${[503, 206]}
    ${[500, 429, 206]}
  `('retries a read Tigris answers with $statuses, letting go of each failed response', async ({ statuses }) => {
    const failures: Response[] = [];
    const requests = mockTigris(
      ...statuses.map((status: number) => () => {
        const response = new Response(status === 206 ? 'tile 0' : 'Slow down', { status });
        if (status !== 206) {
          failures.push(response);
        }
        return response;
      }),
    );
    const stream = await repository().getRangeAsStream({ offset: 0, length: 6 }, 'chunk_0.pmtiles');
    expect(await new Response(stream).text()).toBe('tile 0');
    expect(requests).toHaveLength(statuses.length);
    for (const response of failures) {
      await expect(response.body!.getReader().read()).resolves.toEqual({ done: true, value: undefined });
    }
    expect(pushed).toHaveLength(1);
  });

  it('gives up after three attempts', async () => {
    const requests = mockTigris(() => new Response('Unavailable', { status: 503 }));
    await expect(repository().getRangeAsStream({ offset: 0, length: 6 }, 'chunk_0.pmtiles')).rejects.toThrow(
      'Tigris answered a range read with 503',
    );
    expect(requests).toHaveLength(3);
    expect(pushed).toEqual([]);
  });

  it.each`
    status | body
    ${404} | ${'NoSuchKey'}
    ${403} | ${'SignatureDoesNotMatch'}
    ${416} | ${'InvalidRange'}
  `('fails a range read Tigris answers with $status, without retrying', async ({ status, body }) => {
    const requests = mockTigris(() => new Response(body, { status }));
    await expect(repository().getRangeAsStream({ offset: 0, length: 6 }, 'chunk_0.pmtiles')).rejects.toThrow(
      `Tigris answered a range read with ${status}`,
    );
    expect(requests).toHaveLength(1);
  });

  it.each`
    offset | length | contentLength | ok
    ${0}   | ${6}   | ${'6'}        | ${true}
    ${0}   | ${3}   | ${'6'}        | ${false}
    ${3}   | ${3}   | ${'3'}        | ${false}
  `(
    'takes a 200 for a range at $offset of $length bytes, the whole $contentLength-byte object, only if it is the range: $ok',
    async ({ offset, length, contentLength, ok }) => {
      mockTigris(
        () =>
          new Response('tile 0'.slice(0, Number(contentLength)), {
            status: 200,
            headers: { 'Content-Length': contentLength },
          }),
      );
      const read = repository().getRangeAsStream({ offset, length }, 'chunk_0.pmtiles');
      if (ok) {
        expect(await new Response(await read).text()).toBe('tile 0');
      } else {
        await expect(read).rejects.toThrow('Tigris answered a range read with 200');
      }
    },
  );

  it('fails a whole-object read Tigris answers with an error', async () => {
    mockTigris(() => new Response('AccessDenied', { status: 403 }));
    await expect(repository().getAsStream('styles/light.json')).rejects.toThrow('Tigris answered a read with 403');
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
        'Content-Length': '4',
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
    const { calls } = await fetchGateway('/v1/2/3/1.mvt', {
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

  it.each(['POST', 'PUT', 'DELETE', 'PATCH'])('refuses %s itself', async (method) => {
    const { response, calls } = await fetchGateway('/v1/1/1/0.mvt', { method });
    expect(response.status).toBe(405);
    expect(response.headers.get('Allow')).toBe('GET, HEAD, OPTIONS');
    expect(response.headers.get('Cache-Control')).toBe('no-store');
    expect(calls).toEqual([]);
  });

  it.each(['/v1/1/1/0.mvt', '/v1.json', '/anything'])('answers a CORS preflight for %s itself', async (url) => {
    const { response, calls } = await fetchGateway(url, {
      method: 'OPTIONS',
      headers: { Origin: 'https://photos.example.org', 'Access-Control-Request-Method': 'GET' },
    });
    expect(response.status).toBe(204);
    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*');
    expect(response.headers.get('Access-Control-Allow-Methods')).toBe('GET, HEAD, OPTIONS');
    expect(response.body).toBeNull();
    expect(calls).toEqual([]);
  });

  it.each`
    url                                        | cacheControl                                               | etag
    ${'/v1/2/3/1.mvt'}                         | ${'public, max-age=86400, stale-while-revalidate=2592000'} | ${`"${env.DEPLOYMENT_KEY}"`}
    ${`/v1/2/3/1.mvt?v=${env.DEPLOYMENT_KEY}`} | ${'public, max-age=31536000, immutable'}                   | ${null}
    ${'/v1.json'}                              | ${'public, max-age=300'}                                   | ${`"${env.DEPLOYMENT_KEY}"`}
  `("answers HEAD for $url with GET's headers and no body", async ({ url, cacheControl, etag }) => {
    let tileBody: ReadableStream | null = null;
    const { response, calls } = await fetchGateway(url, {
      method: 'HEAD',
      tiles: () => {
        const tile = cachedTile();
        tileBody = tile.body;
        return tile;
      },
    });
    expect(response.status).toBe(200);
    expect(response.headers.get('Cache-Control')).toBe(cacheControl);
    expect(response.headers.get('ETag')).toBe(etag);
    expect(response.headers.get('Content-Type')).toBe('application/x-protobuf');
    expect(response.headers.get('Content-Length')).toBe('4');
    expect(response.headers.get('Cf-Cache-Status')).toBe('HIT');
    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*');
    expect(response.body).toBeNull();
    // Tiles is asked with GET, which shares the cached entry, and its body is let go.
    expect(calls.map((request) => request.method)).toEqual(['GET']);
    expect(tileBody!.locked).toBe(false);
    await expect(tileBody!.getReader().read()).resolves.toEqual({ done: true, value: undefined });
  });

  it('answers HEAD with a 304 for a response the client already has', async () => {
    const { response } = await fetchGateway('/v1.json', { method: 'HEAD', headers: { 'If-None-Match': etag } });
    expect(response.status).toBe(304);
    expect(response.headers.get('ETag')).toBe(etag);
  });

  it.each`
    url                | tiles
    ${'/v1/nonsense'}  | ${undefined}
    ${'/v1/1/0/0.mvt'} | ${() => new Response('Tile not found', { status: 404, headers: { 'Cache-Control': 'no-store' } })}
  `('answers HEAD for $url with an uncached 404 and no body', async ({ url, tiles }) => {
    const { response } = await fetchGateway(url, { method: 'HEAD', tiles });
    expect(response.status).toBe(404);
    expect(response.headers.get('Cache-Control')).toBe('no-store');
    expect(response.body).toBeNull();
  });

  it.each`
    url                           | reason
    ${'/v1/nonsense'}             | ${'an unknown path'}
    ${'/v1/1/2/3.mvt'}            | ${'a tile beyond its zoom grid'}
    ${'/v1/30/0/0.mvt'}           | ${'a zoom no tile ID exists for'}
    ${'/v2.json'}                 | ${'an unpublished API version'}
    ${'/v2/style/light'}          | ${'a style of an unpublished API version'}
    ${'/v3/1/1/0.mvt?v=20261102'} | ${'a tile of an unpublished API version'}
  `('answers $reason ($url) with an uncached 404 without asking Tiles', async ({ url }) => {
    const { response, calls } = await fetchGateway(url, { headers: { 'If-None-Match': etag } });
    expect(response.status).toBe(404);
    expect(response.headers.get('Cache-Control')).toBe('no-store');
    expect(response.headers.has('ETag')).toBe(false);
    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*');
    expect(calls).toEqual([]);
  });

  it.each`
    url                                        | cacheControl                                               | etag
    ${'/v1/2/3/1.mvt'}                         | ${'public, max-age=86400, stale-while-revalidate=2592000'} | ${`"${env.DEPLOYMENT_KEY}"`}
    ${`/v1/2/3/1.mvt?v=${env.DEPLOYMENT_KEY}`} | ${'public, max-age=31536000, immutable'}                   | ${null}
    ${'/v1/2/3/1.mvt?v=20240101'}              | ${'public, max-age=300'}                                   | ${null}
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

  describe('D1 warm-up', () => {
    // A D1 binding that answers every read with run().
    const stubD1 = (run: () => Promise<unknown>) => {
      const prepare = vi.fn(() => ({ run }));
      const withSession = vi.fn(() => ({ prepare }));
      return { withSession, prepare, d1: { withSession } as unknown as D1Database };
    };
    // Whether a promise settles once everything already queued has run.
    const settles = (promise: Promise<unknown>) =>
      Promise.race([promise.then(() => true), new Promise((resolve) => setTimeout(() => resolve(false), 10))]);

    it.each`
      url                | headers                      | status
      ${'/v1/2/3/1.mvt'} | ${{}}                        | ${200}
      ${'/v1.json'}      | ${{ 'If-None-Match': etag }} | ${304}
    `(
      'warms the nearby replica for $url ($status) without holding the response for it',
      async ({ url, headers, status }) => {
        captureMetrics();
        const warmUps: (() => void)[] = [];
        const { withSession, prepare, d1 } = stubD1(() => new Promise<void>((resolve) => warmUps.push(resolve)));
        const ctx = createExecutionContext();
        Object.defineProperty(ctx, 'exports', { value: { Tiles: { fetch: async () => cachedTile() } } });
        const waitUntil = vi.spyOn(ctx, 'waitUntil');

        // The warm-up is still running when the response comes back.
        const response = await worker.fetch(
          new IncomingRequest(`https://tiles.example${url}`, { headers }),
          { ...env, D1_GLOBAL: d1 },
          ctx,
        );
        expect(response.status).toBe(status);
        expect(withSession).toHaveBeenCalledExactlyOnceWith('first-unconstrained');
        expect(prepare).toHaveBeenCalledExactlyOnceWith('SELECT 1');
        expect(warmUps).toHaveLength(1);

        // It goes to waitUntil, which keeps the request alive until it finishes.
        const [[warmUp]] = waitUntil.mock.calls;
        expect(await settles(warmUp)).toBe(false);
        warmUps[0]();
        expect(await settles(warmUp)).toBe(true);
        await waitOnExecutionContext(ctx);
      },
    );

    it('only logs a failed warm-up', async () => {
      const error = vi.spyOn(console, 'error').mockImplementation(() => {});
      const { d1 } = stubD1(() => Promise.reject(new Error('D1 is down')));
      const ctx = createExecutionContext();
      Object.defineProperty(ctx, 'exports', { value: { Tiles: { fetch: async () => cachedTile() } } });
      const response = await worker.fetch(
        new IncomingRequest('https://tiles.example/v1/2/3/1.mvt'),
        { ...env, D1_GLOBAL: d1 },
        ctx,
      );
      await waitOnExecutionContext(ctx);
      expect(response.status).toBe(200);
      expect(error).toHaveBeenCalledWith('D1 warm-up failed', new Error('D1 is down'));
    });
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
  const tileJson = {
    tilejson: '3.0.0',
    tiles: [],
    vector_layers: [],
    attribution: '<a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
    bounds: [-180, -85.0511287, 180, 85.0511287],
    minzoom: 0,
    maxzoom: 15,
  };
  // What a request's style inlines from the TileJSON.
  const inlinedSource = {
    type: 'vector',
    tiles: [`${env.PUBLIC_URL}/v1/{z}/{x}/{y}.mvt?v=${key}`],
    minzoom: tileJson.minzoom,
    maxzoom: tileJson.maxzoom,
    bounds: tileJson.bounds,
    attribution: tileJson.attribution,
  };
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

  it("serves a tile from a fresh isolate with one D1 query, without the TileJSON's metadata", async () => {
    globalThis.memCache = new Map();
    mockStorage();
    const metrics = captureMetrics();
    const response = await exports.Tiles.fetch(tilesRequest('/v1/0/0/0.mvt'));
    expect(response.status).toBe(200);
    await response.body?.cancel();
    expect(metrics('d1_query').map((tags) => tags.query)).toEqual([expect.stringContaining('startTileId <= ?')]);
    expect(globalThis.memCache.size).toBe(0);
  });

  it.each`
    tile        | reason
    ${'1/0/0'}  | ${'absent from the directory'}
    ${'16/0/0'} | ${'beyond maxzoom'}
    ${'26/0/0'} | ${'at the deepest zoom a tile ID exists for'}
  `('does not let the edge keep a tile $reason', async ({ tile }) => {
    const response = await exports.Tiles.fetch(tilesRequest(`/v1/${tile}.mvt`));
    expect(response.status).toBe(404);
    expect(response.headers.get('Cache-Control')).toBe('no-store');
  });

  // Stands in for a style as map-styles publishes it, its tile source a link to the TileJSON.
  const storedStyle = {
    version: 8,
    sources: { vector: { type: 'vector', url: 'https://tiles.immich.cloud/v1.json' } },
    layers: [{ id: 'background', type: 'background' }],
  };
  const mockStyle = () =>
    // Made per call: a stream belongs to the request that created it.
    vi
      .spyOn(TigrisStorageRepository.prototype, 'getAsStream')
      .mockImplementation(async () => new Response(JSON.stringify(storedStyle)).body!);

  it('reads the TileJSON metadata once per isolate for the TileJSON and styles', async () => {
    globalThis.memCache = new Map();
    mockStyle();
    const metrics = captureMetrics();
    for (const url of ['/v1.json', '/v1/style/light', '/v1.json']) {
      const response = await exports.Tiles.fetch(tilesRequest(url));
      expect(response.status).toBe(200);
      await response.body?.cancel();
    }
    expect(metrics('d1_query').map((tags) => tags.query)).toEqual([expect.stringContaining('startTileId = -1')]);
  });

  it('inlines the tile source into a style, under the canonical host', async () => {
    mockStyle();
    const response = await exports.Tiles.fetch(tilesRequest('/v1/style/dark'));
    expect(response.headers.get('Cache-Control')).toBe(EDGE_CACHE);
    expect(response.headers.get('Cache-Tag')).toBe(`tiles,deploy-${key},type-style`);
    expect(await response.json()).toEqual({ ...storedStyle, sources: { vector: inlinedSource } });
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

  it.each`
    path                | reason
    ${'/v2.json'}       | ${'an unpublished API version'}
    ${'/v1/0/0/1.mvt'}  | ${'a tile beyond its zoom grid'}
    ${'/v1/27/0/0.mvt'} | ${'a zoom no tile ID exists for'}
    ${'/v1/nonsense'}   | ${'an unknown path'}
  `('answers an inner URL for $reason with an uncached 404', async ({ path }) => {
    const response = await exports.Tiles.fetch(`https://tiles.internal/${key}${path}?rev=${CACHE_REVISION}`);
    expect(response.status).toBe(404);
    expect(response.headers.get('Cache-Control')).toBe('no-store');
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

    it("gives styles the TileJSON's tile source, with the 5-minute policy", async () => {
      mockStyle();
      const style = await fetchWith('https://tiles.prod.example/v1/style/light');
      expect(style.headers.get('Cache-Control')).toBe('public, max-age=300');
      expect(style.headers.get('ETag')).toBe(etag);
      const { tiles } = (await (await fetchWith('https://tiles.prod.example/v1.json')).json()) as JsonResponse;
      expect(((await style.json()) as StyleJson).sources.vector).toEqual({ ...inlinedSource, tiles });
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

    it('answers HEAD for a tile Tiles reads from storage without its body', async () => {
      mockStorage();
      const response = await exports.default.fetch(`https://tiles.example/v1/0/0/0.mvt?v=${key}`, { method: 'HEAD' });
      expect(response.status).toBe(200);
      expect(response.headers.get('Content-Encoding')).toBe('gzip');
      expect(response.headers.get('Cache-Control')).toBe('public, max-age=31536000, immutable');
      expect(response.body).toBeNull();
    });

    it('hands clients the stored tile bytes, encoded once', async () => {
      mockStorage();
      const response = await exports.default.fetch(`https://tiles.example/v1/0/0/0.mvt?v=${key}`);
      expect(response.status).toBe(200);
      expect(response.headers.get('Content-Encoding')).toBe('gzip');
      expect(new TextDecoder().decode(await response.arrayBuffer())).toBe('tile 0');
    });

    it('streams tile bytes from storage to the client as they arrive', async () => {
      // Random bytes don't compress, so the first half of the gzipped tile decodes to a good part of it.
      const tile = crypto.getRandomValues(new Uint8Array(64 * 1024));
      const gzipped = new Uint8Array(
        await new Response(new Blob([tile]).stream().pipeThrough(new CompressionStream('gzip'))).arrayBuffer(),
      );
      // A storage read that sends half the tile and never finishes: anything that waited for the whole body would
      // hang.
      vi.spyOn(TigrisStorageRepository.prototype, 'getRangeAsStream').mockImplementation(
        async () =>
          new ReadableStream({ start: (controller) => controller.enqueue(gzipped.slice(0, gzipped.length / 2)) }),
      );
      const response = await exports.default.fetch(`https://tiles.example/v1/0/0/0.mvt?v=${key}`);
      expect(response.status).toBe(200);
      const reader = response.body!.getReader();
      const { value } = await reader.read();
      expect(value!.length).toBeGreaterThan(0);
      expect(value).toEqual(tile.slice(0, value!.length));
      await reader.cancel();
    });
  });
});

describe('inlineTileSource', () => {
  const tileJson = {
    tiles: ['https://tiles.example/v1/{z}/{x}/{y}.mvt?v=20261102'],
    minzoom: 0,
    maxzoom: 15,
    bounds: [-180, -85, 180, 85],
    attribution: 'OpenStreetMap',
    vector_layers: [],
  } as unknown as JsonResponse;
  const style = (sources: StyleJson['sources']) => ({ version: 8, sources, layers: [] }) as unknown as StyleJson;

  it('replaces only the link to the TileJSON, and leaves the stored style as it was', () => {
    const vector = { type: 'vector', url: 'https://tiles.immich.cloud/v1.json', promoteId: 'id' };
    const other = { type: 'raster', url: 'https://example.org/raster.json' };
    const stored = style({ vector, other });
    expect(inlineTileSource(stored, tileJson).sources).toEqual({
      vector: {
        type: 'vector',
        promoteId: 'id',
        tiles: ['https://tiles.example/v1/{z}/{x}/{y}.mvt?v=20261102'],
        minzoom: 0,
        maxzoom: 15,
        bounds: [-180, -85, 180, 85],
        attribution: 'OpenStreetMap',
      },
      other,
    });
    expect(stored.sources.vector).toBe(vector);
    expect(vector.url).toBe('https://tiles.immich.cloud/v1.json');
  });

  it('refuses a style without the vector source', () => {
    expect(() => inlineTileSource(style({}), tileJson)).toThrow('Style has no "vector" tile source');
  });
});

describe('PMTilesService', () => {
  const key = '20261102';
  const tileJson = { tiles: [] as string[], minzoom: 0, maxzoom: 15 } as unknown as JsonResponse;
  const memCacheOf = (memCache: Map<string, unknown>) =>
    ({
      get: (key: string) => memCache.get(key),
      set: (key: string, value: unknown) => memCache.set(key, value),
    }) as IMemCacheRepository;
  const metrics = {
    monitorAsyncFunction:
      (_operation: unknown, call: (...args: unknown[]) => Promise<unknown>) =>
      (...args: unknown[]) =>
        call(...args),
    push: () => {},
  } as unknown as IMetricsRepository;
  // A D1 table holding the metadata row and the rows given, answering both of the service's queries.
  const stubDb = (rows: { startTileId: number; entry: string }[]) => {
    const all = [{ startTileId: -1, entry: JSON.stringify(tileJson) }, ...rows];
    const query = vi.fn(async (sql: string, tileId?: number) => {
      const results = sql.includes('startTileId <= ?')
        ? all.filter((row) => row.startTileId <= tileId!).slice(-1)
        : all.filter((row) => row.startTileId === -1);
      return { success: true, results, meta: {} } as unknown as D1Result<Record<string, unknown>>;
    });
    return { query, db: { query } as IDatabaseRepository };
  };
  const storage = () => {
    const getRangeAsStream = vi.fn(async () => new Blob(['tile']).stream());
    return {
      getRangeAsStream,
      source: { getDeploymentKey: () => key, getRangeAsStream } as unknown as IStorageRepository,
    };
  };
  // Two directories: tiles 7 and 8 (2/1/1 and 2/0/1), the latter stored 10 bytes after the former in chunk 3, and
  // tile 13 (2/2/2).
  const directory = (tileIdStart: number, tileIds: number[]) => ({
    startTileId: tileIdStart,
    entry: DirectoryString.fromDirectory({
      tileIdStart,
      offsetStart: 100,
      entries: tileIds.map((tileId, i) => ({ tileId, offset: 100 + 10 * i, length: 10, runLength: 1, chunkId: 3 })),
    }).toString(),
  });
  const directories = [directory(7, [7, 8]), directory(13, [13])];

  it("points clients at the live deployment's versioned tile URLs", async () => {
    const memCache = new Map<string, unknown>([[key, tileJson]]);
    const service = new PMTilesService(
      { getDeploymentKey: () => key } as IStorageRepository,
      memCacheOf(memCache),
      {} as IMetricsRepository,
      {} as IDatabaseRepository,
    );
    const response = await service.getJsonResponse('1', 'https://tiles.example');
    expect(response.tiles).toEqual(['https://tiles.example/v1/{z}/{x}/{y}.mvt?v=20261102']);
    // The cached metadata is shared by every request, so it stays as stored.
    expect(tileJson.tiles).toEqual([]);
  });

  it('reads the metadata once per isolate', async () => {
    const { query, db } = stubDb([]);
    const service = new PMTilesService(storage().source, memCacheOf(new Map()), metrics, db);
    await service.getJsonResponse('1', 'https://tiles.example');
    await service.getJsonResponse('1', 'https://tiles.example');
    expect(query).toHaveBeenCalledOnce();
  });

  it('serves a tile with a single query, the directory lookup, even before the metadata was read', async () => {
    const { query, db } = stubDb(directories);
    const { getRangeAsStream, source } = storage();
    const memCache = new Map<string, unknown>();
    const service = new PMTilesService(source, memCacheOf(memCache), metrics, db);
    const tile = await service.getTile(2, 0, 1);
    expect(await new Response(tile).text()).toBe('tile');
    expect(query.mock.calls).toEqual([[expect.stringContaining('startTileId <= ?'), 8]]);
    expect(getRangeAsStream).toHaveBeenCalledExactlyOnceWith({ offset: 110, length: 10 }, 'chunk_3.pmtiles');
    expect(memCache.size).toBe(0);
  });

  it.each`
    tile        | tileId        | reason
    ${'0/0/0'}  | ${0}          | ${'before the first directory, where the lookup lands on the metadata row'}
    ${'2/0/0'}  | ${5}          | ${'before the first directory, at a zoom the data has'}
    ${'2/0/2'}  | ${9}          | ${'between directories'}
    ${'2/3/0'}  | ${20}         | ${'past the last tile of the last directory'}
    ${'16/0/0'} | ${1431655765} | ${'at a zoom deeper than the data has'}
  `('has no tile $tile, $reason', async ({ tile, tileId }) => {
    const { query, db } = stubDb(directories);
    const { getRangeAsStream, source } = storage();
    const service = new PMTilesService(source, memCacheOf(new Map()), metrics, db);
    const [z, x, y] = tile.split('/').map(Number);
    expect(await service.getTile(z, x, y)).toBeUndefined();
    expect(query.mock.calls).toEqual([[expect.stringContaining('startTileId <= ?'), tileId]]);
    expect(getRangeAsStream).not.toHaveBeenCalled();
  });

  it.each`
    tile
    ${'0/1/0'}
    ${'1/0/2'}
    ${'27/0/0'}
  `('has no tile $tile, without a lookup, as the tile cannot exist', async ({ tile }) => {
    const { query, db } = stubDb(directories);
    const service = new PMTilesService(storage().source, memCacheOf(new Map()), metrics, db);
    const [z, x, y] = tile.split('/').map(Number);
    expect(await service.getTile(z, x, y)).toBeUndefined();
    expect(query).not.toHaveBeenCalled();
  });
});
