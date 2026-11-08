// The gateway's race: a tile GET whose cached answer is late is also read from storage by the gateway itself.
import { createExecutionContext, env, waitOnExecutionContext } from 'cloudflare:test';
import { exports } from 'cloudflare:workers';
import worker, { acceptsGzip, raceDelay } from '../src';
import { DirectoryString } from '../src/pmtiles.service';
import { zxyToTileId } from '../src/pmtiles/utils';
import {
  CloudflareDeferredRepository,
  InfluxMetricsProvider,
  Metric,
  TigrisStorageRepository,
} from '../src/repository';
import { CACHE_REVISION } from '../src/request';
import { Tiles } from '../src/tiles';

const IncomingRequest = Request<unknown, IncomingRequestCfProperties>;
const key = env.DEPLOYMENT_KEY;
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const gzip = (bytes: Uint8Array) => new Blob([bytes as BlobPart]).stream().pipeThrough(new CompressionStream('gzip'));
const gunzip = async (bytes: Uint8Array) =>
  new Uint8Array(
    await new Response(
      new Blob([bytes as BlobPart]).stream().pipeThrough(new DecompressionStream('gzip')),
    ).arrayBuffer(),
  );

// Chunk 7 as stored: tiles gzipped back to back, after some other bytes.
const tiles = new Map<string, Uint8Array>();
let chunk = new Uint8Array();
const SAMPLE = ['0/0/0', '1/0/1', '1/1/1', '2/1/2', '3/5/3'];

beforeAll(async () => {
  const parts: Uint8Array[] = [crypto.getRandomValues(new Uint8Array(777))];
  let offset = 777;
  const entries = [];
  for (const zxy of SAMPLE) {
    const raw = crypto.getRandomValues(new Uint8Array(1000 + 300 * parts.length));
    tiles.set(zxy, raw);
    const gzipped = new Uint8Array(await new Response(gzip(raw)).arrayBuffer());
    const [z, x, y] = zxy.split('/').map(Number);
    entries.push({ tileId: zxyToTileId(z, x, y), offset, length: gzipped.length, runLength: 1, chunkId: 7 });
    parts.push(gzipped);
    offset += gzipped.length;
  }
  chunk = new Uint8Array(await new Blob(parts as BlobPart[]).arrayBuffer());
  entries.sort((a, b) => a.tileId - b.tileId);
  const directory = DirectoryString.fromDirectory({ tileIdStart: 0, offsetStart: 0, entries }).toString();
  await env.D1_GLOBAL.batch([
    env.D1_GLOBAL.prepare(
      `CREATE TABLE IF NOT EXISTS cache_entries_${key} (startTileId INTEGER PRIMARY KEY, entry TEXT NOT NULL)`,
    ),
    env.D1_GLOBAL.prepare(`INSERT OR REPLACE INTO cache_entries_${key} VALUES (0, ?)`).bind(directory),
  ]);
});

beforeEach(() => {
  vi.spyOn(console, 'log').mockImplementation(() => {});
});
afterEach(() => {
  vi.restoreAllMocks();
});

/** Stands in for Tigris, serving ranges of chunk 7 after a delay; records reads and the ones cancelled. */
const mockStorage = (delay = 0) => {
  const storage = { reads: 0, cancelled: 0 };
  vi.spyOn(TigrisStorageRepository.prototype, 'getRangeAsStream').mockImplementation(async ({ offset, length }) => {
    storage.reads++;
    await sleep(delay);
    const bytes = chunk.slice(offset, offset + length);
    return new ReadableStream(
      {
        pull(controller) {
          controller.enqueue(bytes);
          controller.close();
        },
        cancel() {
          storage.cancelled++;
        },
      },
      { highWaterMark: 0 },
    );
  });
  return storage;
};

// Tiles' own fetch, before any test stands in for it.
const tilesFetch = Tiles.prototype.fetch;

type TierOptions = { delay: number; response?: () => Response };
type TierState = { calls: number; drained: boolean; cancelled: boolean };

/**
 * The cache's answer (the cached Tiles entrypoint, uncached in tests) after a delay, as on a full miss at the upper
 * tier: Tiles' own, or the one given. Records whether its body was read to its end or cancelled.
 */
const tierAnswer = async (
  { delay, response }: TierOptions,
  state: TierState,
  tilesAnswer: () => Promise<Response>,
): Promise<Response> => {
  state.calls++;
  await sleep(delay);
  const answer = response ? response() : await tilesAnswer();
  if (!answer.body) {
    return answer;
  }
  const reader = answer.body.getReader();
  const body = new ReadableStream({
    async pull(controller) {
      const { done, value } = await reader.read();
      if (done) {
        state.drained = true;
        controller.close();
      } else {
        controller.enqueue(value);
      }
    },
    cancel(reason) {
      state.cancelled = true;
      return reader.cancel(reason);
    },
  });
  return new Response(body, { status: answer.status, headers: answer.headers, encodeBody: 'manual' });
};

/** The cache for clients that reach the gateway through the runtime (fetchClient): Tiles' entrypoint answers late. */
const mockTier = (options: TierOptions) => {
  const state = { calls: 0, drained: false, cancelled: false };
  vi.spyOn(Tiles.prototype, 'fetch').mockImplementation(function (this: Tiles, request: Request) {
    return tierAnswer(options, state, () => tilesFetch.call(this, request));
  });
  return state;
};

// A tile the cache has, as it hands it back.
const cachedTile = (zxy: string, cacheStatus: string) => () =>
  new Response(gzip(tiles.get(zxy)!), {
    headers: { 'Cf-Cache-Status': cacheStatus, 'Content-Encoding': 'gzip', 'Content-Type': 'application/x-protobuf' },
    encodeBody: 'manual',
  });
const failure = () => new Response('Internal Server Error', { status: 500, headers: { 'Cache-Control': 'no-store' } });

type GatewayOptions = {
  method?: string;
  headers?: Record<string, string>;
  cf?: Partial<IncomingRequestCfProperties>;
  vars?: Partial<WorkerEnv>;
};

/**
 * The gateway called directly, with the cache answering as tier says (through Tiles' entrypoint unless it gives the
 * answer), and everything it leaves to run after the response (the cache's fill, metrics) waited for. Bodies are as the
 * gateway hands them on, before any encoding; tier records what the gateway did with the cache's.
 */
const fetchGateway = async (
  url: string,
  tierOptions: TierOptions,
  { method = 'GET', headers = { 'Accept-Encoding': 'gzip' }, cf = {}, vars = {} }: GatewayOptions = {},
) => {
  const tier = { calls: 0, drained: false, cancelled: false };
  const ctx = createExecutionContext();
  Object.defineProperty(ctx, 'exports', {
    value: {
      Tiles: { fetch: (request: Request) => tierAnswer(tierOptions, tier, () => exports.Tiles.fetch(request)) },
    },
  });
  const request = new IncomingRequest(`https://tiles.example${url}`, {
    method,
    headers,
    cf: { colo: 'LHR', continent: 'EU', ...cf } as IncomingRequestCfProperties,
  });
  const response = await worker.fetch(request, { ...env, ...vars }, ctx);
  const body = method === 'HEAD' ? null : new Uint8Array(await response.arrayBuffer());
  await waitOnExecutionContext(ctx);
  return { response, body, tier };
};

/** The gateway as clients reach it, through the runtime, which encodes its responses; then a wait for what's left. */
const fetchClient = async (url: string, settle: number) => {
  const response = await exports.default.fetch(`https://tiles.example${url}`, {
    headers: { 'Accept-Encoding': 'gzip' },
    cf: { colo: 'LHR', continent: 'EU' },
  });
  const body = new Uint8Array(await response.arrayBuffer());
  await sleep(settle);
  return { response, body };
};

describe('raceDelay', () => {
  it.each`
    value        | ms
    ${undefined} | ${120}
    ${''}        | ${120}
    ${'0'}       | ${0}
    ${'250'}     | ${250}
    ${'-1'}      | ${undefined}
    ${'off'}     | ${undefined}
  `('reads RACE_MS $value as $ms', ({ value, ms }) => {
    expect(raceDelay(value)).toBe(ms);
  });
});

describe('acceptsGzip', () => {
  it.each`
    header                 | accepts
    ${'gzip, deflate, br'} | ${true}
    ${'br;q=1.0, GZIP'}    | ${true}
    ${'gzip;q=0.5'}        | ${true}
    ${'gzip;q=0'}          | ${false}
    ${'br'}                | ${false}
    ${'*'}                 | ${false}
    ${''}                  | ${false}
    ${null}                | ${false}
  `('reads Accept-Encoding $header as $accepts', ({ header, accepts }) => {
    expect(acceptsGzip(header)).toBe(accepts);
  });
});

describe('CloudflareDeferredRepository', () => {
  it('runs deferred calls once what they wait for has settled', async () => {
    const ctx = createExecutionContext();
    const deferred = new CloudflareDeferredRepository(ctx);
    const events: string[] = [];
    deferred.defer(async () => events.push('flush'));
    deferred.waitFor(sleep(50).then(() => events.push('late')));
    deferred.waitFor(Promise.reject(new Error('failed')));
    deferred.runDeferred();
    await waitOnExecutionContext(ctx);
    expect(events).toEqual(['late', 'flush']);
  });
});

describe('gateway race', () => {
  it("serves the cache's answer in time, without reading storage", async () => {
    const storage = mockStorage();
    const { response } = await fetchGateway('/v1/0/0/0.mvt', { delay: 0, response: cachedTile('0/0/0', 'HIT') });
    expect(response.headers.get('X-Race')).toBe('none');
    expect(response.headers.get('Cf-Cache-Status')).toBe('HIT');
    expect(storage.reads).toBe(0);
  });

  it("serves the local read when the cache is late, and reads the cache's answer to its end", async () => {
    const storage = mockStorage();
    const { response, body, tier } = await fetchGateway('/v1/1/0/1.mvt', { delay: 400 });
    expect(response.status).toBe(200);
    expect(response.headers.get('X-Race')).toBe('local');
    expect(response.headers.get('Content-Encoding')).toBe('gzip');
    expect(response.headers.get('Cache-Control')).toBe('public, max-age=86400, stale-while-revalidate=2592000');
    expect(response.headers.has('Cf-Cache-Status')).toBe(false);
    // As stored: the runtime sends it on without encoding it again (see "gzipped once" below).
    expect(await gunzip(body!)).toEqual(tiles.get('1/0/1'));
    // The cache's fill completes.
    expect(tier).toEqual({ calls: 1, drained: true, cancelled: false });
    // Both read storage: the gateway, and Tiles for the cache.
    expect(storage.reads).toBe(2);
  });

  it("serves the cache's answer when it comes before the local read's, and cancels that", async () => {
    const storage = mockStorage(600);
    const { response } = await fetchGateway('/v1/2/1/2.mvt', { delay: 200, response: cachedTile('2/1/2', 'MISS') });
    expect(response.headers.get('X-Race')).toBe('tier');
    expect(response.headers.get('Cf-Cache-Status')).toBe('MISS');
    expect(storage).toEqual({ reads: 1, cancelled: 1 });
  });

  it("keeps the cache's 404 when the local read finds no tile either", async () => {
    mockStorage();
    const { response, tier } = await fetchGateway('/v1/1/0/0.mvt', { delay: 300 });
    expect(response.status).toBe(404);
    expect(response.headers.get('X-Race')).toBe('tier');
    expect(response.headers.get('Cache-Control')).toBe('no-store');
    expect(tier.calls).toBe(1);
  });

  it("serves the local read when the cache's late answer is a failure", async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    mockStorage(100);
    const { response, body, tier } = await fetchGateway('/v1/1/1/1.mvt', { delay: 150, response: failure });
    expect(response.status).toBe(200);
    expect(response.headers.get('X-Race')).toBe('local');
    expect(await gunzip(body!)).toEqual(tiles.get('1/1/1'));
    expect(tier.drained).toBe(true);
  });

  it("serves the cache's failure when the local read finds no tile", async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    mockStorage();
    const { response } = await fetchGateway('/v1/1/0/0.mvt', { delay: 300, response: failure });
    expect(response.status).toBe(500);
    expect(response.headers.get('X-Race')).toBe('tier');
    expect(response.headers.get('Cache-Control')).toBe('no-store');
  });

  it.each(['/v1.json', '/v1/style/light'])('never races for %s', async (url) => {
    const storage = mockStorage();
    const json = () => new Response('{}', { headers: { 'Content-Type': 'application/json' } });
    const { response } = await fetchGateway(url, { delay: 300, response: json });
    expect(response.status).toBe(200);
    expect(response.headers.has('X-Race')).toBe(false);
    expect(storage.reads).toBe(0);
  });

  // Cloudflare hands workers Accept-Encoding as "br, gzip", with what the client sent in cf.clientAcceptEncoding.
  const sent = (clientAcceptEncoding: string) => ({ clientAcceptEncoding });
  it.each`
    method    | headers                              | cf                            | vars                 | reason
    ${'HEAD'} | ${{ 'Accept-Encoding': 'gzip' }}     | ${{}}                         | ${{}}                | ${'for HEAD'}
    ${'GET'}  | ${{}}                                | ${{}}                         | ${{}}                | ${'for a client that takes no encoding'}
    ${'GET'}  | ${{ 'Accept-Encoding': 'br' }}       | ${{}}                         | ${{}}                | ${'for a client that takes no gzip'}
    ${'GET'}  | ${{ 'Accept-Encoding': 'gzip;q=0' }} | ${{}}                         | ${{}}                | ${'for a client that refuses gzip'}
    ${'GET'}  | ${{ 'Accept-Encoding': 'br, gzip' }} | ${sent('')}                   | ${{}}                | ${'for a client that sent no Accept-Encoding to Cloudflare'}
    ${'GET'}  | ${{ 'Accept-Encoding': 'br, gzip' }} | ${sent('gzip;q=0, identity')} | ${{}}                | ${'for a client that refused gzip to Cloudflare'}
    ${'GET'}  | ${{ 'Accept-Encoding': 'gzip' }}     | ${{}}                         | ${{ RACE_MS: '-1' }} | ${'with RACE_MS negative'}
  `('never races $reason', async ({ method, headers, cf, vars }) => {
    const storage = mockStorage();
    const { response, tier } = await fetchGateway('/v1/1/0/1.mvt', { delay: 300 }, { method, headers, cf, vars });
    expect(response.status).toBe(200);
    expect(response.headers.has('X-Race')).toBe(false);
    expect(tier.calls).toBe(1);
    // Only Tiles' own read.
    expect(storage.reads).toBe(1);
  });

  it('races for a client whose own Accept-Encoding takes gzip', async () => {
    mockStorage();
    const { response } = await fetchGateway(
      '/v1/1/0/1.mvt',
      { delay: 300 },
      { headers: { 'Accept-Encoding': 'br, gzip' }, cf: sent('gzip, deflate') },
    );
    expect(response.headers.get('X-Race')).toBe('local');
  });

  it('gives the cache RACE_MS to answer', async () => {
    const storage = mockStorage();
    const { response } = await fetchGateway('/v1/1/0/1.mvt', { delay: 300 }, { vars: { RACE_MS: '600' } });
    expect(response.headers.get('X-Race')).toBe('none');
    expect(storage.reads).toBe(1);
  });

  it.each(SAMPLE)('hands clients the bytes of tile %s gzipped once, whichever path answers', async (zxy) => {
    mockStorage();
    // Baseline: Tiles' own answer.
    const baseline = new Uint8Array(
      await (
        await exports.Tiles.fetch(`https://tiles.internal/${key}/v1/${zxy}.mvt?rev=${CACHE_REVISION}`)
      ).arrayBuffer(),
    );
    mockTier({ delay: 400 });
    const local = await fetchClient(`/v1/${zxy}.mvt`, 600);
    mockTier({ delay: 0 });
    const tier = await fetchClient(`/v1/${zxy}.mvt`, 200);
    expect(local.response.headers.get('X-Race')).toBe('local');
    expect(tier.response.headers.get('X-Race')).toBe('none');
    expect(baseline).toEqual(tiles.get(zxy));
    // Decoded once, by the client: a body gzipped twice would still be gzip.
    expect(local.response.headers.get('Content-Encoding')).toBe('gzip');
    expect(local.body).toEqual(baseline);
    expect(tier.body).toEqual(baseline);
  });

  describe('metrics', () => {
    // Every metric pushed and every flush of the metrics, in order.
    const captureMetrics = () => {
      const events: (Metric | 'flush')[] = [];
      vi.spyOn(InfluxMetricsProvider.prototype, 'pushMetric').mockImplementation((metric: Metric) => {
        events.push(metric);
      });
      vi.spyOn(InfluxMetricsProvider.prototype, 'flush').mockImplementation(async () => {
        events.push('flush');
      });
      const tags = (name: string) =>
        events.flatMap((event) => (event !== 'flush' && event.name === name ? [Object.fromEntries(event.tags)] : []));
      const names = () => events.map((event) => (event === 'flush' ? event : event.name));
      return { tags, names };
    };

    it("tags the gateway's request and cache metrics with what answered", async () => {
      const { tags } = captureMetrics();
      mockStorage();
      await fetchGateway('/v1/0/0/0.mvt', { delay: 0, response: cachedTile('0/0/0', 'HIT') });
      // A stored copy, late: the gateway's own read answers.
      await fetchGateway('/v1/1/0/1.mvt', { delay: 400, response: cachedTile('1/0/1', 'HIT') });
      mockStorage(600);
      await fetchGateway('/v1/2/1/2.mvt', { delay: 200, response: cachedTile('2/1/2', 'MISS') });
      // Not raced.
      await fetchGateway('/v1.json', { delay: 0, response: cachedTile('0/0/0', 'HIT') });

      const outcome = ({ cache_status, race }: Record<string, string>) => ({ cache_status, race });
      expect(tags('tiles_handle_request').map(outcome)).toEqual([
        { cache_status: 'hit', race: 'none' },
        { cache_status: 'none', race: 'local' },
        { cache_status: 'miss', race: 'tier' },
        { cache_status: 'hit', race: 'none' },
      ]);
      expect(tags('tiles_cache_fetch').map(outcome)).toEqual([
        { cache_status: 'hit', race: 'none' },
        { cache_status: 'hit', race: 'local' },
        { cache_status: 'miss', race: 'tier' },
        { cache_status: 'hit', race: 'none' },
      ]);
      // A stored copy counts as a CDN hit whichever answer the client got, as Tiles didn't run for it.
      expect(
        tags('tiles_cdn_hit').map(({ request_type, cache_status, race }) => [request_type, cache_status, race]),
      ).toEqual([
        ['tile', 'hit', 'none'],
        ['tile', 'hit', 'local'],
        ['json', 'hit', 'none'],
      ]);
      // The gateway's own reads count apart from Tiles' runs, which count requests, and name the gateway's colo.
      expect(tags('tiles_tile_request')).toEqual([]);
      expect(tags('tiles_local_tile_request')).toEqual([
        expect.objectContaining({ z: '1', url: 'unversioned', colo: 'LHR' }),
        expect.objectContaining({ z: '2', url: 'unversioned', colo: 'LHR' }),
      ]);
      expect(tags('tiles_local_d1_lookup').map(({ tiles_colo }) => tiles_colo)).toEqual(['LHR', 'LHR']);
    });

    it('sends the metrics once a late cache answer has reported', async () => {
      const { names } = captureMetrics();
      mockStorage();
      await fetchGateway('/v1/1/0/1.mvt', { delay: 400, response: cachedTile('1/0/1', 'HIT') });
      const sent = names();
      expect(sent.filter((name) => name === 'flush')).toHaveLength(1);
      expect(sent.indexOf('tiles_handle_request')).toBeLessThan(sent.indexOf('tiles_cache_fetch'));
      expect(sent.indexOf('tiles_cache_fetch')).toBeLessThan(sent.indexOf('flush'));
      expect(sent.indexOf('tiles_cdn_hit')).toBeLessThan(sent.indexOf('flush'));
    });
  });
});
