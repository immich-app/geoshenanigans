import { createExecutionContext, createScheduledController, env, waitOnExecutionContext } from 'cloudflare:test';
import worker from '../src';
import {
  heartbeatLine,
  influxLines,
  O11yConfig,
  o11yConfig,
  O11yLogger,
  o11yMeasurement,
  O11yMetricsProvider,
  sampleRequest,
  sendHeartbeat,
  vmrange,
} from '../src/o11y';
import { DirectoryString } from '../src/pmtiles.service';
import { Metric } from '../src/repository';

const IncomingRequest = Request<unknown, IncomingRequestCfProperties>;

const identity = { project: 'geoshenanigans', cluster: 'tiles', env: 'prod', provider: 'cloudflare', region: 'world' };
const config: O11yConfig = { url: 'https://o11y.test', token: 'secret', identity };
const identityTags = 'project=geoshenanigans,cluster=tiles,env=prod,provider=cloudflare,region=world';

type Call = { url: string; init: RequestInit };

/**
 * Replaces fetch: o11y POSTs are recorded and answered 204, Tigris reads get `tigris`, and anything else (the test
 * runtime's own traffic, such as the Cache API's) goes through.
 */
function mockFetch(tigris: () => Response = () => new Response('unexpected', { status: 599 })) {
  const realFetch = globalThis.fetch.bind(globalThis);
  const calls: Call[] = [];
  const spy = vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
    const url = input instanceof Request ? input.url : String(input);
    if (url.startsWith('https://o11y.test/')) {
      calls.push({ url, init: init ?? {} });
      return new Response(null, { status: 204 });
    }
    if (new URL(url).hostname.endsWith('.tigris.dev')) {
      return tigris();
    }
    return realFetch(input, init);
  });
  const o11yCalls = () =>
    spy.mock.calls.filter(([input]) =>
      String(input instanceof Request ? input.url : input).startsWith('https://o11y.test/'),
    );
  return { calls, o11yCalls };
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe('o11yConfig', () => {
  it('ships nothing without both the gateway and the token', () => {
    expect(o11yConfig({ ENVIRONMENT: 'prod' })).toBeUndefined();
    expect(o11yConfig({ ENVIRONMENT: 'prod', O11Y_URL: 'https://o11y.test' })).toBeUndefined();
    expect(o11yConfig({ ENVIRONMENT: 'prod', O11Y_TOKEN: 'secret' })).toBeUndefined();
    expect(o11yConfig({ ENVIRONMENT: 'prod', O11Y_URL: ' ', O11Y_TOKEN: 'secret' })).toBeUndefined();
  });

  it("stamps the tiles identity with o11y's short environment names", () => {
    expect(o11yConfig({ ENVIRONMENT: 'prod', O11Y_URL: 'https://o11y.test/', O11Y_TOKEN: 'secret' })).toEqual(config);
    expect(
      o11yConfig({ ENVIRONMENT: 'production', O11Y_URL: 'https://o11y.test', O11Y_TOKEN: 'secret' })?.identity.env,
    ).toBe('prod');
    expect(o11yConfig({ ENVIRONMENT: 'dev', O11Y_URL: 'https://o11y.test', O11Y_TOKEN: 'secret' })?.identity.env).toBe(
      'dev',
    );
  });
});

describe('influxLines', () => {
  // A metric without a duration has one line at most.
  const oneLine = (metric: Metric) => {
    const lines = influxLines(metric, identity);
    expect(lines.length).toBeLessThanOrEqual(1);
    return lines[0];
  };

  it('writes the identity labels, the kept tags and every field', () => {
    const metric = Metric.create('tiles_tile_request')
      .addTag('z', '5')
      .addTag('continent', 'EU')
      .intField('invocation', 1)
      .durationField('duration', 12.5);
    expect(influxLines(metric, identity)).toEqual([
      `tiles_tile_request,${identityTags},z=5,continent=EU invocation=1,duration=12.5`,
      `tiles_tile_request_duration,${identityTags},z=5,continent=EU,vmrange=1.136e+01...1.292e+01 bucket=1`,
    ]);
  });

  it("leaves out tags that would make a series per client, query or error, and anything it doesn't know", () => {
    const metric = Metric.create('d1_query')
      .addTags({
        continent: 'NA',
        colo: 'IAD',
        asOrg: 'Example Networks',
        apexDomain: 'example.com',
        protocol: 'https:',
      })
      .addTag('query', 'SELECT * FROM t WHERE id = ?')
      .addTag('error', 'boom')
      .addTag('project', 'someone-else')
      .addTag('served_by_region', 'ENAM')
      .addTag('surprise', 'x')
      .intField('count', 1);
    expect(influxLines(metric, identity)).toEqual([
      `tiles_d1_query,${identityTags},continent=NA,served_by_region=ENAM count=1`,
    ]);
  });

  it('keeps the colo only on the request as a whole', () => {
    const tags = { continent: 'EU', colo: 'AMS' };
    expect(oneLine(Metric.create('tiles_handle_request').addTags(tags).intField('invocation', 1))).toBe(
      `tiles_handle_request,${identityTags},continent=EU,colo=AMS invocation=1`,
    );
    expect(oneLine(Metric.create('tiles_tile_request').addTags(tags).intField('invocation', 1))).toBe(
      `tiles_tile_request,${identityTags},continent=EU invocation=1`,
    );
  });

  it('keeps only the zooms, versions and styles the worker serves, as the URL could name any', () => {
    const line = (tags: Record<string, string>) =>
      oneLine(Metric.create('tiles_tile_request').addTags(tags).intField('invocation', 1));
    expect(line({ z: '7', version: '1' })).toBe(`tiles_tile_request,${identityTags},z=7,version=1 invocation=1`);
    expect(line({ z: '007', version: '123456' })).toBe(
      `tiles_tile_request,${identityTags},z=7,version=other invocation=1`,
    );
    expect(line({ z: '16' })).toBe(`tiles_tile_request,${identityTags},z=other invocation=1`);
    expect(line({ z: '99999999999999999999' })).toBe(`tiles_tile_request,${identityTags},z=other invocation=1`);
    expect(line({ style: 'dark' })).toBe(`tiles_tile_request,${identityTags},style=dark invocation=1`);
    expect(line({ style: 'arbitrary_name' })).toBe(`tiles_tile_request,${identityTags},style=other invocation=1`);
  });

  it("gives each duration a 1 in its VictoriaMetrics histogram bucket, with the metric's tags", () => {
    const metric = Metric.create('d1_query')
      .addTag('served_by_region', 'WEUR')
      .intField('count', 1)
      .durationField('duration', 30)
      .intField('sql_duration', 2);
    expect(influxLines(metric, identity)).toEqual([
      `tiles_d1_query,${identityTags},served_by_region=WEUR count=1,duration=30,sql_duration=2`,
      `tiles_d1_query_duration,${identityTags},served_by_region=WEUR,vmrange=2.783e+01...3.162e+01 bucket=1`,
    ]);
  });

  it('drops empty tags and non-finite fields, and writes nothing without a field', () => {
    const metric = Metric.create('tiles_handle_request')
      .addTag('continent', '')
      .intField('invocation', 1)
      .intField('broken', Number.NaN);
    expect(oneLine(metric)).toBe(`tiles_handle_request,${identityTags} invocation=1`);
    expect(oneLine(Metric.create('tiles_handle_request').addTag('continent', 'EU'))).toBeUndefined();
  });

  it('escapes what line protocol reserves', () => {
    const metric = Metric.create('tiles_handle_request').addTag('colo', 'a b,c=d').intField('invocation', 1);
    expect(oneLine(metric)).toBe(`tiles_handle_request,${identityTags},colo=a\\ b\\,c\\=d invocation=1`);
  });

  it('names every series tiles_*', () => {
    expect(o11yMeasurement('tigris_storage_get')).toBe('tiles_tigris_storage_get');
    expect(o11yMeasurement('tiles_cdn_hit')).toBe('tiles_cdn_hit');
  });
});

describe('vmrange', () => {
  it("names the bucket as VictoriaMetrics' histogram_bucket does, an edge in the bucket below", () => {
    // Buckets VictoriaMetrics v1.155.0's stream aggregation wrote for these values.
    expect(vmrange(10)).toBe('8.799e+00...1.000e+01');
    expect(vmrange(20)).toBe('1.896e+01...2.154e+01');
    expect(vmrange(30)).toBe('2.783e+01...3.162e+01');
    expect(vmrange(70)).toBe('6.813e+01...7.743e+01');
    expect(vmrange(100)).toBe('8.799e+01...1.000e+02');
    expect(vmrange(140)).toBe('1.292e+02...1.468e+02');
    expect(vmrange(0.5)).toBe('4.642e-01...5.275e-01');
    expect(vmrange(4000)).toBe('3.594e+03...4.084e+03');
  });

  it('puts zero and below in the lowest bucket', () => {
    expect(vmrange(0)).toBe('0...1.000e-09');
    expect(vmrange(-1)).toBe('0...1.000e-09');
  });
});

describe('O11yMetricsProvider', () => {
  it("sends a request's lines to the serverless ingest path in one POST", async () => {
    const { calls } = mockFetch();
    const provider = new O11yMetricsProvider(config);
    provider.pushMetric(Metric.create('tiles_handle_request').intField('invocation', 1));
    provider.pushMetric(Metric.create('tiles_cdn_hit').addTag('request_type', 'tile').intField('count', 1));
    await provider.flush();
    expect(calls).toHaveLength(1);
    expect(calls[0].url).toBe('https://o11y.test/serverless/insert/influx/write');
    expect(calls[0].init.method).toBe('POST');
    expect(new Headers(calls[0].init.headers).get('Authorization')).toBe('Bearer secret');
    expect(String(calls[0].init.body).split('\n')).toEqual([
      `tiles_handle_request,${identityTags} invocation=1`,
      `tiles_cdn_hit,${identityTags},request_type=tile count=1`,
    ]);
    await provider.flush();
    expect(calls).toHaveLength(1);
  });

  it('sends nothing without a config', async () => {
    const { o11yCalls } = mockFetch();
    const provider = new O11yMetricsProvider(undefined);
    provider.pushMetric(Metric.create('tiles_handle_request').intField('invocation', 1));
    await provider.flush();
    expect(o11yCalls()).toHaveLength(0);
  });

  it('never fails the request when the gateway does', async () => {
    vi.spyOn(globalThis, 'fetch').mockRejectedValue(new Error('unreachable'));
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
    const provider = new O11yMetricsProvider(config);
    provider.pushMetric(Metric.create('tiles_handle_request').intField('invocation', 1));
    await expect(provider.flush()).resolves.toBeUndefined();

    vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('bad token', { status: 401 }));
    provider.pushMetric(Metric.create('tiles_handle_request').intField('invocation', 1));
    await expect(provider.flush()).resolves.toBeUndefined();
    expect(consoleError).toHaveBeenLastCalledWith('o11y: /serverless/insert/influx/write answered 401', 'bad token');
  });
});

describe('O11yLogger', () => {
  it("sends a request's lines to VictoriaLogs with its fields and the identity labels", async () => {
    const { calls } = mockFetch();
    vi.spyOn(console, 'info').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const logger = new O11yLogger(config, { ray: 'abc-AMS', path: '/v1.json' });
    logger.info('Request', { status: 200, project: 'not-this' });
    logger.error('Request failed', new Error('boom'), { request_type: 'tile' });
    await logger.flush();

    expect(calls).toHaveLength(1);
    expect(calls[0].url).toBe('https://o11y.test/insert/jsonline?_stream_fields=project,cluster,env');
    expect(new Headers(calls[0].init.headers).get('Content-Type')).toBe('application/stream+json');
    const [info, error] = String(calls[0].init.body)
      .trimEnd()
      .split('\n')
      .map((line) => JSON.parse(line));
    expect(info).toMatchObject({ _msg: 'Request', level: 'info', status: 200, ray: 'abc-AMS', ...identity });
    expect(error).toMatchObject({ _msg: 'Request failed', level: 'error', error: 'boom', request_type: 'tile' });
    expect(error.stack).toContain('boom');
    expect(Date.parse(info._time)).not.toBeNaN();
  });

  it('only writes to the console without a config', async () => {
    const { o11yCalls } = mockFetch();
    const consoleWarn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const logger = new O11yLogger(undefined, { ray: 'abc' });
    logger.warn('Slow', { ms: 5 });
    await logger.flush();
    expect(consoleWarn).toHaveBeenCalledWith('Slow', { ray: 'abc', ms: 5 });
    expect(o11yCalls()).toHaveLength(0);
  });
});

describe('heartbeat', () => {
  it('is a gauge of when it was sent, with the identity labels and the deployment served', () => {
    expect(heartbeatLine(identity, '20261101', 1794140000123)).toBe(
      `tiles_heartbeat,${identityTags},deployment_key=20261101 timestamp_seconds=1794140000`,
    );
  });

  it('goes straight to /insert/0/, not the serverless path, as one sample', async () => {
    const { calls } = mockFetch();
    await sendHeartbeat(config, '20261101', 1794140000000);
    expect(calls).toHaveLength(1);
    expect(calls[0].url).toBe('https://o11y.test/insert/0/influx/write');
    expect(new Headers(calls[0].init.headers).get('Authorization')).toBe('Bearer secret');
    expect(String(calls[0].init.body)).toBe(
      `tiles_heartbeat,${identityTags},deployment_key=20261101 timestamp_seconds=1794140000`,
    );
  });

  it("is what the worker's cron sends, and only the tiles worker with a gateway sends it", async () => {
    const run = async (bindings: object) => {
      const ctx = createExecutionContext();
      await worker.scheduled!(createScheduledController({ cron: '* * * * *' }), bindings as typeof env, ctx);
      await waitOnExecutionContext(ctx);
    };
    const { calls } = mockFetch();
    const o11yEnv = { ...env, ENVIRONMENT: 'prod', O11Y_URL: 'https://o11y.test', O11Y_TOKEN: 'secret' };
    await run(o11yEnv);
    expect(calls.map(({ url, init }) => [url, String(init.body)])).toEqual([
      [
        'https://o11y.test/insert/0/influx/write',
        expect.stringMatching(
          new RegExp(`^tiles_heartbeat,${identityTags},deployment_key=${env.DEPLOYMENT_KEY} timestamp_seconds=\\d+$`),
        ),
      ],
    ]);
    await run(env);
    await run({ ...o11yEnv, WORKER_TYPE: 'D1_PROXY' });
    expect(calls).toHaveLength(1);
  });
});

describe('sampleRequest', () => {
  it('samples 1% unless told otherwise', () => {
    expect(sampleRequest(undefined, () => 0.005)).toBe(true);
    expect(sampleRequest(undefined, () => 0.02)).toBe(false);
    expect(sampleRequest('', () => 0.005)).toBe(true);
    expect(sampleRequest('0.5', () => 0.4)).toBe(true);
    expect(sampleRequest('0', () => 0)).toBe(false);
    expect(sampleRequest('1', () => 0.99)).toBe(true);
    expect(sampleRequest('lots', () => 0.02)).toBe(false);
    expect(sampleRequest('2', () => 0.02)).toBe(false);
  });
});

describe('worker o11y', () => {
  const key = env.DEPLOYMENT_KEY;
  const o11yEnv = {
    ...env,
    ENVIRONMENT: 'prod',
    O11Y_URL: 'https://o11y.test',
    O11Y_TOKEN: 'secret',
    TIGRIS_KEY_ID: 'key',
    TIGRIS_ACCESS_KEY: 'secret',
  };
  const fetchWith = async (url: string, bindings: object, headers: Record<string, string> = {}) => {
    const ctx = createExecutionContext();
    const response = await worker.fetch(new IncomingRequest(url, { headers }), bindings as typeof env, ctx);
    await waitOnExecutionContext(ctx);
    return response;
  };
  const bodies = (calls: Call[], path: string) =>
    calls
      .filter(({ url }) => new URL(url).pathname === path)
      .flatMap(({ init }) => String(init.body).trimEnd().split('\n'));

  beforeAll(async () => {
    const tileJson = { tilejson: '3.0.0', tiles: [], vector_layers: [], minzoom: 0, maxzoom: 15 };
    const directory = DirectoryString.fromDirectory({
      tileIdStart: 0,
      offsetStart: 0,
      entries: [{ tileId: 0, offset: 0, length: 10, runLength: 1, chunkId: 0 }],
    }).toString();
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

  it('ships the metrics of a CDN hit, its response status and a sampled request log', async () => {
    const url = 'https://tiles.example/v1/style/light.json?case=o11y';
    await caches.default.put(
      url,
      new Response('{}', {
        headers: { 'PMTiles-Deployment-Key': key, 'Tiles-Cache-Revision': '2', 'Cache-Control': 'public, max-age=300' },
      }),
    );
    const { calls } = mockFetch();
    vi.spyOn(console, 'info').mockImplementation(() => {});
    const response = await fetchWith(
      url,
      { ...o11yEnv, REQUEST_LOG_SAMPLE_RATE: '1' },
      { Origin: 'https://photos.example.org' },
    );
    expect(response.status).toBe(200);

    const metrics = bodies(calls, '/serverless/insert/influx/write');
    expect(metrics).toContainEqual(
      expect.stringMatching(
        /^tiles_handle_request,project=geoshenanigans,cluster=tiles,env=prod,provider=cloudflare,region=world.* invocation=1,duration=/,
      ),
    );
    expect(metrics).toContainEqual(expect.stringMatching(/^tiles_cdn_hit,.*request_type=style,style=light.* count=1$/));
    expect(metrics).toContainEqual(expect.stringMatching(/^tiles_response,.*request_type=style,status=200 count=1$/));
    expect(metrics.join('\n')).not.toMatch(/apexDomain|asOrg|protocol|example\.org/);

    const [log] = bodies(calls, '/insert/jsonline').map((line) => JSON.parse(line));
    expect(log).toMatchObject({
      _msg: 'Request',
      kind: 'request',
      request_type: 'style',
      status: 200,
      apex_domain: 'example.org',
      path: '/v1/style/light.json',
      method: 'GET',
      project: 'geoshenanigans',
      cluster: 'tiles',
      env: 'prod',
    });
  });

  it('ships a failed request as a 500, an error count and an error log', async () => {
    // Tigris refuses the tile read.
    const { calls } = mockFetch(() => new Response('denied', { status: 403 }));
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const response = await fetchWith('https://tiles.example/v1/0/0/0.mvt?case=o11y-fail', {
      ...o11yEnv,
      REQUEST_LOG_SAMPLE_RATE: '0',
    });
    expect(response.status).toBe(500);

    const metrics = bodies(calls, '/serverless/insert/influx/write');
    expect(metrics).toContainEqual(
      expect.stringMatching(/^tiles_tile_request,.*z=0.* invocation=1,errors=1,duration=/),
    );
    expect(metrics).toContainEqual(expect.stringMatching(/^tiles_request_error,.*request_type=tile count=1$/));
    expect(metrics).toContainEqual(expect.stringMatching(/^tiles_response,.*request_type=tile,status=500 count=1$/));

    const logs = bodies(calls, '/insert/jsonline').map((line) => JSON.parse(line));
    expect(logs).toEqual([
      expect.objectContaining({ _msg: 'Request failed', level: 'error', request_type: 'tile', path: '/v1/0/0/0.mvt' }),
    ]);
  });

  it('ships nothing without the gateway', async () => {
    const { o11yCalls } = mockFetch();
    vi.spyOn(console, 'info').mockImplementation(() => {});
    const url = 'https://tiles.example/v1/style/dark.json?case=o11y-off';
    await caches.default.put(
      url,
      new Response('{}', {
        headers: { 'PMTiles-Deployment-Key': key, 'Tiles-Cache-Revision': '2', 'Cache-Control': 'public, max-age=300' },
      }),
    );
    expect((await fetchWith(url, { ...env, REQUEST_LOG_SAMPLE_RATE: '1' })).status).toBe(200);
    expect(o11yCalls()).toHaveLength(0);
  });
});
