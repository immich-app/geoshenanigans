import { createExecutionContext, createScheduledController, env, waitOnExecutionContext } from 'cloudflare:test';
import worker from '../src';
import { runBunnyCron } from '../src/bunny-logs';
import { probeConfig, probeIntervalMinutes } from '../src/bunny-logs/config';
import { LOGGING_API } from '../src/bunny-logs/log-reader';
import { GLOBALPING_API, HIT_TILE, missTile, PROBE_CONTINENTS, probeDue, runProbe } from '../src/bunny-logs/probe';
import { O11yLogger } from '../src/o11y';
import { Call, json, mockFetch, o11y, pushedLogs, pushedSamples, testDeps } from './bunny-helpers';

// 12:10, a probe minute at the default interval.
const scheduled = Date.UTC(2026, 10, 18, 12, 10, 0);
const config = { token: 'gp-token', publicUrl: new URL('https://tiles.example.com'), intervalMinutes: 10 };

const finished = (continent: string, firstByte: number, fields: object = {}) => ({
  probe: { continent, country: 'XX', city: 'Somewhere', network: 'Example Net', asn: 64500, tags: [], resolvers: [] },
  result: {
    status: 'finished',
    statusCode: 200,
    headers: { 'cdn-cache': 'HIT', server: 'BunnyCDN-DE1-1083' },
    timings: { total: firstByte + 40, dns: 5, tcp: 10, tls: 20, firstByte, download: 1 },
    rawOutput: '',
    ...fields,
  },
});

const measurement = (id: string, status: string, results: unknown[]) =>
  json({ id, type: 'http', status, target: 'tiles.example.com', probesCount: results.length, results });

type Routes = {
  create?: (call: Call, index: number) => Response;
  get?: (id: string, poll: number) => Response;
};

function globalping({ create, get }: Routes) {
  let creates = 0;
  const polls = new Map<string, number>();
  return (call: Call) => {
    if (call.url === GLOBALPING_API && call.method === 'POST') {
      return create?.(call, creates++) ?? json({ id: `m${creates}`, probesCount: 6 }, 202);
    }
    if (call.url.startsWith(`${GLOBALPING_API}/`) && call.method === 'GET') {
      const id = decodeURIComponent(call.url.slice(GLOBALPING_API.length + 1));
      const poll = polls.get(id) ?? 0;
      polls.set(id, poll + 1);
      return get?.(id, poll);
    }
    return undefined;
  };
}

// null: no settings.
async function probe(routes: Routes, probeSettings: typeof config | null = config) {
  const fetchMock = mockFetch(globalping(routes));
  const deps = testDeps(scheduled + 3000, () => 0.25);
  const logger = new O11yLogger(o11y);
  const result = await runProbe(probeSettings ?? undefined, o11y, logger, scheduled, deps);
  await logger.flush();
  return { result, deps, ...fetchMock };
}

const creates = (calls: Call[]) => calls.filter(({ url, method }) => url === GLOBALPING_API && method === 'POST');
const metrics = (calls: Call[]) => pushedSamples(calls).map(({ name, labels, value }) => ({ name, labels, value }));

beforeEach(() => {
  vi.spyOn(console, 'info').mockImplementation(() => {});
  vi.spyOn(console, 'warn').mockImplementation(() => {});
  vi.spyOn(console, 'error').mockImplementation(() => {});
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe('probe settings', () => {
  it('needs the Globalping token and an http(s) public URL', () => {
    expect(probeConfig({ GLOBALPING_TOKEN: 'gp' })).toBeUndefined();
    expect(probeConfig({ BUNNY_PUBLIC_URL: 'https://tiles.example.com' })).toBeUndefined();
    expect(probeConfig({ GLOBALPING_TOKEN: 'gp', BUNNY_PUBLIC_URL: 'tiles.example.com' })).toBeUndefined();
    expect(probeConfig({ GLOBALPING_TOKEN: 'gp', BUNNY_PUBLIC_URL: 'ftp://tiles.example.com' })).toBeUndefined();
    expect(probeConfig({ GLOBALPING_TOKEN: 'gp-token', BUNNY_PUBLIC_URL: 'https://tiles.example.com' })).toEqual(
      config,
    );
  });

  it('runs every 10 minutes unless told otherwise, and never more often than every 5', () => {
    expect(probeIntervalMinutes(undefined)).toBe(10);
    expect(probeIntervalMinutes('')).toBe(10);
    expect(probeIntervalMinutes('often')).toBe(10);
    expect(probeIntervalMinutes('0')).toBe(10);
    expect(probeIntervalMinutes('2.5')).toBe(10);
    expect(probeIntervalMinutes('1')).toBe(5);
    expect(probeIntervalMinutes('15')).toBe(15);
    expect(probeDue(scheduled, 10)).toBe(true);
    expect(probeDue(scheduled + 59_000, 10)).toBe(true);
    expect(probeDue(scheduled + 60_000, 10)).toBe(false);
    expect(probeDue(scheduled + 5 * 60_000, 5)).toBe(true);
  });

  it('misses with a z15 position inside the grid', () => {
    expect(missTile(() => 0)).toBe('15/0/0');
    expect(missTile(() => 0.9999999999)).toBe('15/32767/32767');
    expect(missTile(() => 0.5)).toBe('15/16384/16384');
  });
});

describe('runProbe', () => {
  it('measures the hit and miss tiles from the same probe per continent and pushes their TTFBs', async () => {
    const results = (base: number) => [
      finished('EU', base),
      finished('NA', base + 10),
      finished('SA', base + 20),
      finished('AS', base + 30),
      finished('OC', base + 40),
      {
        probe: { continent: 'AF' },
        result: { status: 'failed', failureSource: 'target', rawOutput: 'connect ETIMEDOUT', statusCode: null },
      },
    ];
    const { result, calls, deps } = await probe({
      create: (_call, index) => json({ id: index === 0 ? 'hit-1' : 'miss-1', probesCount: 6 }, 202),
      get: (id, poll) =>
        poll === 0
          ? measurement(id, 'in-progress', [])
          : measurement(id, 'finished', results(id === 'hit-1' ? 20 : 200)),
    });
    expect(result).toBe('ok');

    const [hit, miss] = creates(calls);
    expect(hit.headers.get('Authorization')).toBe('Bearer gp-token');
    expect(hit.headers.get('User-Agent')).toMatch(/^immich-tiles-probe\//);
    expect(JSON.parse(hit.body)).toEqual({
      type: 'http',
      target: 'tiles.example.com',
      locations: PROBE_CONTINENTS.map((continent) => ({ continent, limit: 1 })),
      measurementOptions: {
        protocol: 'HTTPS',
        request: { method: 'GET', path: `/v1/${HIT_TILE}.mvt`, headers: { 'Accept-Encoding': 'gzip' } },
      },
    });
    expect(JSON.parse(miss.body)).toMatchObject({
      locations: 'hit-1',
      measurementOptions: { request: { path: '/v1/15/8192/8192.mvt' } },
    });
    // Polled no faster than the API allows.
    expect(deps.sleeps.every((ms) => ms >= 1000)).toBe(true);

    const samples = pushedSamples(calls);
    expect(new Set(samples.map(({ timestamp }) => timestamp))).toEqual(new Set([scheduled]));
    expect(metrics(calls)).toEqual(
      expect.arrayContaining([
        { name: 'tiles_bunny_probe_ttfb_ms', labels: { continent: 'EU', kind: 'hit' }, value: 20 },
        { name: 'tiles_bunny_probe_ttfb_ms', labels: { continent: 'OC', kind: 'hit' }, value: 60 },
        { name: 'tiles_bunny_probe_ttfb_ms', labels: { continent: 'EU', kind: 'miss' }, value: 200 },
        { name: 'tiles_bunny_probe_errors', labels: { continent: 'EU', kind: 'hit' }, value: 0 },
        { name: 'tiles_bunny_probe_errors', labels: { continent: 'AF', kind: 'hit' }, value: 1 },
        { name: 'tiles_bunny_probe_errors', labels: { continent: 'AF', kind: 'miss' }, value: 1 },
        { name: 'tiles_bunny_probe_runs', labels: { result: 'ok' }, value: 1 },
      ]),
    );
    expect(samples.filter(({ name }) => name === 'tiles_bunny_probe_ttfb_ms')).toHaveLength(10);
    expect(samples.filter(({ name }) => name === 'tiles_bunny_probe_errors')).toHaveLength(12);

    const lines = pushedLogs(calls).filter(({ kind }) => kind === 'probe');
    expect(lines).toHaveLength(12);
    expect(lines[0]).toMatchObject({
      _msg: 'Probe',
      target: 'hit',
      measurement_id: 'hit-1',
      continent: 'EU',
      status: 200,
      ttfb_ms: 20,
      total_ms: 60,
      cache: 'HIT',
      server: 'BunnyCDN-DE1-1083',
    });
    expect(lines[0]).not.toHaveProperty('failure');
    expect(lines[5]).toMatchObject({
      continent: 'AF',
      test_status: 'failed',
      failure: 'connect ETIMEDOUT',
      failure_source: 'target',
    });
    expect(lines[5]).not.toHaveProperty('ttfb_ms');
  });

  it("doesn't count a probe that went offline, and takes any status but in-progress as final", async () => {
    const { result, calls } = await probe({
      get: (id) =>
        measurement(id, 'done-somehow', [
          finished('EU', 30),
          { probe: { continent: 'NA' }, result: { status: 'offline', rawOutput: '' } },
        ]),
    });
    expect(result).toBe('ok');
    expect(calls.filter(({ method }) => method === 'GET')).toHaveLength(2);
    expect(metrics(calls).filter(({ labels }) => labels.continent === 'NA')).toEqual([]);
  });

  it('counts a non-200 answer as an error, not a TTFB', async () => {
    const { calls } = await probe({
      get: (id) => measurement(id, 'finished', [finished('EU', 30, { statusCode: 404 })]),
    });
    expect(metrics(calls)).toEqual(
      expect.arrayContaining([
        { name: 'tiles_bunny_probe_errors', labels: { continent: 'EU', kind: 'hit' }, value: 1 },
      ]),
    );
    expect(metrics(calls).filter(({ name }) => name === 'tiles_bunny_probe_ttfb_ms')).toEqual([]);
  });

  it('reports an error when Globalping refuses, measuring what it can on its own probes', async () => {
    const { result, calls } = await probe({
      create: (_call, index) =>
        index === 0
          ? json({ error: { type: 'rate_limit_exceeded', message: 'API rate limit exceeded.' } }, 429)
          : json({ id: 'miss-2', probesCount: 6 }, 202),
      get: (id) => measurement(id, 'finished', [finished('EU', 150)]),
    });
    expect(result).toBe('error');
    expect(JSON.parse(creates(calls)[1].body).locations).toEqual(
      PROBE_CONTINENTS.map((continent) => ({ continent, limit: 1 })),
    );
    expect(metrics(calls)).toEqual([
      { name: 'tiles_bunny_probe_ttfb_ms', labels: { continent: 'EU', kind: 'miss' }, value: 150 },
      { name: 'tiles_bunny_probe_errors', labels: { continent: 'EU', kind: 'miss' }, value: 0 },
      { name: 'tiles_bunny_probe_runs', labels: { result: 'error' }, value: 1 },
    ]);
    expect(pushedLogs(calls)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          _msg: 'Globalping measurement not created',
          target: 'hit',
          error: expect.stringContaining('429'),
        }),
      ]),
    );
  });

  it('pushes only the run when no measurement could be created', async () => {
    const { result, calls } = await probe({ create: () => new Response('down', { status: 503 }) });
    expect(result).toBe('error');
    expect(calls.filter(({ method }) => method === 'GET')).toHaveLength(0);
    expect(metrics(calls)).toEqual([{ name: 'tiles_bunny_probe_runs', labels: { result: 'error' }, value: 1 }]);
  });

  it('gives up at the deadline, counting the unfinished tests as errors', async () => {
    const { result, calls, deps } = await probe({
      get: (id) =>
        measurement(id, 'in-progress', [
          finished('EU', 25),
          { probe: { continent: 'NA' }, result: { status: 'in-progress', rawOutput: '' } },
        ]),
    });
    expect(result).toBe('error');
    const waited = deps.now() - (scheduled + 3000);
    expect(waited).toBeGreaterThanOrEqual(45_000);
    expect(waited).toBeLessThan(50_000);
    expect(metrics(calls)).toEqual(
      expect.arrayContaining([
        { name: 'tiles_bunny_probe_ttfb_ms', labels: { continent: 'EU', kind: 'hit' }, value: 25 },
        { name: 'tiles_bunny_probe_errors', labels: { continent: 'NA', kind: 'hit' }, value: 1 },
        { name: 'tiles_bunny_probe_runs', labels: { result: 'error' }, value: 1 },
      ]),
    );
  });

  it('keeps polling through a failed read of the results', async () => {
    const { result, calls } = await probe({
      get: (id, poll) =>
        poll === 0 ? new Response('busy', { status: 429 }) : measurement(id, 'finished', [finished('EU', 40)]),
    });
    expect(result).toBe('ok');
    expect(metrics(calls)).toEqual(
      expect.arrayContaining([{ name: 'tiles_bunny_probe_runs', labels: { result: 'ok' }, value: 1 }]),
    );
  });

  it('is skipped, calling nothing but o11y, without its settings', async () => {
    const { result, calls } = await probe({}, null);
    expect(result).toBe('skipped');
    expect(calls.filter(({ url }) => url.startsWith(GLOBALPING_API))).toHaveLength(0);
    expect(pushedSamples(calls)).toEqual([
      { name: 'tiles_bunny_probe_runs', labels: { result: 'skipped' }, value: 1, timestamp: scheduled },
    ]);
  });
});

describe('the Bunny cron', () => {
  const page = () => json({ data: [], pagination: { hasMore: false } });

  it('runs the log reader every minute and the probe on its minutes, each skipped without its settings', async () => {
    const { calls } = mockFetch();
    await runBunnyCron(env as WorkerEnv, o11y, scheduled, testDeps(scheduled));
    expect(metrics(calls)).toEqual(
      expect.arrayContaining([
        { name: 'tiles_bunny_log_reader_runs', labels: { result: 'skipped' }, value: 1 },
        { name: 'tiles_bunny_probe_runs', labels: { result: 'skipped' }, value: 1 },
      ]),
    );
    calls.length = 0;
    await runBunnyCron(env as WorkerEnv, o11y, scheduled + 60_000, testDeps(scheduled));
    expect(metrics(calls)).toEqual([{ name: 'tiles_bunny_log_reader_runs', labels: { result: 'skipped' }, value: 1 }]);
  });

  it('reads the logs and probes with the settings from the environment', async () => {
    const routes = globalping({ get: (id) => measurement(id, 'finished', [finished('EU', 10)]) });
    const { calls } = mockFetch((call) => (call.url.startsWith(LOGGING_API) ? page() : routes(call)));
    const bunnyEnv = {
      ...env,
      BUNNY_API_KEY: 'account-key',
      BUNNY_PULL_ZONE_ID: '12345',
      BUNNY_PUBLIC_URL: 'https://tiles.example.com',
      GLOBALPING_TOKEN: 'gp-token',
      BUNNY_PROBE_INTERVAL_MINUTES: '5',
    } as WorkerEnv;
    await runBunnyCron(bunnyEnv, o11y, scheduled + 5 * 60_000, testDeps(scheduled));
    expect(calls.some(({ url }) => url.startsWith(`${LOGGING_API}/12345/logs`))).toBe(true);
    expect(metrics(calls)).toEqual(
      expect.arrayContaining([
        { name: 'tiles_bunny_log_reader_runs', labels: { result: 'ok' }, value: 1 },
        { name: 'tiles_bunny_probe_runs', labels: { result: 'ok' }, value: 1 },
      ]),
    );
  });

  it("is part of the worker's scheduled run, next to the heartbeat", async () => {
    const { calls } = mockFetch();
    const ctx = createExecutionContext();
    const o11yEnv = { ...env, ENVIRONMENT: 'prod', O11Y_URL: 'https://o11y.test', O11Y_TOKEN: 'secret' };
    await worker.scheduled!(
      createScheduledController({ cron: '* * * * *', scheduledTime: scheduled + 60_000 }),
      o11yEnv as typeof env,
      ctx,
    );
    await waitOnExecutionContext(ctx);
    expect(calls.map(({ url }) => url).sort()).toEqual([
      'https://o11y.test/insert/0/influx/write',
      'https://o11y.test/insert/0/prometheus/api/v1/import/prometheus',
    ]);
    expect(metrics(calls)).toEqual([{ name: 'tiles_bunny_log_reader_runs', labels: { result: 'skipped' }, value: 1 }]);
  });
});
