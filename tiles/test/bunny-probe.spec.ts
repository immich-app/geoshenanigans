import { createExecutionContext, createScheduledController, env, waitOnExecutionContext } from 'cloudflare:test';
import worker from '../src';
import { runBunnyCron } from '../src/bunny-logs';
import { MAX_PROBE_INTERVAL_MINUTES, ProbeConfig, probeConfig, probeIntervalMinutes } from '../src/bunny-logs/config';
import { LOGGING_API } from '../src/bunny-logs/log-reader';
import {
  GLOBALPING_API,
  HIT_TILE,
  hitTile,
  missTile,
  PROBE_CONTINENTS,
  probeDue,
  probeTiles,
  runProbe,
  TILEJSON_LABELS,
  tileUrl,
} from '../src/bunny-logs/probe';
import { O11yLogger } from '../src/o11y';
import { Call, json, mockFetch, o11y, pushedLogs, pushedSamples, testDeps } from './bunny-helpers';

// 12:10, a probe minute at the default interval.
const scheduled = Date.UTC(2026, 10, 18, 12, 10, 0);
const config = { token: 'gp-token', publicUrl: new URL('https://tiles.example.com'), intervalMinutes: 10 };

const tileHeaders = (cache = 'HIT') => ({
  'cdn-cache': cache,
  server: 'BunnyCDN-DE1-1083',
  'content-type': 'application/x-protobuf',
  'content-encoding': 'gzip',
});

const finished = (continent: string, firstByte: number, fields: object = {}) => ({
  probe: { continent, country: 'XX', city: 'Somewhere', network: 'Example Net', asn: 64500, tags: [], resolvers: [] },
  result: {
    status: 'finished',
    statusCode: 200,
    headers: tileHeaders(),
    timings: { total: firstByte + 40, dns: 5, tcp: 10, tls: 20, firstByte, download: 1 },
    rawOutput: '',
    ...fields,
  },
});

/** The tests given, plus a finished one from each continent they lack: every continent has a verdict. */
const everywhere = (results: { probe: { continent: string } }[], fields: object = {}) => [
  ...results,
  ...PROBE_CONTINENTS.filter((continent) => !results.some(({ probe }) => probe.continent === continent)).map(
    (continent) => finished(continent, 50, fields),
  ),
];

const measurement = (id: string, status: string, results: unknown[]) =>
  json({ id, type: 'http', status, target: 'tiles.example.com', probesCount: results.length, results });

const TILEJSON_URL = 'https://tiles.example.com/v1.json';
const tileJsonOf = (template: string, fields: object = {}) =>
  json({ tilejson: '3.0.0', name: 'OpenMapTiles', tiles: [template], minzoom: 0, maxzoom: 15, ...fields });

type Routes = {
  create?: (call: Call, index: number) => Response;
  get?: (id: string, poll: number) => Response;
  // The public origin's TileJSON: the tiles at v1 unless told otherwise.
  tileJson?: () => Response;
};

function globalping({ create, get, tileJson }: Routes) {
  let creates = 0;
  const polls = new Map<string, number>();
  return (call: Call) => {
    if (call.url === TILEJSON_URL && call.method === 'GET') {
      return tileJson?.() ?? tileJsonOf('https://tiles.example.com/v1/{z}/{x}/{y}.mvt');
    }
    if (call.url === GLOBALPING_API && call.method === 'POST') {
      const index = creates++;
      return create?.(call, index) ?? json({ id: `m${index + 1}`, probesCount: 6 }, 202);
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
async function probe(routes: Routes, probeSettings: ProbeConfig | null = config) {
  const fetchMock = mockFetch(globalping(routes));
  const deps = testDeps(scheduled + 3000, () => 0.25);
  const logger = new O11yLogger(o11y);
  const result = await runProbe(probeSettings ?? undefined, o11y, logger, scheduled, deps);
  await logger.flush();
  return { result, deps, ...fetchMock };
}

// Measurement ids hit-1 and miss-1, and the miss measurement's answer: a miss from every continent.
const hitAndMiss = (hitResults: unknown[]): Routes => ({
  create: (_call, index) => json({ id: index === 0 ? 'hit-1' : 'miss-1', probesCount: hitResults.length }, 202),
  get: (id) =>
    measurement(id, 'finished', id === 'hit-1' ? hitResults : everywhere([], { headers: tileHeaders('MISS') })),
});

const NO_VERDICT_FROM = 'Globalping measurement has no verdict from some continents';
// The Worker's read of the TileJSON, a test of its own every run.
const tileJsonRead = (value: number) => ({ name: 'tiles_bunny_probe_errors', labels: { ...TILEJSON_LABELS }, value });

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

  it('takes a cutover origin only when it is an http(s) origin alone', () => {
    const settings = (origin: string) =>
      probeConfig({
        GLOBALPING_TOKEN: 'gp-token',
        BUNNY_PUBLIC_URL: 'https://tiles.example.com',
        BUNNY_PROBE_CUTOVER_ORIGIN: origin,
      });
    expect(settings('https://tiles-test.example.com')).toEqual({
      ...config,
      cutoverOrigin: 'https://tiles-test.example.com',
    });
    expect(settings(' https://Tiles-Test.example.com:443/ ')?.cutoverOrigin).toBe('https://tiles-test.example.com');
    expect(settings('http://tiles-test.example.com:8080')?.cutoverOrigin).toBe('http://tiles-test.example.com:8080');
    for (const origin of [
      '',
      'tiles-test.example.com',
      'ftp://tiles-test.example.com',
      'https://tiles-test.example.com/v1',
      'https://tiles-test.example.com/?v=1',
      'https://user:pass@tiles-test.example.com',
    ]) {
      // The probe still runs; only the public origin is allowed.
      expect(settings(origin)).toEqual(config);
      expect(settings(origin)).not.toHaveProperty('cutoverOrigin');
    }
  });

  it('runs every 10 minutes unless told otherwise, never more often than every 5 nor less than every 14', () => {
    expect(probeIntervalMinutes(undefined)).toBe(10);
    expect(probeIntervalMinutes('')).toBe(10);
    expect(probeIntervalMinutes('often')).toBe(10);
    expect(probeIntervalMinutes('0')).toBe(10);
    expect(probeIntervalMinutes('2.5')).toBe(10);
    expect(probeIntervalMinutes('1')).toBe(5);
    expect(probeIntervalMinutes('14')).toBe(14);
    // The probe alerts' 30-minute windows need two runs: 15 or 30 minutes apart they would hold one at times.
    expect(MAX_PROBE_INTERVAL_MINUTES).toBe(14);
    expect(probeIntervalMinutes('15')).toBe(14);
    expect(probeIntervalMinutes('30')).toBe(14);
    expect(probeDue(scheduled, 10)).toBe(true);
    expect(probeDue(scheduled + 59_000, 10)).toBe(true);
    expect(probeDue(scheduled + 60_000, 10)).toBe(false);
    expect(probeDue(scheduled + 5 * 60_000, 5)).toBe(true);
  });

  it('misses with a z15 position inside the grid', () => {
    expect(missTile(() => 0)).toBe('15/0/0');
    expect(missTile(() => 0.9999999999)).toBe('15/32767/32767');
    expect(missTile(() => 0.5)).toBe('15/16384/16384');
    expect(missTile(() => 0.9999999999, 12)).toBe('12/4095/4095');
  });

  it('takes the hit tile, or the one that holds it or its centre, at any zoom', () => {
    expect(hitTile(2)).toBe(HIT_TILE);
    expect(hitTile(1)).toBe('1/1/0');
    expect(hitTile(0)).toBe('0/0/0');
    // HIT_TILE's centre is at x 2.5, y 1.5 of z2's grid.
    expect(hitTile(3)).toBe('3/5/3');
    expect(hitTile(5)).toBe('5/20/12');
  });

  it("keeps both tiles within the TileJSON's zoom range", () => {
    const half = () => 0.5;
    expect(probeTiles({ minZoom: 0, maxZoom: 15 }, half)).toEqual({ hit: HIT_TILE, miss: '15/16384/16384' });
    expect(probeTiles({ minZoom: 0, maxZoom: 30 }, half)).toEqual({ hit: HIT_TILE, miss: '15/16384/16384' });
    // A staged or clipped build that stops at z12, and one that starts at z4.
    expect(probeTiles({ minZoom: 0, maxZoom: 12 }, half)).toEqual({ hit: HIT_TILE, miss: '12/2048/2048' });
    expect(probeTiles({ minZoom: 4, maxZoom: 15 }, half)).toEqual({ hit: '4/10/6', miss: '15/16384/16384' });
    expect(probeTiles({ minZoom: 0, maxZoom: 1 }, half)).toEqual({ hit: '1/1/0', miss: '1/1/1' });
    expect(probeTiles({ minZoom: 16, maxZoom: 18 }, half)).toEqual({ hit: '16/40960/24576', miss: '16/32768/32768' });
  });
});

describe('tileUrl', () => {
  const origin = new URL('https://tiles.example.com');
  const path = (template: string, tile = HIT_TILE, publicUrl = origin) => tileUrl(publicUrl, template, tile).pathname;

  it("puts the tile in the TileJSON's template, under whatever prefix that names", () => {
    expect(path('https://tiles.example.com/v1/{z}/{x}/{y}.mvt')).toBe('/v1/2/2/1.mvt');
    expect(path('https://tiles.example.com/v1/e2/{z}/{x}/{y}.mvt', '15/1/2')).toBe('/v1/e2/15/1/2.mvt');
    const underPath = new URL('https://cdn.example.com/tiles/');
    expect(path('https://cdn.example.com/tiles/v1/{z}/{x}/{y}.mvt', '0/0/0', underPath)).toBe('/tiles/v1/0/0/0.mvt');
  });

  it("keeps another of the zone's hostnames, as the TileJSON names the test one after a DNS cutover", () => {
    const url = tileUrl(origin, 'https://tiles-test.example.com/v1/e2/{z}/{x}/{y}.mvt', HIT_TILE);
    expect(url.origin).toBe('https://tiles-test.example.com');
    expect(url.pathname).toBe('/v1/e2/2/2/1.mvt');
  });

  it.each([
    'https://tiles.example.com/v1/{z}/{x}/{y}.mvt?v=20261118',
    'https://tiles.example.com/v1/{z}/{x}/{y}.mvt#tile',
    'https://tiles.example.com/v1/{z}/{x}.mvt',
    'https://tiles.example.com/v1/{z}/{z}/{x}/{y}.mvt',
    'ftp://tiles.example.com/v1/{z}/{x}/{y}.mvt',
    '/v1/{z}/{x}/{y}.mvt',
  ])('refuses %s, which is no tile URL to probe', (template) => {
    expect(() => tileUrl(origin, template, HIT_TILE)).toThrow(/Tile template/);
  });

  it("refuses a template outside the public URL's path", () => {
    const underPath = new URL('https://cdn.example.com/tiles');
    expect(() => tileUrl(underPath, 'https://cdn.example.com/other/v1/{z}/{x}/{y}.mvt', HIT_TILE)).toThrow(
      /isn't an http\(s\) tile URL under \/tiles/,
    );
  });
});

describe('runProbe', () => {
  it('measures the hit and miss tiles from the same probe per continent and pushes their TTFBs', async () => {
    const results = (base: number, cache: string) => [
      finished('EU', base, { headers: tileHeaders(cache) }),
      finished('NA', base + 10, { headers: tileHeaders(cache) }),
      finished('SA', base + 20, { headers: tileHeaders(cache) }),
      finished('AS', base + 30, { headers: tileHeaders(cache) }),
      finished('OC', base + 40, { headers: tileHeaders(cache) }),
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
          : measurement(id, 'finished', id === 'hit-1' ? results(20, 'HIT') : results(200, 'MISS')),
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
        { name: 'tiles_bunny_probe_ttfb_ms', labels: { continent: 'EU', kind: 'hit', target: 'hit' }, value: 20 },
        { name: 'tiles_bunny_probe_ttfb_ms', labels: { continent: 'OC', kind: 'hit', target: 'hit' }, value: 60 },
        { name: 'tiles_bunny_probe_ttfb_ms', labels: { continent: 'EU', kind: 'miss', target: 'miss' }, value: 200 },
        { name: 'tiles_bunny_probe_errors', labels: { continent: 'EU', kind: 'hit', target: 'hit' }, value: 0 },
        { name: 'tiles_bunny_probe_errors', labels: { continent: 'AF', kind: 'hit', target: 'hit' }, value: 1 },
        { name: 'tiles_bunny_probe_errors', labels: { continent: 'AF', kind: 'miss', target: 'miss' }, value: 1 },
        // The TileJSON read worked.
        tileJsonRead(0),
        { name: 'tiles_bunny_probe_runs', labels: { result: 'ok' }, value: 1 },
      ]),
    );
    expect(samples.filter(({ name }) => name === 'tiles_bunny_probe_ttfb_ms')).toHaveLength(10);
    expect(samples.filter(({ name }) => name === 'tiles_bunny_probe_errors')).toHaveLength(13);

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
      content_encoding: 'gzip',
      content_type: 'application/x-protobuf',
      counted: true,
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
        measurement(
          id,
          'done-somehow',
          everywhere([{ probe: { continent: 'NA' }, result: { status: 'offline', rawOutput: '' } }]),
        ),
    });
    // One poll of each measurement: the hit one and the miss one, by the ids the fake gives them by default.
    expect(
      calls
        .filter(({ url, method }) => url.startsWith(`${GLOBALPING_API}/`) && method === 'GET')
        .map(({ url }) => url.slice(GLOBALPING_API.length + 1)),
    ).toEqual(['m1', 'm2']);
    expect(JSON.parse(creates(calls)[1].body).locations).toBe('m1');
    expect(metrics(calls).filter(({ labels }) => labels.continent === 'NA')).toEqual([]);
    // Its continent went unmeasured, which the run reports.
    expect(result).toBe('error');
    expect(pushedLogs(calls).filter(({ _msg }) => _msg === NO_VERDICT_FROM)).toEqual([
      expect.objectContaining({ target: 'hit', missing: 'NA' }),
      expect.objectContaining({ target: 'miss', missing: 'NA' }),
    ]);
  });

  it('reports a continent without a verdict, keeping the samples of the others', async () => {
    // Globalping had no probe in AF (its router leaves the location out), and NA's failed at its DNS resolver.
    const results = [
      finished('EU', 20),
      { probe: { continent: 'NA' }, result: { status: 'failed', failureSource: 'resolver', rawOutput: '' } },
      finished('SA', 30),
      finished('AS', 40),
      finished('OC', 50),
    ];
    const { result, calls } = await probe({
      create: (_call, index) => json({ id: index === 0 ? 'hit-1' : 'miss-1', probesCount: 5 }, 202),
      get: (id) => measurement(id, 'finished', results),
    });
    expect(result).toBe('error');
    const samples = metrics(calls);
    expect(samples.filter(({ name }) => name === 'tiles_bunny_probe_ttfb_ms')).toHaveLength(8);
    expect(samples.filter(({ labels }) => labels.continent === 'NA' || labels.continent === 'AF')).toEqual([]);
    expect(samples).toContainEqual({ name: 'tiles_bunny_probe_runs', labels: { result: 'error' }, value: 1 });
    expect(pushedLogs(calls).filter(({ _msg }) => _msg === NO_VERDICT_FROM)).toEqual([
      expect.objectContaining({ level: 'error', target: 'hit', measurement_id: 'hit-1', tests: 5, missing: 'NA,AF' }),
      expect.objectContaining({ level: 'error', target: 'miss', measurement_id: 'miss-1', tests: 5, missing: 'NA,AF' }),
    ]);
  });

  it('counts a non-200 answer as an error, not a TTFB', async () => {
    const { calls } = await probe({
      get: (id) => measurement(id, 'finished', [finished('EU', 30, { statusCode: 404 })]),
    });
    expect(metrics(calls)).toEqual(
      expect.arrayContaining([
        { name: 'tiles_bunny_probe_errors', labels: { continent: 'EU', kind: 'hit', target: 'hit' }, value: 1 },
        { name: 'tiles_bunny_probe_errors', labels: { continent: 'EU', kind: 'hit', target: 'miss' }, value: 1 },
      ]),
    );
    expect(metrics(calls).filter(({ name }) => name === 'tiles_bunny_probe_ttfb_ms')).toEqual([]);
  });

  it('counts a 200 without Content-Encoding: gzip as an error, as no map can decode it', async () => {
    const { result, calls } = await probe(
      hitAndMiss(
        everywhere([
          finished('EU', 30, { headers: { 'cdn-cache': 'HIT', 'content-type': 'application/octet-stream' } }),
          finished('NA', 30, { headers: { ...tileHeaders(), 'content-encoding': 'br' } }),
          finished('AS', 30, { headers: { 'CDN-Cache': 'HIT', 'Content-Encoding': 'GZIP' } }),
        ]),
      ),
    );
    expect(result).toBe('ok');
    const kept = metrics(calls).filter(
      ({ labels }) => labels.kind === 'hit' && ['EU', 'NA', 'AS'].includes(labels.continent),
    );
    expect(kept).toEqual(
      expect.arrayContaining([
        { name: 'tiles_bunny_probe_errors', labels: { continent: 'EU', kind: 'hit', target: 'hit' }, value: 1 },
        { name: 'tiles_bunny_probe_errors', labels: { continent: 'NA', kind: 'hit', target: 'hit' }, value: 1 },
        { name: 'tiles_bunny_probe_errors', labels: { continent: 'AS', kind: 'hit', target: 'hit' }, value: 0 },
        { name: 'tiles_bunny_probe_ttfb_ms', labels: { continent: 'AS', kind: 'hit', target: 'hit' }, value: 30 },
      ]),
    );
    expect(kept.filter(({ name }) => name === 'tiles_bunny_probe_ttfb_ms')).toHaveLength(1);
    const lines = pushedLogs(calls).filter(({ kind, continent }) => kind === 'probe' && continent === 'EU');
    expect(lines[0]).toMatchObject({ status: 200, content_type: 'application/octet-stream' });
    expect(lines[0]).not.toHaveProperty('content_encoding');
  });

  it("labels a test by the edge's CDN-Cache, so a purged hit tile counts as a miss", async () => {
    const { calls } = await probe({
      create: (_call, index) => json({ id: index === 0 ? 'hit-1' : 'miss-1', probesCount: 3 }, 202),
      get: (id) =>
        measurement(
          id,
          'finished',
          id === 'hit-1'
            ? [
                finished('EU', 90, { headers: tileHeaders('MISS') }),
                finished('NA', 15, { headers: tileHeaders('STALE') }),
                finished('AS', 20, { headers: { ...tileHeaders(), 'cdn-cache': undefined } }),
              ]
            : [
                finished('EU', 12, { headers: tileHeaders('HIT') }),
                finished('NA', 95, { headers: tileHeaders('EXPIRED') }),
              ],
        ),
    });
    const ttfb = metrics(calls).filter(({ name }) => name === 'tiles_bunny_probe_ttfb_ms');
    expect(ttfb).toEqual([
      { name: 'tiles_bunny_probe_ttfb_ms', labels: { continent: 'EU', kind: 'miss', target: 'hit' }, value: 90 },
      { name: 'tiles_bunny_probe_ttfb_ms', labels: { continent: 'NA', kind: 'hit', target: 'hit' }, value: 15 },
      // No CDN-Cache (not served by Bunny): the target's kind.
      { name: 'tiles_bunny_probe_ttfb_ms', labels: { continent: 'AS', kind: 'hit', target: 'hit' }, value: 20 },
      { name: 'tiles_bunny_probe_ttfb_ms', labels: { continent: 'EU', kind: 'hit', target: 'miss' }, value: 12 },
      { name: 'tiles_bunny_probe_ttfb_ms', labels: { continent: 'NA', kind: 'miss', target: 'miss' }, value: 95 },
    ]);
    // The log line keeps what was asked for.
    expect(pushedLogs(calls).filter(({ kind }) => kind === 'probe')[0]).toMatchObject({
      target: 'hit',
      cache: 'MISS',
      continent: 'EU',
    });
  });

  it('keeps both targets apart when they report the same kind from one continent, as after a purge', async () => {
    // The purge left the hit tile cold in EU, so both targets answer MISS there; in NA both fail as misses. The other
    // continents both see a hit.
    const cold = { headers: tileHeaders('MISS') };
    const { result, calls } = await probe({
      create: (_call, index) => json({ id: index === 0 ? 'hit-1' : 'miss-1', probesCount: 6 }, 202),
      get: (id) =>
        measurement(
          id,
          'finished',
          everywhere([
            finished('EU', id === 'hit-1' ? 300 : 120, cold),
            finished('NA', 40, { ...cold, statusCode: 404 }),
          ]),
        ),
    });
    expect(result).toBe('ok');
    const samples = pushedSamples(calls);
    // No two samples share a series and a timestamp, so o11y's dedup (the biggest of them) drops none of them.
    const series = samples.map(({ name, labels, timestamp }) => `${name}${JSON.stringify(labels)}@${timestamp}`);
    expect(new Set(series).size).toBe(series.length);
    // The TileJSON read's verdict, under its own labels (continent 'worker'), which the filter below leaves out.
    expect(metrics(calls)).toContainEqual(tileJsonRead(0));
    expect(
      metrics(calls).filter(({ labels }) => labels.continent === undefined || ['EU', 'NA'].includes(labels.continent)),
    ).toEqual([
      { name: 'tiles_bunny_probe_ttfb_ms', labels: { continent: 'EU', kind: 'miss', target: 'hit' }, value: 300 },
      { name: 'tiles_bunny_probe_errors', labels: { continent: 'EU', kind: 'miss', target: 'hit' }, value: 0 },
      { name: 'tiles_bunny_probe_errors', labels: { continent: 'NA', kind: 'miss', target: 'hit' }, value: 1 },
      { name: 'tiles_bunny_probe_ttfb_ms', labels: { continent: 'EU', kind: 'miss', target: 'miss' }, value: 120 },
      { name: 'tiles_bunny_probe_errors', labels: { continent: 'EU', kind: 'miss', target: 'miss' }, value: 0 },
      { name: 'tiles_bunny_probe_errors', labels: { continent: 'NA', kind: 'miss', target: 'miss' }, value: 1 },
      { name: 'tiles_bunny_probe_runs', labels: { result: 'ok' }, value: 1 },
    ]);
  });

  it("doesn't count failures Globalping puts on the probe's resolver or on itself, but does a target's", async () => {
    const failed = (continent: string, failureSource: string | undefined, rawOutput: string) => ({
      probe: { continent },
      result: { status: 'failed', ...(failureSource && { failureSource }), rawOutput },
    });
    const { result, calls } = await probe(
      hitAndMiss([
        failed('EU', 'internal', 'The measurement timed out.'),
        failed('NA', 'resolver', 'The measurement timed out during DNS resolution.'),
        failed('SA', 'target', 'Request timed out while waiting for the first response byte.'),
        failed('AS', undefined, 'incorrect header check'),
        finished('OC', 40),
        finished('AF', 45),
      ]),
    );
    const errors = metrics(calls).filter(
      ({ name, labels }) => name === 'tiles_bunny_probe_errors' && labels.kind === 'hit',
    );
    expect(errors).toEqual([
      { name: 'tiles_bunny_probe_errors', labels: { continent: 'SA', kind: 'hit', target: 'hit' }, value: 1 },
      { name: 'tiles_bunny_probe_errors', labels: { continent: 'AS', kind: 'hit', target: 'hit' }, value: 1 },
      { name: 'tiles_bunny_probe_errors', labels: { continent: 'OC', kind: 'hit', target: 'hit' }, value: 0 },
      { name: 'tiles_bunny_probe_errors', labels: { continent: 'AF', kind: 'hit', target: 'hit' }, value: 0 },
    ]);
    // Uncounted, yet EU and NA went unmeasured: the run says so.
    expect(result).toBe('error');
    expect(pushedLogs(calls).filter(({ _msg }) => _msg === NO_VERDICT_FROM)).toEqual([
      expect.objectContaining({ target: 'hit', missing: 'EU,NA' }),
    ]);
    expect(pushedLogs(calls).filter(({ kind, target }) => kind === 'probe' && target === 'hit')).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ continent: 'EU', counted: false, failure_source: 'internal' }),
        expect.objectContaining({ continent: 'NA', counted: false, failure_source: 'resolver' }),
      ]),
    );
  });

  it('reports an error when no test of a measurement says anything about the tiles', async () => {
    const { result, calls } = await probe({
      get: (id) =>
        measurement(id, 'finished', [
          { probe: { continent: 'EU' }, result: { status: 'failed', failureSource: 'internal', rawOutput: '' } },
          { probe: { continent: 'NA' }, result: { status: 'offline', rawOutput: '' } },
        ]),
    });
    expect(result).toBe('error');
    expect(metrics(calls)).toEqual([
      tileJsonRead(0),
      { name: 'tiles_bunny_probe_runs', labels: { result: 'error' }, value: 1 },
    ]);
    expect(pushedLogs(calls)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          _msg: 'Globalping measurement has no test with a verdict',
          tests: 2,
          level: 'error',
        }),
      ]),
    );
  });

  it('reports an error when Globalping refuses, measuring what it can on its own probes', async () => {
    const { result, calls } = await probe({
      create: (_call, index) =>
        index === 0
          ? json({ error: { type: 'rate_limit_exceeded', message: 'API rate limit exceeded.' } }, 429)
          : json({ id: 'miss-2', probesCount: 6 }, 202),
      get: (id) => measurement(id, 'finished', [finished('EU', 150, { headers: tileHeaders('MISS') })]),
    });
    expect(result).toBe('error');
    expect(JSON.parse(creates(calls)[1].body).locations).toEqual(
      PROBE_CONTINENTS.map((continent) => ({ continent, limit: 1 })),
    );
    expect(metrics(calls)).toEqual([
      tileJsonRead(0),
      { name: 'tiles_bunny_probe_ttfb_ms', labels: { continent: 'EU', kind: 'miss', target: 'miss' }, value: 150 },
      { name: 'tiles_bunny_probe_errors', labels: { continent: 'EU', kind: 'miss', target: 'miss' }, value: 0 },
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

  it('pushes only the TileJSON read and the run when no measurement could be created', async () => {
    const { result, calls } = await probe({ create: () => new Response('down', { status: 503 }) });
    expect(result).toBe('error');
    expect(calls.filter(({ url, method }) => url.startsWith(GLOBALPING_API) && method === 'GET')).toHaveLength(0);
    expect(metrics(calls)).toEqual([
      tileJsonRead(0),
      { name: 'tiles_bunny_probe_runs', labels: { result: 'error' }, value: 1 },
    ]);
  });

  it("gives up at the deadline, not counting tests still in progress (Globalping's delay)", async () => {
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
        { name: 'tiles_bunny_probe_ttfb_ms', labels: { continent: 'EU', kind: 'hit', target: 'hit' }, value: 25 },
        { name: 'tiles_bunny_probe_runs', labels: { result: 'error' }, value: 1 },
      ]),
    );
    expect(metrics(calls).filter(({ labels }) => labels.continent === 'NA')).toEqual([]);
  });

  it('keeps polling through a failed read of the results', async () => {
    const { result, calls } = await probe({
      get: (id, poll) =>
        poll === 0 ? new Response('busy', { status: 429 }) : measurement(id, 'finished', everywhere([])),
    });
    expect(result).toBe('ok');
    expect(metrics(calls)).toEqual(
      expect.arrayContaining([{ name: 'tiles_bunny_probe_runs', labels: { result: 'ok' }, value: 1 }]),
    );
  });

  it('probes the tile URLs the TileJSON advertises, as after a full rewrite to v1/e2', async () => {
    const { result, calls } = await probe({
      tileJson: () => tileJsonOf('https://tiles.example.com/v1/e2/{z}/{x}/{y}.mvt'),
      get: (id) => measurement(id, 'finished', everywhere([])),
    });
    expect(result).toBe('ok');
    const tileJsonCalls = calls.filter(({ url }) => url === TILEJSON_URL);
    expect(tileJsonCalls).toHaveLength(1);
    expect(tileJsonCalls[0].method).toBe('GET');
    const posts = creates(calls);
    expect(calls.indexOf(tileJsonCalls[0])).toBeLessThan(calls.indexOf(posts[0]));
    expect(posts.map(({ body }) => JSON.parse(body).measurementOptions.request.path)).toEqual([
      `/v1/e2/${HIT_TILE}.mvt`,
      '/v1/e2/15/8192/8192.mvt',
    ]);
    expect(pushedLogs(calls).filter(({ kind }) => kind === 'probe')[0]).toMatchObject({
      path: `/v1/e2/${HIT_TILE}.mvt`,
    });
  });

  it("probes the TileJSON's path on the public origin when the TileJSON names the cutover origin", async () => {
    // From a DNS cutover until the next sync, the TileJSON on the public hostname still names the test one.
    const { result, calls } = await probe(
      {
        tileJson: () => tileJsonOf('https://tiles-test.example.com/v1/e2/{z}/{x}/{y}.mvt'),
        get: (id) => measurement(id, 'finished', everywhere([])),
      },
      { ...config, cutoverOrigin: 'https://tiles-test.example.com' },
    );
    expect(result).toBe('ok');
    expect(creates(calls).map(({ body }) => JSON.parse(body))).toEqual([
      expect.objectContaining({
        target: 'tiles.example.com',
        measurementOptions: expect.objectContaining({ request: expect.objectContaining({ path: '/v1/e2/2/2/1.mvt' }) }),
      }),
      expect.objectContaining({
        target: 'tiles.example.com',
        measurementOptions: expect.objectContaining({
          request: expect.objectContaining({ path: '/v1/e2/15/8192/8192.mvt' }),
        }),
      }),
    ]);
    expect(pushedLogs(calls)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          _msg: 'Probe TileJSON names tiles on the cutover origin',
          level: 'warn',
          tile_origin: 'https://tiles-test.example.com',
        }),
      ]),
    );
    expect(metrics(calls)).toContainEqual(tileJsonRead(0));
  });

  it('fails the TileJSON test, measuring nothing, when it names another origin than the cutover one', async () => {
    // Clients would load the tiles from there, even if the same paths work on the public origin.
    const { result, calls } = await probe(
      { tileJson: () => tileJsonOf('https://tiles-old.example.com/v1/{z}/{x}/{y}.mvt') },
      { ...config, cutoverOrigin: 'https://tiles-test.example.com' },
    );
    expect(result).toBe('error');
    expect(calls.filter(({ url }) => url.startsWith(GLOBALPING_API))).toEqual([]);
    expect(metrics(calls)).toEqual([
      tileJsonRead(1),
      { name: 'tiles_bunny_probe_runs', labels: { result: 'error' }, value: 1 },
    ]);
    expect(pushedLogs(calls)).toEqual([
      expect.objectContaining({
        _msg: 'Probe tile URLs unknown',
        level: 'error',
        error: expect.stringContaining('https://tiles-old.example.com'),
      }),
    ]);
  });

  it("probes tiles within a clipped build's zoom range", async () => {
    // A staged build that stops at z12 and starts at z4: no z15 file, nor a z2 one, for the probe to find.
    const { result, calls } = await probe({
      tileJson: () => tileJsonOf('https://tiles.example.com/v1/{z}/{x}/{y}.mvt', { minzoom: 4, maxzoom: 12 }),
      get: (id) => measurement(id, 'finished', everywhere([])),
    });
    expect(result).toBe('ok');
    expect(creates(calls).map(({ body }) => JSON.parse(body).measurementOptions.request.path)).toEqual([
      '/v1/4/10/6.mvt',
      '/v1/12/1024/1024.mvt',
    ]);
  });

  it('measures the planet build at z2 and z15 when its TileJSON gives no zoom range', async () => {
    const { calls } = await probe({
      tileJson: () => json({ tilejson: '3.0.0', tiles: ['https://tiles.example.com/v1/{z}/{x}/{y}.mvt'] }),
      get: (id) => measurement(id, 'finished', everywhere([])),
    });
    expect(creates(calls).map(({ body }) => JSON.parse(body).measurementOptions.request.path)).toEqual([
      `/v1/${HIT_TILE}.mvt`,
      '/v1/15/8192/8192.mvt',
    ]);
  });

  it.each([
    ['is not found', () => new Response('not found', { status: 404 })],
    ['answers 503, as in a CDN outage', () => new Response('Domain suspended or not configured', { status: 503 })],
    [
      'times out',
      (): Response => {
        throw new DOMException('The operation was aborted due to timeout', 'TimeoutError');
      },
    ],
    ['is no JSON', () => new Response('<html>', { status: 200 })],
    ['has no tile template', () => json({ tilejson: '3.0.0', tiles: [] })],
    ['names tiles with a query', () => tileJsonOf('https://tiles.example.com/v1/{z}/{x}/{y}.mvt?v=20261118')],
    // Clients follow it there; without a cutover origin configured only the public one is allowed.
    ['names tiles on another origin', () => tileJsonOf('https://tiles-test.example.com/v1/{z}/{x}/{y}.mvt')],
    [
      'has a minzoom above its maxzoom',
      () => tileJsonOf('https://tiles.example.com/v1/{z}/{x}/{y}.mvt', { minzoom: 9, maxzoom: 4 }),
    ],
    [
      'has a maxzoom that is no zoom',
      () => tileJsonOf('https://tiles.example.com/v1/{z}/{x}/{y}.mvt', { maxzoom: 14.5 }),
    ],
    ['has a maxzoom beyond 30', () => tileJsonOf('https://tiles.example.com/v1/{z}/{x}/{y}.mvt', { maxzoom: 31 })],
    ['has a minzoom that is text', () => tileJsonOf('https://tiles.example.com/v1/{z}/{x}/{y}.mvt', { minzoom: '0' })],
  ])(
    'pushes a failed TileJSON test the critical alert counts, measuring no tile, when the TileJSON %s',
    async (_, tileJson) => {
      const { result, calls } = await probe({ tileJson });
      expect(result).toBe('error');
      // Not the old prefix instead.
      expect(calls.filter(({ url }) => url.startsWith(GLOBALPING_API))).toEqual([]);
      // A test with a verdict, an error, as geoshenanigans-tiles-bunny-probe-errors sums them (by continent and kind).
      expect(metrics(calls)).toEqual([
        {
          name: 'tiles_bunny_probe_errors',
          labels: { continent: 'worker', kind: 'tilejson', target: 'tilejson' },
          value: 1,
        },
        { name: 'tiles_bunny_probe_runs', labels: { result: 'error' }, value: 1 },
      ]);
      expect(new Set(pushedSamples(calls).map(({ timestamp }) => timestamp))).toEqual(new Set([scheduled]));
      expect(pushedLogs(calls)).toEqual([
        expect.objectContaining({ _msg: 'Probe tile URLs unknown', level: 'error', job: 'bunny_probe' }),
      ]);
    },
  );

  it("doesn't follow a redirect from Globalping, which would carry the token along", async () => {
    const elsewhere = 'https://api.elsewhere.example/v1/measurements';
    const { result, calls } = await probe({
      create: () => new Response(null, { status: 307, headers: { Location: elsewhere } }),
    });
    expect(result).toBe('error');
    const posts = creates(calls);
    expect(posts).toHaveLength(2);
    for (const post of posts) {
      expect(post.redirect).toBe('manual');
    }
    expect(calls.filter(({ url }) => url.startsWith('https://api.elsewhere.example/'))).toEqual([]);
    expect(pushedLogs(calls)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ _msg: 'Globalping measurement not created', error: expect.stringContaining('307') }),
      ]),
    );
  });

  it('is skipped, calling nothing but o11y, without its settings', async () => {
    const { result, calls } = await probe({}, null);
    expect(result).toBe('skipped');
    expect(calls.filter(({ url }) => url.startsWith(GLOBALPING_API))).toHaveLength(0);
    expect(calls.filter(({ url }) => url === TILEJSON_URL)).toHaveLength(0);
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
    const routes = globalping({ get: (id) => measurement(id, 'finished', everywhere([finished('EU', 10)])) });
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
