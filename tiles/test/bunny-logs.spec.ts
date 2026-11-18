import { logReaderConfig } from '../src/bunny-logs/config';
import {
  classifyPath,
  LAG_MINUTES,
  LOGGING_API,
  LogRow,
  logWindow,
  MAX_LOG_LINES,
  MAX_PAGES,
  PAGE_SIZE,
  READ_DEADLINE_MS,
  runLogReader,
} from '../src/bunny-logs/log-reader';
import { exposition } from '../src/bunny-logs/samples';
import { O11yLogger } from '../src/o11y';
import { Call, identity, json, mockFetch, o11y, pushedLogs, pushedSamples, testDeps } from './bunny-helpers';

const config = { apiKey: 'account-key', pullZoneId: '12345' };
// Scheduled for 12:05 (cron runs start a few seconds late): reads 12:02-12:03.
const scheduled = Date.UTC(2026, 10, 18, 12, 5, 0);
const windowStart = Date.UTC(2026, 10, 18, 12, 2, 0);
const windowEnd = Date.UTC(2026, 10, 18, 12, 3, 0);
const runAt = scheduled + 4000;

const row = (fields: LogRow & { [key: string]: unknown } = {}) => ({
  timestamp: '2026-11-18T12:02:10.5+00:00',
  pullZoneId: 12345,
  requestId: 'f1e2d3c4',
  cacheStatus: 'HIT',
  statusCode: 200,
  bytesSent: 1000,
  remoteIp: '203.0.113.7',
  countryCode: 'GB',
  edgeLocation: 'UK',
  scheme: 'https',
  host: 'tiles.example.com',
  path: '/v1/5/16/10.mvt?v=20261118',
  url: 'https://tiles.example.com/v1/5/16/10.mvt?v=20261118',
  userAgent: 'Mozilla/5.0 (X11; Linux x86_64)',
  referer: 'https://photos.example.co.uk/map?album=private-name',
  ja4Fingerprint: 't13d1714h1_ab0a1bf427ad_5ae85263eb3b',
  asn: 64500,
  asnOrganization: 'Example Networks Ltd.',
  ...fields,
});

const page = (data: unknown[], hasMore = false) => json({ data, pagination: { hasMore }, query: {} });

const isWindowQuery = (call: Call) =>
  call.url.startsWith(`${LOGGING_API}/12345/logs`) && new URL(call.url).searchParams.get('order') === 'asc';
const isLagQuery = (call: Call) =>
  call.url.startsWith(`${LOGGING_API}/12345/logs`) && new URL(call.url).searchParams.get('order') === 'desc';

/** Answers the window's pages in turn and the lag query with `newest` (no row when undefined). */
function loggingApi(pages: (() => Response)[], newest?: string) {
  let next = 0;
  return (call: Call) => {
    if (isLagQuery(call)) {
      return page(newest ? [row({ timestamp: newest })] : []);
    }
    if (isWindowQuery(call)) {
      return pages[Math.min(next++, pages.length - 1)]();
    }
    return undefined;
  };
}

async function run(
  pages: (() => Response)[],
  {
    newest,
    sampleRate = '0',
    readerConfig = config,
  }: { newest?: string; sampleRate?: string; readerConfig?: typeof config | null } = {},
) {
  const fetchMock = mockFetch(loggingApi(pages, newest));
  const deps = testDeps(runAt);
  const logger = new O11yLogger(o11y);
  const result = await runLogReader(readerConfig ?? undefined, o11y, logger, scheduled, sampleRate, deps);
  await logger.flush();
  return { result, deps, ...fetchMock };
}

const sampleValue = (samples: ReturnType<typeof pushedSamples>, name: string, labels: object = {}) =>
  samples.find((sample) => sample.name === name && JSON.stringify(sample.labels) === JSON.stringify(labels))?.value;

beforeEach(() => {
  vi.spyOn(console, 'info').mockImplementation(() => {});
  vi.spyOn(console, 'warn').mockImplementation(() => {});
  vi.spyOn(console, 'error').mockImplementation(() => {});
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe('logWindow', () => {
  it(`is the closed minute ${LAG_MINUTES} minutes before the scheduled one`, () => {
    expect(logWindow(scheduled)).toEqual({ start: windowStart, end: windowEnd });
    expect(logWindow(scheduled + 59_999)).toEqual({ start: windowStart, end: windowEnd });
    expect(logWindow(scheduled + 60_000).start).toBe(windowEnd);
  });
});

describe('classifyPath', () => {
  it('tells tiles (with their zoom), the TileJSON and styles from anything else', () => {
    expect(classifyPath('/v1/15/17000/11000.mvt')).toEqual({ kind: 'tile', zoom: '15' });
    expect(classifyPath('/v1/0/0/0.mvt?v=20261118')).toEqual({ kind: 'tile', zoom: '0' });
    expect(classifyPath('/v1/16/1/1.mvt')).toEqual({ kind: 'tile', zoom: 'other' });
    expect(classifyPath('/v1/05/1/1.mvt')).toEqual({ kind: 'tile', zoom: 'other' });
    expect(classifyPath('/v1.json')).toEqual({ kind: 'tilejson' });
    expect(classifyPath('/v1.json?x=1')).toEqual({ kind: 'tilejson' });
    expect(classifyPath('/v1/style/light.json')).toEqual({ kind: 'style' });
    expect(classifyPath('/v1')).toEqual({ kind: 'other' });
    expect(classifyPath('/favicon.ico')).toEqual({ kind: 'other' });
    expect(classifyPath('/v1/1/1/1.png')).toEqual({ kind: 'other' });
    expect(classifyPath(undefined)).toEqual({ kind: 'other' });
  });
});

describe('logReaderConfig', () => {
  it('needs the account key and a numeric pull zone id', () => {
    expect(logReaderConfig({})).toBeUndefined();
    expect(logReaderConfig({ BUNNY_API_KEY: 'key' })).toBeUndefined();
    expect(logReaderConfig({ BUNNY_PULL_ZONE_ID: '12345' })).toBeUndefined();
    expect(logReaderConfig({ BUNNY_API_KEY: ' ', BUNNY_PULL_ZONE_ID: '12345' })).toBeUndefined();
    expect(logReaderConfig({ BUNNY_API_KEY: 'key', BUNNY_PULL_ZONE_ID: 'zone' })).toBeUndefined();
    expect(logReaderConfig({ BUNNY_API_KEY: ' key ', BUNNY_PULL_ZONE_ID: ' 12345 ' })).toEqual({
      apiKey: 'key',
      pullZoneId: '12345',
    });
  });
});

describe('exposition', () => {
  it('stamps every sample with its time and the identity labels, which no label can shadow', () => {
    expect(
      exposition(
        [
          { name: 'tiles_bunny_requests', labels: { kind: 'tile', region: 'UK', odd: 'a"b\\c\nd' }, value: 3 },
          { name: 'tiles_bunny_log_rows', labels: {}, value: NaN },
        ],
        identity,
        1794140000000,
      ),
    ).toBe(
      'tiles_bunny_requests{kind="tile",region="world",odd="a\\"b\\\\c\\nd",project="geoshenanigans",cluster="tiles",' +
        'env="prod",provider="cloudflare"} 3 1794140000000\n',
    );
  });
});

describe('runLogReader', () => {
  it("reads the window's pages with the account key and pushes one batch of counts at the minute's end", async () => {
    const first = [
      row(),
      row({ requestId: 'b', cacheStatus: 'MISS', bytesSent: 2000, path: '/v1/15/17000/11000.mvt' }),
    ];
    const second = [
      row({ requestId: 'c', path: '/v1.json', statusCode: 304, bytesSent: 0, edgeLocation: 'NY', countryCode: 'US' }),
      row({ requestId: 'd', path: '/v1/style/dark.json', cacheStatus: 'REVALIDATED' }),
      row({ requestId: 'e', path: '/wp-login.php', statusCode: 404, edgeLocation: 'weird!', countryCode: '--' }),
    ];
    const { result, calls } = await run([() => page(first, true), () => page(second)], {
      newest: new Date(runAt - 2500).toISOString(),
    });
    expect(result).toBe('ok');

    const windowCalls = calls.filter(isWindowQuery);
    expect(windowCalls.map(({ url }) => Object.fromEntries(new URL(url).searchParams))).toEqual([
      { from: '2026-11-18T12:02:00Z', to: '2026-11-18T12:03:00Z', limit: String(PAGE_SIZE), offset: '0', order: 'asc' },
      { from: '2026-11-18T12:02:00Z', to: '2026-11-18T12:03:00Z', limit: String(PAGE_SIZE), offset: '2', order: 'asc' },
    ]);
    for (const call of calls.filter((call) => call.url.startsWith(LOGGING_API))) {
      expect(call.headers.get('AccessKey')).toBe('account-key');
    }
    const lag = calls.find(isLagQuery)!;
    expect(Object.fromEntries(new URL(lag.url).searchParams)).toMatchObject({
      from: '2026-11-18T11:55:04Z',
      limit: '1',
      order: 'desc',
    });

    const pushes = calls.filter(({ url }) => url.includes('/insert/0/prometheus/'));
    expect(pushes).toHaveLength(1);
    expect(new Headers(pushes[0].headers).get('Authorization')).toBe('Bearer secret');
    const samples = pushedSamples(calls);
    expect(new Set(samples.map(({ timestamp }) => timestamp))).toEqual(new Set([windowEnd]));
    expect(samples.map(({ name, labels, value }) => ({ name, labels, value }))).toEqual(
      expect.arrayContaining([
        {
          name: 'tiles_bunny_requests',
          labels: { kind: 'tile', status: '200', cache: 'HIT', edge_location: 'UK' },
          value: 1,
        },
        {
          name: 'tiles_bunny_requests',
          labels: { kind: 'tile', status: '200', cache: 'MISS', edge_location: 'UK' },
          value: 1,
        },
        {
          name: 'tiles_bunny_requests',
          labels: { kind: 'tilejson', status: '304', cache: 'HIT', edge_location: 'NY' },
          value: 1,
        },
        {
          name: 'tiles_bunny_requests',
          labels: { kind: 'style', status: '200', cache: 'REVALIDATED', edge_location: 'UK' },
          value: 1,
        },
        {
          name: 'tiles_bunny_requests',
          labels: { kind: 'other', status: '404', cache: 'HIT', edge_location: 'other' },
          value: 1,
        },
        { name: 'tiles_bunny_bytes', labels: { kind: 'tile', edge_location: 'UK' }, value: 3000 },
        { name: 'tiles_bunny_bytes', labels: { kind: 'tilejson', edge_location: 'NY' }, value: 0 },
        { name: 'tiles_bunny_tile_requests', labels: { zoom: '5', cache: 'HIT' }, value: 1 },
        { name: 'tiles_bunny_tile_requests', labels: { zoom: '15', cache: 'MISS' }, value: 1 },
        { name: 'tiles_bunny_country_requests', labels: { country: 'GB' }, value: 3 },
        { name: 'tiles_bunny_country_requests', labels: { country: 'US' }, value: 1 },
        { name: 'tiles_bunny_country_requests', labels: { country: 'other' }, value: 1 },
        { name: 'tiles_bunny_log_rows', labels: {}, value: 5 },
        { name: 'tiles_bunny_log_lag_seconds', labels: {}, value: 2.5 },
        { name: 'tiles_bunny_log_reader_runs', labels: { result: 'ok' }, value: 1 },
      ]),
    );
    expect(samples.filter(({ name }) => name === 'tiles_bunny_requests')).toHaveLength(5);
  });

  it('pushes a zero row count for an empty minute, and no lag without a recent row', async () => {
    const { result, calls } = await run([() => page([])]);
    expect(result).toBe('ok');
    expect(calls.filter(isWindowQuery)).toHaveLength(1);
    expect(pushedSamples(calls).map(({ name, labels, value }) => ({ name, labels, value }))).toEqual([
      { name: 'tiles_bunny_log_rows', labels: {}, value: 0 },
      { name: 'tiles_bunny_log_reader_runs', labels: { result: 'ok' }, value: 1 },
    ]);
  });

  it('is skipped, calling nothing but o11y, without its settings', async () => {
    const { result, calls } = await run([() => page([row()])], { readerConfig: null });
    expect(result).toBe('skipped');
    expect(calls.filter(({ url }) => url.startsWith(LOGGING_API))).toHaveLength(0);
    expect(pushedSamples(calls)).toEqual([
      { name: 'tiles_bunny_log_reader_runs', labels: { result: 'skipped' }, value: 1, timestamp: windowEnd },
    ]);
  });

  it("doesn't retry a refused key, and pushes no counts for a failed read", async () => {
    const { result, calls, deps } = await run([() => json({ error: { code: 'unauthorized' } }, 401)]);
    expect(result).toBe('error');
    expect(calls.filter(isWindowQuery)).toHaveLength(1);
    expect(deps.sleeps).toEqual([]);
    expect(pushedSamples(calls).map(({ name, labels }) => ({ name, labels }))).toEqual([
      { name: 'tiles_bunny_log_reader_runs', labels: { result: 'error' } },
    ]);
    expect(pushedLogs(calls)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          _msg: 'Bunny log reader failed',
          level: 'error',
          job: 'bunny_logs',
          error: expect.stringContaining('401'),
        }),
      ]),
    );
    expect(JSON.stringify(pushedLogs(calls))).not.toContain('account-key');
  });

  it('retries rate limits, server errors and unreadable bodies, then gives up', async () => {
    const recovered = await run([
      () => json({ error: 'slow down' }, 429),
      () => new Response('not json', { status: 200 }),
      () => page([row()]),
    ]);
    expect(recovered.result).toBe('ok');
    expect(recovered.deps.sleeps).toEqual([1000, 2000]);
    expect(sampleValue(pushedSamples(recovered.calls), 'tiles_bunny_log_rows')).toBe(1);

    const failed = await run([() => new Response('bad gateway', { status: 502 })]);
    expect(failed.result).toBe('error');
    expect(failed.calls.filter(isWindowQuery)).toHaveLength(3);
    expect(sampleValue(pushedSamples(failed.calls), 'tiles_bunny_log_rows')).toBeUndefined();
  });

  it('fails a read that loses its connection midway rather than push part of the minute', async () => {
    const { result, calls } = await run([
      () => page([row()], true),
      () => {
        throw new TypeError('Network connection lost.');
      },
    ]);
    expect(result).toBe('error');
    expect(calls.filter(isWindowQuery)).toHaveLength(4);
    expect(sampleValue(pushedSamples(calls), 'tiles_bunny_log_rows')).toBeUndefined();
  });

  it(`stops after ${MAX_PAGES} pages and reports the run as an error, with the counts so far`, async () => {
    const { result, calls } = await run([() => page([row(), row()], true)]);
    expect(result).toBe('error');
    expect(calls.filter(isWindowQuery)).toHaveLength(MAX_PAGES);
    const samples = pushedSamples(calls);
    expect(sampleValue(samples, 'tiles_bunny_log_rows')).toBe(2 * MAX_PAGES);
    expect(sampleValue(samples, 'tiles_bunny_log_reader_runs', { result: 'error' })).toBe(1);
    expect(pushedLogs(calls)).toEqual(
      expect.arrayContaining([expect.objectContaining({ _msg: expect.stringContaining('truncated'), level: 'error' })]),
    );
  });

  it("samples request lines at the request's own time, with the site but no IP, fingerprint or page", async () => {
    const rows = [
      row(),
      row({ requestId: 'b', referer: '-', countryCode: 'DE', timestamp: '2026-11-18T12:02:59.9+00:00' }),
    ];
    const { calls } = await run([() => page(rows)], { sampleRate: '1' });
    const lines = pushedLogs(calls).filter(({ kind }) => kind === 'request');
    expect(lines).toEqual([
      {
        _time: '2026-11-18T12:02:10.500Z',
        _msg: 'Request',
        level: 'info',
        kind: 'request',
        cdn: 'bunny',
        request_type: 'tile',
        z: '5',
        status: 200,
        cache: 'HIT',
        edge_location: 'UK',
        country: 'GB',
        as_org: 'Example Networks Ltd.',
        asn: 64500,
        apex_domain: 'example.co.uk',
        referer_host: 'photos.example.co.uk',
        user_agent: 'Mozilla/5.0 (X11; Linux x86_64)',
        bytes: 1000,
        host: 'tiles.example.com',
        path: '/v1/5/16/10.mvt',
        request_id: 'f1e2d3c4',
        ...identity,
      },
      expect.objectContaining({ _time: '2026-11-18T12:02:59.900Z', country: 'DE', request_id: 'b' }),
    ]);
    expect(lines[1]).not.toHaveProperty('apex_domain');
    const sent = JSON.stringify(lines);
    for (const secret of ['203.0.113.7', 'remoteIp', 't13d1714h1', 'private-name', '20261118']) {
      expect(sent).not.toContain(secret);
    }
  });

  it('samples the same rows on every run, and at most a bounded number a minute', async () => {
    const rows = Array.from({ length: 2000 }, (_, i) => row({ requestId: `request-${i}` }));
    const sampled = async () => {
      const { calls } = await run([() => page(rows)], { sampleRate: '0.1' });
      return pushedLogs(calls)
        .filter(({ kind }) => kind === 'request')
        .map(({ request_id }) => request_id);
    };
    const first = await sampled();
    expect(first.length).toBeGreaterThan(100);
    expect(first.length).toBeLessThan(300);
    expect(await sampled()).toEqual(first);

    const { calls } = await run([() => page(rows)], { sampleRate: '1' });
    expect(pushedLogs(calls).filter(({ kind }) => kind === 'request')).toHaveLength(MAX_LOG_LINES);
  });

  it('gives up on a slow API once the read deadline has passed', async () => {
    const fetchMock = mockFetch((call) => {
      if (isWindowQuery(call)) {
        deps.advance(20_000);
        return page([row()], true);
      }
      return isLagQuery(call) ? page([]) : undefined;
    });
    const deps = testDeps(runAt);
    const logger = new O11yLogger(o11y);
    expect(await runLogReader(config, o11y, logger, scheduled, '0', deps)).toBe('error');
    await logger.flush();
    expect(fetchMock.calls.filter(isWindowQuery)).toHaveLength(Math.ceil(READ_DEADLINE_MS / 20_000));
    expect(pushedLogs(fetchMock.calls)).toEqual(
      expect.arrayContaining([expect.objectContaining({ error: expect.stringContaining('not read within 50s') })]),
    );
  });

  it('still pushes the counts when the lag query fails', async () => {
    const fetchMock = mockFetch((call) => {
      if (isLagQuery(call)) {
        return json({ error: 'nope' }, 400);
      }
      return isWindowQuery(call) ? page([row()]) : undefined;
    });
    const logger = new O11yLogger(o11y);
    expect(await runLogReader(config, o11y, logger, scheduled, '0', testDeps(runAt))).toBe('ok');
    await logger.flush();
    const samples = pushedSamples(fetchMock.calls);
    expect(sampleValue(samples, 'tiles_bunny_log_rows')).toBe(1);
    expect(sampleValue(samples, 'tiles_bunny_log_lag_seconds')).toBeUndefined();
    expect(pushedLogs(fetchMock.calls)).toEqual(
      expect.arrayContaining([expect.objectContaining({ _msg: 'Bunny log lag unknown', level: 'warn' })]),
    );
  });
});
