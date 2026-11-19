import { logReaderConfig } from '../src/bunny-logs/config';
import {
  classifyPath,
  LAG_MINUTES,
  LOGGING_API,
  LogRow,
  logWindow,
  MAX_LOG_LINES,
  MAX_PAGES,
  Pacer,
  PAGE_SIZE,
  RATE_LIMIT_WAIT_MS,
  READ_DEADLINE_MS,
  REQUEST_SPACING_MS,
  REREAD_LAG_MINUTES,
  runLogReader,
} from '../src/bunny-logs/log-reader';
import { exposition } from '../src/bunny-logs/samples';
import { O11yLogger } from '../src/o11y';
import { Call, identity, json, mockFetch, o11y, pushedLogs, pushedSamples, testDeps } from './bunny-helpers';

const config = { apiKey: 'account-key', pullZoneId: '12345' };
// Scheduled for 12:05 (cron runs start a few seconds late): reads 12:02-12:03 and re-reads 12:01-12:02.
const scheduled = Date.UTC(2026, 10, 18, 12, 5, 0);
const windowStart = Date.UTC(2026, 10, 18, 12, 2, 0);
const windowEnd = Date.UTC(2026, 10, 18, 12, 3, 0);
const rereadStart = Date.UTC(2026, 10, 18, 12, 1, 0);
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

const iso = (ms: number) => new Date(ms).toISOString().replace('.000Z', 'Z');
const isLogQuery = (call: Call) => call.url.startsWith(`${LOGGING_API}/12345/logs`);
/** A page of a minute's rows: of the minute starting at `start` when given (the newest minute read by default). */
const isWindowQuery = (call: Call, start: number | null = windowStart) =>
  isLogQuery(call) &&
  new URL(call.url).searchParams.get('order') === 'asc' &&
  (start === null || new URL(call.url).searchParams.get('from') === iso(start));
const isRereadQuery = (call: Call) => isWindowQuery(call, rereadStart);
const isLagQuery = (call: Call) => isLogQuery(call) && new URL(call.url).searchParams.get('order') === 'desc';

type Pages = (() => Response)[];

/** Answers each minute's pages in turn (from `pages` by the minute's start) and the lag query with `newest`. */
function loggingApi(pages: { [start: string]: Pages }, newest?: string) {
  const next = new Map<string, number>();
  return (call: Call) => {
    if (isLagQuery(call)) {
      return page(newest ? [row({ timestamp: newest })] : []);
    }
    if (isWindowQuery(call, null)) {
      const from = new URL(call.url).searchParams.get('from')!;
      const minute = pages[from] ?? [() => page([])];
      const index = next.get(from) ?? 0;
      next.set(from, index + 1);
      return minute[Math.min(index, minute.length - 1)]();
    }
    return undefined;
  };
}

/** One run: `pages` answer the newest minute, `reread` the minute before (an empty minute by default). */
async function run(
  pages: Pages,
  {
    newest,
    sampleRate = '0',
    readerConfig = config,
    reread = [() => page([])],
    at = scheduled,
  }: { newest?: string; sampleRate?: string; readerConfig?: typeof config | null; reread?: Pages; at?: number } = {},
) {
  const fetchMock = mockFetch(
    loggingApi({ [iso(logWindow(at).start)]: pages, [iso(logWindow(at, REREAD_LAG_MINUTES).start)]: reread }, newest),
  );
  const deps = testDeps(at + 4000);
  const logger = new O11yLogger(o11y);
  const result = await runLogReader(readerConfig ?? undefined, o11y, logger, at, sampleRate, deps);
  await logger.flush();
  return { result, deps, ...fetchMock };
}

const sampleValue = (
  samples: ReturnType<typeof pushedSamples>,
  name: string,
  labels: object = {},
  timestamp = windowEnd,
) =>
  samples.find(
    (sample) =>
      sample.name === name &&
      sample.timestamp === timestamp &&
      JSON.stringify(sample.labels) === JSON.stringify(labels),
  )?.value;

/** What VictoriaMetrics keeps of samples a series has at one timestamp: the biggest. */
function dedup(samples: ReturnType<typeof pushedSamples>) {
  const kept = new Map<string, (typeof samples)[number]>();
  for (const sample of samples) {
    const key = `${sample.name}${JSON.stringify(sample.labels)}@${sample.timestamp}`;
    if (!kept.has(key) || kept.get(key)!.value < sample.value) {
      kept.set(key, sample);
    }
  }
  return [...kept.values()];
}

/** The pause before each retry (the pacer's shorter waits left out). */
const retryWaits = (sleeps: number[]) => sleeps.filter((ms) => ms > REQUEST_SPACING_MS);

beforeEach(() => {
  vi.spyOn(console, 'info').mockImplementation(() => {});
  vi.spyOn(console, 'warn').mockImplementation(() => {});
  vi.spyOn(console, 'error').mockImplementation(() => {});
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe('logWindow', () => {
  it(`is the closed minute ${LAG_MINUTES} minutes before the scheduled one, or the one before for the re-read`, () => {
    expect(logWindow(scheduled)).toEqual({ start: windowStart, end: windowEnd });
    expect(logWindow(scheduled + 59_999)).toEqual({ start: windowStart, end: windowEnd });
    expect(logWindow(scheduled + 60_000).start).toBe(windowEnd);
    expect(logWindow(scheduled, REREAD_LAG_MINUTES)).toEqual({ start: rereadStart, end: windowStart });
    // The next run re-reads what this one read first.
    expect(logWindow(scheduled + 60_000, REREAD_LAG_MINUTES)).toEqual(logWindow(scheduled));
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

    const windowCalls = calls.filter((call) => isWindowQuery(call));
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
    const all = pushedSamples(calls);
    // The re-read minute (empty here) at its own end.
    expect(all.filter(({ timestamp }) => timestamp === windowStart)).toEqual([
      { name: 'tiles_bunny_log_rows', labels: {}, value: 0, timestamp: windowStart },
    ]);
    const samples = all.filter(({ timestamp }) => timestamp === windowEnd);
    expect(samples).toHaveLength(all.length - 1);
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
        { name: 'tiles_bunny_log_reader_runs', labels: { result: 'ok' }, value: 1 },
      ]),
    );
    expect(samples.filter(({ name }) => name === 'tiles_bunny_requests')).toHaveLength(5);
    // 2.5s behind when asked; the test clock also counts the pacer's wait for the re-read's page.
    const lagSeconds = sampleValue(samples, 'tiles_bunny_log_lag_seconds');
    expect(lagSeconds).toBeGreaterThanOrEqual(2.5);
    expect(lagSeconds).toBeLessThanOrEqual(2.5 + (2 * REQUEST_SPACING_MS) / 1000);
  });

  it('pushes a zero row count for an empty minute, and no lag without a recent row', async () => {
    const { result, calls } = await run([() => page([])]);
    expect(result).toBe('ok');
    expect(calls.filter((call) => isWindowQuery(call))).toHaveLength(1);
    expect(calls.filter(isRereadQuery)).toHaveLength(1);
    expect(pushedSamples(calls)).toEqual([
      { name: 'tiles_bunny_log_rows', labels: {}, value: 0, timestamp: windowStart },
      { name: 'tiles_bunny_log_rows', labels: {}, value: 0, timestamp: windowEnd },
      { name: 'tiles_bunny_log_reader_runs', labels: { result: 'ok' }, value: 1, timestamp: windowEnd },
    ]);
  });

  it("re-reads the minute the previous run read first and pushes it at that minute's end", async () => {
    const { result, calls } = await run([() => page([row()])], {
      reread: [() => page([row({ requestId: 'x' }), row({ requestId: 'y' }), row({ requestId: 'z' })])],
    });
    expect(result).toBe('ok');
    const reread = calls.filter(isRereadQuery);
    expect(reread.map(({ url }) => Object.fromEntries(new URL(url).searchParams))).toEqual([
      { from: '2026-11-18T12:01:00Z', to: '2026-11-18T12:02:00Z', limit: String(PAGE_SIZE), offset: '0', order: 'asc' },
    ]);
    // The re-read goes first: it is its minute's last chance.
    expect(calls.findIndex(isRereadQuery)).toBeLessThan(calls.findIndex((call) => isWindowQuery(call)));
    const samples = pushedSamples(calls);
    expect(sampleValue(samples, 'tiles_bunny_log_rows', {}, windowStart)).toBe(3);
    expect(sampleValue(samples, 'tiles_bunny_country_requests', { country: 'GB' }, windowStart)).toBe(3);
    expect(sampleValue(samples, 'tiles_bunny_log_rows', {}, windowEnd)).toBe(1);
    // One run, one result, at the newest minute's end only.
    expect(samples.filter(({ name }) => name === 'tiles_bunny_log_reader_runs')).toEqual([
      { name: 'tiles_bunny_log_reader_runs', labels: { result: 'ok' }, value: 1, timestamp: windowEnd },
    ]);
  });

  it("replaces a minute's short or failed first read with its re-read, never counting it twice", async () => {
    // 12:02's logs arrive late: its first read (run at 12:05) sees 1 row, its re-read (run at 12:06) all 3.
    const late = [row({ requestId: 'a' }), row({ requestId: 'b' }), row({ requestId: 'c' })];
    const firstRun = await run([() => page(late.slice(0, 1))]);
    const secondRun = await run([() => page([])], { at: scheduled + 60_000, reread: [() => page(late)] });
    const kept = dedup([...pushedSamples(firstRun.calls), ...pushedSamples(secondRun.calls)]);
    expect(sampleValue(kept, 'tiles_bunny_log_rows')).toBe(3);
    expect(
      sampleValue(kept, 'tiles_bunny_requests', { kind: 'tile', status: '200', cache: 'HIT', edge_location: 'UK' }),
    ).toBe(3);
    expect(sampleValue(kept, 'tiles_bunny_bytes', { kind: 'tile', edge_location: 'UK' })).toBe(3000);

    // A first read that fails (here a refused key) is filled in by the re-read.
    const failedRun = await run([() => json({ error: 'nope' }, 401)]);
    expect(failedRun.result).toBe('error');
    expect(sampleValue(pushedSamples(failedRun.calls), 'tiles_bunny_log_rows')).toBeUndefined();
    const kept2 = dedup([...pushedSamples(failedRun.calls), ...pushedSamples(secondRun.calls)]);
    expect(sampleValue(kept2, 'tiles_bunny_log_rows')).toBe(3);
  });

  it('keeps the newest minute when the re-read fails, and reports the run as an error', async () => {
    const { result, calls } = await run([() => page([row()])], {
      reread: [() => json({ error: { code: 'unauthorized' } }, 401)],
    });
    expect(result).toBe('error');
    const samples = pushedSamples(calls);
    expect(samples.filter(({ timestamp }) => timestamp === windowStart)).toEqual([]);
    expect(sampleValue(samples, 'tiles_bunny_log_rows')).toBe(1);
    expect(sampleValue(samples, 'tiles_bunny_log_reader_runs', { result: 'error' })).toBe(1);
    expect(pushedLogs(calls)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          _msg: 'Bunny log reader failed',
          read: 'reread',
          window_start: '2026-11-18T12:01:00.000Z',
        }),
      ]),
    );
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
    const refused = () => json({ error: { code: 'unauthorized' } }, 401);
    const { result, calls, deps } = await run([refused], { reread: [refused] });
    expect(result).toBe('error');
    expect(calls.filter((call) => isWindowQuery(call))).toHaveLength(1);
    expect(calls.filter(isRereadQuery)).toHaveLength(1);
    expect(retryWaits(deps.sleeps)).toEqual([]);
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

  it('retries server errors and unreadable bodies twice, 1s and then 2s later, then gives up', async () => {
    const recovered = await run([
      () => new Response('bad gateway', { status: 502 }),
      () => new Response('not json', { status: 200 }),
      () => page([row()]),
    ]);
    expect(recovered.result).toBe('ok');
    expect(retryWaits(recovered.deps.sleeps)).toEqual([1000, 2000]);
    expect(sampleValue(pushedSamples(recovered.calls), 'tiles_bunny_log_rows')).toBe(1);

    const failed = await run([() => new Response('bad gateway', { status: 502 })]);
    expect(failed.result).toBe('error');
    expect(failed.calls.filter((call) => isWindowQuery(call))).toHaveLength(3);
    expect(sampleValue(pushedSamples(failed.calls), 'tiles_bunny_log_rows')).toBeUndefined();
  });

  it("waits out a 429's 10s window (or Retry-After), past the 3 tries, until the deadline", async () => {
    const limited = () => json({ code: 'rate_limited', message: 'Too many requests' }, 429);
    const recovered = await run([limited, limited, limited, limited, () => page([row()])]);
    expect(recovered.result).toBe('ok');
    expect(retryWaits(recovered.deps.sleeps)).toEqual(Array(4).fill(RATE_LIMIT_WAIT_MS));
    expect(sampleValue(pushedSamples(recovered.calls), 'tiles_bunny_log_rows')).toBe(1);

    const told = await run([
      () => new Response('{}', { status: 429, headers: { 'Retry-After': '3' } }),
      () => page([row()]),
    ]);
    expect(told.result).toBe('ok');
    expect(retryWaits(told.deps.sleeps)).toEqual([3000]);

    // No wait that would end past the read deadline.
    const failed = await run([limited]);
    expect(failed.result).toBe('error');
    expect(failed.deps.now() - (scheduled + 4000)).toBeLessThan(READ_DEADLINE_MS);
    expect(failed.calls.filter((call) => isWindowQuery(call))).toHaveLength(READ_DEADLINE_MS / RATE_LIMIT_WAIT_MS);
    expect(pushedLogs(failed.calls)).toEqual(
      expect.arrayContaining([expect.objectContaining({ read: 'first', error: expect.stringContaining('429') })]),
    );
  });

  it(`starts a run's Logging API requests at least ${REQUEST_SPACING_MS}ms apart, whatever they read`, async () => {
    const deps = testDeps(runAt);
    const pacer = new Pacer(deps);
    expect(await Promise.all([pacer.turn(), pacer.turn(), pacer.turn()])).toEqual([
      runAt,
      runAt + REQUEST_SPACING_MS,
      runAt + 2 * REQUEST_SPACING_MS,
    ]);
    deps.advance(5000);
    expect(await pacer.turn()).toBe(runAt + 2 * REQUEST_SPACING_MS + 5000);

    // A run's lag query and both minutes' pages share one pacer: 6 requests take at least 5 spacings.
    const {
      result,
      calls,
      deps: runDeps,
    } = await run([() => page([row()], true), () => page([row()], true), () => page([row()])], {
      reread: [() => page([row()], true), () => page([row()])],
    });
    expect(result).toBe('ok');
    expect(calls.filter(isLogQuery)).toHaveLength(6);
    expect(runDeps.now() - runAt).toBeGreaterThanOrEqual(5 * REQUEST_SPACING_MS);
  });

  it('fails a read that loses its connection midway rather than push part of the minute', async () => {
    const { result, calls } = await run([
      () => page([row()], true),
      () => {
        throw new TypeError('Network connection lost.');
      },
    ]);
    expect(result).toBe('error');
    expect(calls.filter((call) => isWindowQuery(call))).toHaveLength(4);
    expect(sampleValue(pushedSamples(calls), 'tiles_bunny_log_rows')).toBeUndefined();
  });

  it('fails a read whose empty page still claims more rows rather than push part of the minute', async () => {
    const { result, calls } = await run([() => page([row()], true), () => page([], true)], {
      reread: [() => page([row({ requestId: 'x' })], true), () => page([], true)],
    });
    expect(result).toBe('error');
    // One request per page: the empty page isn't retried, and nothing is read past it.
    expect(calls.filter((call) => isWindowQuery(call))).toHaveLength(2);
    expect(calls.filter(isRereadQuery)).toHaveLength(2);
    const samples = pushedSamples(calls);
    expect(samples.filter(({ name }) => name !== 'tiles_bunny_log_reader_runs')).toEqual([]);
    expect(sampleValue(samples, 'tiles_bunny_log_reader_runs', { result: 'error' })).toBe(1);
    expect(pushedLogs(calls)).toEqual(
      expect.arrayContaining(
        ['first', 'reread'].map((read) =>
          expect.objectContaining({
            _msg: 'Bunny log reader failed',
            read,
            rows: 1,
            error: expect.stringContaining('empty page with hasMore (offset 1)'),
          }),
        ),
      ),
    );
  });

  it(`stops after ${MAX_PAGES} pages and reports the run as an error, with the counts so far`, async () => {
    const { result, calls } = await run([() => page([row(), row()], true)]);
    expect(result).toBe('error');
    expect(calls.filter((call) => isWindowQuery(call))).toHaveLength(MAX_PAGES);
    const samples = pushedSamples(calls);
    expect(sampleValue(samples, 'tiles_bunny_log_rows')).toBe(2 * MAX_PAGES);
    expect(sampleValue(samples, 'tiles_bunny_log_reader_runs', { result: 'error' })).toBe(1);
    expect(pushedLogs(calls)).toEqual(
      expect.arrayContaining([expect.objectContaining({ _msg: expect.stringContaining('truncated'), level: 'error' })]),
    );
  });

  it("samples request lines at the request's own time, with the site but no IP, fingerprint or page", async () => {
    const rows = [
      row({ timestamp: '2026-11-18T12:01:10.5+00:00' }),
      row({ requestId: 'b', referer: '-', countryCode: 'DE', timestamp: '2026-11-18T12:01:59.9+00:00' }),
    ];
    // Lines come from each minute's re-read only, so none is logged twice.
    const { calls } = await run([() => page([row({ requestId: 'newest' })])], {
      sampleRate: '1',
      reread: [() => page(rows)],
    });
    const lines = pushedLogs(calls).filter(({ kind }) => kind === 'request');
    expect(lines).toEqual([
      {
        _time: '2026-11-18T12:01:10.500Z',
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
      expect.objectContaining({ _time: '2026-11-18T12:01:59.900Z', country: 'DE', request_id: 'b' }),
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
      const { calls } = await run([() => page([])], { sampleRate: '0.1', reread: [() => page(rows)] });
      return pushedLogs(calls)
        .filter(({ kind }) => kind === 'request')
        .map(({ request_id }) => request_id);
    };
    const first = await sampled();
    expect(first.length).toBeGreaterThan(100);
    expect(first.length).toBeLessThan(300);
    expect(await sampled()).toEqual(first);

    const { calls } = await run([() => page([])], { sampleRate: '1', reread: [() => page(rows)] });
    expect(pushedLogs(calls).filter(({ kind }) => kind === 'request')).toHaveLength(MAX_LOG_LINES);
  });

  it('gives up on a slow API once the read deadline has passed', async () => {
    const fetchMock = mockFetch((call) => {
      if (isWindowQuery(call, null)) {
        deps.advance(20_000);
        return page([row()], true);
      }
      return isLagQuery(call) ? page([]) : undefined;
    });
    const deps = testDeps(runAt);
    const logger = new O11yLogger(o11y);
    expect(await runLogReader(config, o11y, logger, scheduled, '0', deps)).toBe('error');
    await logger.flush();
    // Both minutes share the deadline: the re-read used it up.
    expect(fetchMock.calls.filter((call) => isWindowQuery(call, null))).toHaveLength(
      Math.ceil(READ_DEADLINE_MS / 20_000),
    );
    expect(pushedLogs(fetchMock.calls)).toEqual(
      expect.arrayContaining([expect.objectContaining({ error: expect.stringContaining('not read within 50s') })]),
    );
  });

  it('still pushes the counts when the lag query fails', async () => {
    const fetchMock = mockFetch((call) => {
      if (isLagQuery(call)) {
        return json({ error: 'nope' }, 400);
      }
      return isWindowQuery(call, null) ? page([row()]) : undefined;
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
