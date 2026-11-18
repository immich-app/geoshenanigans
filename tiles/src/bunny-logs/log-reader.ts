/**
 * Bunny's CDN request logs (Logging API v2) as per-minute counts in o11y, plus a sample of the requests in
 * VictoriaLogs.
 *
 * Each cron run reads one closed minute, LAG_MINUTES behind the minute it was scheduled for, so a repeated run reads the
 * same minute and writes the same samples at the same time (see samples.ts): no state is kept between runs. A minute the
 * cron skips is a gap, never a double count.
 */
import { parse as parseDomain } from 'tldts';
import { LogFields, O11yConfig, O11yLogger, sampleRequest } from '../o11y';
import { LogReaderConfig } from './config';
import { Deps, finite, RunResult, text } from './deps';
import { Labels, pushSamples, Sample } from './samples';

export const LOGGING_API = 'https://logging.bunnycdn.com/v2/pullzones';

const MINUTE_MS = 60_000;
// Rows are complete about 17s after the request. Reading the minute that ended 2 minutes before the run leaves a wide
// margin for a slow log pipeline or a late cron, and keeps the samples' times within VictoriaMetrics' 5-minute
// -search.cacheTimestampOffset, so cached query results don't hide them.
export const LAG_MINUTES = 3;
// The API's largest page.
export const PAGE_SIZE = 10_000;
// 300k rows a minute (5k requests/s, 50 times today's peak) at about 6.5 MB and a few tens of ms of CPU per page.
// Past it the run counts the first MAX_PAGES pages and reports an error.
export const MAX_PAGES = 30;
// Sampled request lines per minute: 1% of 100 requests/s is 60.
export const MAX_LOG_LINES = 300;
const ATTEMPTS = 3;
const RETRY_DELAY_MS = 1000;
const REQUEST_TIMEOUT_MS = 20_000;
// No request starts after this long, so a slow API can't pile up runs (each request can still take its timeout).
export const READ_DEADLINE_MS = 50_000;
// How far back the lag query looks for the newest row.
const LAG_LOOKBACK_MS = 10 * MINUTE_MS;

// The planet build's maxzoom.
const MAX_ZOOM = 15;

/** A Logging API row, as far as it is used here. remoteIp and ja4Fingerprint are deliberately never read. */
export type LogRow = {
  timestamp?: unknown;
  requestId?: unknown;
  cacheStatus?: unknown;
  statusCode?: unknown;
  bytesSent?: unknown;
  countryCode?: unknown;
  edgeLocation?: unknown;
  host?: unknown;
  path?: unknown;
  userAgent?: unknown;
  referer?: unknown;
  asn?: unknown;
  asnOrganization?: unknown;
};

type LogPage = { data: LogRow[]; hasMore: boolean };

export type Window = { start: number; end: number };

/** The closed minute a run scheduled at scheduledTime reads: [start, end) in ms. */
export function logWindow(scheduledTime: number): Window {
  const start = (Math.floor(scheduledTime / MINUTE_MS) - LAG_MINUTES) * MINUTE_MS;
  return { start, end: start + MINUTE_MS };
}

export type RequestKind = 'tile' | 'tilejson' | 'style' | 'other';

const TILE_PATH = /^\/v\d+\/(\d+)\/\d+\/\d+\.mvt$/;
const TILEJSON_PATH = /^\/v\d+\.json$/;
const STYLE_PATH = /^\/v\d+\/style\//;

/** What a request asked for, from its path (the query string is ignored), and a tile's zoom, "other" past maxzoom. */
export function classifyPath(path: string | undefined): { kind: RequestKind; zoom?: string } {
  const pathname = (path ?? '').split(/[?#]/)[0];
  const tile = TILE_PATH.exec(pathname);
  if (tile) {
    const zoom = tile[1];
    return { kind: 'tile', zoom: /^(0|[1-9]\d?)$/.test(zoom) && Number(zoom) <= MAX_ZOOM ? zoom : 'other' };
  }
  if (TILEJSON_PATH.test(pathname)) {
    return { kind: 'tilejson' };
  }
  if (STYLE_PATH.test(pathname)) {
    return { kind: 'style' };
  }
  return { kind: 'other' };
}

// Label values come from Bunny, but every combination is a series, so anything off the expected shape is "other".
const bounded = (value: unknown, pattern: RegExp) =>
  typeof value === 'string' && pattern.test(value) ? value : 'other';
const statusLabel = (value: unknown) =>
  typeof value === 'number' && Number.isInteger(value) && value >= 100 && value <= 599 ? String(value) : 'other';

/** Sums by label set; the label keys are in the same order for every add of one counter. */
class Counter {
  private series = new Map<string, Sample>();

  constructor(private name: string) {}

  add(labels: Labels, value: number) {
    const key = Object.values(labels).join('\0');
    const sample = this.series.get(key);
    if (sample) {
      sample.value += value;
    } else {
      this.series.set(key, { name: this.name, labels, value });
    }
  }

  get samples(): Sample[] {
    return [...this.series.values()];
  }
}

/** A row's place in [0, 1) from its request id, so a repeated run samples the same rows. */
function idFraction(id: string): number {
  // FNV-1a.
  let hash = 0x811c9dc5;
  for (let i = 0; i < id.length; i++) {
    hash = Math.imul(hash ^ id.charCodeAt(i), 0x01000193);
  }
  return (hash >>> 0) / 2 ** 32;
}

/** The registrable domain and host of a Referer, without its path (which can name a user's page). */
function refererSite(referer: unknown): { apex_domain?: string; referer_host?: string } {
  if (typeof referer !== 'string' || referer === '' || referer === '-') {
    return {};
  }
  try {
    const { hostname } = new URL(referer);
    if (!hostname) {
      return {};
    }
    return { apex_domain: parseDomain(hostname).domain ?? hostname, referer_host: hostname };
  } catch {
    return {};
  }
}

/** One minute's rows as the tiles_bunny_* counts, and the sampled rows as log lines. */
export class WindowAggregate {
  rows = 0;
  readonly lines: LogFields[] = [];
  private requests = new Counter('tiles_bunny_requests');
  private bytes = new Counter('tiles_bunny_bytes');
  private tiles = new Counter('tiles_bunny_tile_requests');
  private countries = new Counter('tiles_bunny_country_requests');

  constructor(
    private window: Window,
    private sampleRate: string | undefined,
    private random: () => number,
  ) {}

  add(row: LogRow) {
    this.rows++;
    const path = text(row.path, 2048);
    const { kind, zoom } = classifyPath(path);
    const status = statusLabel(row.statusCode);
    const cache = bounded(row.cacheStatus, /^[A-Z_]{1,20}$/);
    // edge_location, not region: region is one of o11y's identity labels (world for the tiles worker).
    const edge = bounded(row.edgeLocation, /^[A-Z0-9]{1,8}$/);
    this.requests.add({ kind, status, cache, edge_location: edge }, 1);
    this.bytes.add({ kind, edge_location: edge }, Math.max(0, finite(row.bytesSent) ?? 0));
    if (kind === 'tile') {
      this.tiles.add({ zoom: zoom!, cache }, 1);
    }
    this.countries.add({ country: bounded(row.countryCode, /^[A-Z]{2}$/) }, 1);

    const id = text(row.requestId);
    if (this.lines.length < MAX_LOG_LINES && sampleRequest(this.sampleRate, id ? () => idFraction(id) : this.random)) {
      this.lines.push(this.line(row, kind, zoom, path));
    }
  }

  get samples(): Sample[] {
    return [
      ...this.requests.samples,
      ...this.bytes.samples,
      ...this.tiles.samples,
      ...this.countries.samples,
      { name: 'tiles_bunny_log_rows', labels: {}, value: this.rows },
    ];
  }

  // The fields the worker's own request sample has where Bunny has them (request_type, status, z, apex_domain, as_org,
  // country), plus Bunny's. No client IP.
  private line(row: LogRow, kind: RequestKind, zoom: string | undefined, path: string | undefined): LogFields {
    const time = Date.parse(text(row.timestamp) ?? '');
    return {
      _time: new Date(Number.isFinite(time) ? time : this.window.start).toISOString(),
      kind: 'request',
      cdn: 'bunny',
      request_type: kind,
      z: zoom,
      status: finite(row.statusCode),
      cache: text(row.cacheStatus, 32),
      edge_location: text(row.edgeLocation, 32),
      country: text(row.countryCode, 8),
      as_org: text(row.asnOrganization, 128),
      asn: finite(row.asn),
      ...refererSite(row.referer),
      user_agent: text(row.userAgent),
      bytes: finite(row.bytesSent),
      host: text(row.host),
      path: path?.split(/[?#]/)[0].slice(0, 256),
      request_id: text(row.requestId, 64),
    };
  }
}

// The API takes whole seconds: from is inclusive and to exclusive, so whole minutes don't overlap.
const isoSeconds = (ms: number) => new Date(Math.floor(ms / 1000) * 1000).toISOString().replace(/\.\d{3}Z$/, 'Z');

type PageQuery = { from: number; to: number; limit: number; offset: number; order: 'asc' | 'desc'; deadline: number };

/** One page, retried on 429, 5xx, timeouts and unreadable bodies; anything else (a bad key, a bad query) throws. */
async function getLogPage(config: LogReaderConfig, query: PageQuery, deps: Deps): Promise<LogPage> {
  const params = new URLSearchParams({
    from: isoSeconds(query.from),
    to: isoSeconds(query.to),
    limit: String(query.limit),
    offset: String(query.offset),
    order: query.order,
  });
  const url = `${LOGGING_API}/${config.pullZoneId}/logs?${params}`;
  for (let attempt = 1; ; attempt++) {
    if (deps.now() >= query.deadline) {
      throw new Error(`Logging API not read within ${READ_DEADLINE_MS / 1000}s (offset ${query.offset})`);
    }
    let reason: string;
    let retry = true;
    try {
      const response = await fetch(url, {
        headers: { AccessKey: config.apiKey, Accept: 'application/json' },
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
      if (response.ok) {
        const body = (await response.json()) as { data?: unknown; pagination?: { hasMore?: unknown } };
        if (Array.isArray(body?.data) && typeof body.pagination?.hasMore === 'boolean') {
          return { data: body.data as LogRow[], hasMore: body.pagination.hasMore };
        }
        reason = 'a body without data and pagination.hasMore';
      } else {
        const detail = await response.text().catch(() => '');
        reason = `${response.status} ${detail.slice(0, 300)}`;
        retry = response.status === 429 || response.status >= 500;
      }
    } catch (error) {
      reason = error instanceof Error ? error.message : String(error);
    }
    if (!retry || attempt >= ATTEMPTS) {
      throw new Error(`Logging API answered ${reason} (offset ${query.offset}, attempt ${attempt})`);
    }
    await deps.sleep(RETRY_DELAY_MS * attempt);
  }
}

/** Every row of the window into the aggregate, page by page; truncated when MAX_PAGES didn't reach the end. */
export async function readWindow(
  config: LogReaderConfig,
  window: Window,
  aggregate: WindowAggregate,
  deps: Deps,
  deadline = deps.now() + READ_DEADLINE_MS,
): Promise<{ pages: number; truncated: boolean }> {
  let offset = 0;
  for (let pages = 1; ; pages++) {
    const page = await getLogPage(
      config,
      { from: window.start, to: window.end, limit: PAGE_SIZE, offset, order: 'asc', deadline },
      deps,
    );
    for (const row of page.data) {
      aggregate.add(row ?? {});
    }
    if (!page.hasMore || page.data.length === 0) {
      return { pages, truncated: false };
    }
    if (pages >= MAX_PAGES) {
      return { pages, truncated: true };
    }
    offset += page.data.length;
  }
}

/**
 * Seconds from the newest row the Logging API has (from the last 10 minutes) to now: how far behind the logs are. Above
 * the 2-minute margin LAG_MINUTES leaves, minutes are read before they are complete. Undefined without a row.
 */
export async function logLagSeconds(config: LogReaderConfig, deps: Deps): Promise<number | undefined> {
  const now = deps.now();
  const page = await getLogPage(
    config,
    {
      from: now - LAG_LOOKBACK_MS,
      to: now + 1000,
      limit: 1,
      offset: 0,
      order: 'desc',
      deadline: now + READ_DEADLINE_MS,
    },
    deps,
  );
  const newest = Date.parse(text(page.data[0]?.timestamp) ?? '');
  return Number.isFinite(newest) ? Math.max(0, deps.now() - newest) / 1000 : undefined;
}

const runSample = (result: RunResult): Sample => ({
  name: 'tiles_bunny_log_reader_runs',
  labels: { result },
  value: 1,
});

/**
 * Reads the minute scheduledTime's run is for and pushes its counts, the lag and the run's result, all at the minute's
 * end; then logs the sampled rows. Never rejects. Without config it pushes only result="skipped".
 */
export async function runLogReader(
  config: LogReaderConfig | undefined,
  o11y: O11yConfig,
  logger: O11yLogger,
  scheduledTime: number,
  sampleRate: string | undefined,
  deps: Deps,
): Promise<RunResult> {
  const window = logWindow(scheduledTime);
  if (!config) {
    await pushSamples(o11y, [runSample('skipped')], window.end);
    return 'skipped';
  }
  const fields = { job: 'bunny_logs', window_start: new Date(window.start).toISOString() };
  const lag = logLagSeconds(config, deps).catch((error) => {
    logger.warn('Bunny log lag unknown', { ...fields, error: error instanceof Error ? error.message : String(error) });
    return undefined;
  });
  const aggregate = new WindowAggregate(window, sampleRate, deps.random);
  let result: RunResult = 'ok';
  let samples: Sample[] = [];
  try {
    const { pages, truncated } = await readWindow(config, window, aggregate, deps);
    samples = aggregate.samples;
    if (truncated) {
      result = 'error';
      logger.error('Bunny log window truncated: its counts are low', undefined, {
        ...fields,
        pages,
        rows: aggregate.rows,
      });
    }
    for (const line of aggregate.lines) {
      logger.info('Request', line);
    }
  } catch (error) {
    // Part of a minute would read as a drop in traffic, so a failed read pushes no counts.
    result = 'error';
    logger.error('Bunny log reader failed', error, { ...fields, rows: aggregate.rows });
  }
  const lagSeconds = await lag;
  if (lagSeconds !== undefined) {
    samples.push({ name: 'tiles_bunny_log_lag_seconds', labels: {}, value: lagSeconds });
  }
  samples.push(runSample(result));
  await pushSamples(o11y, samples, window.end);
  return result;
}
