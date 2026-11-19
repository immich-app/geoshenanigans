/**
 * Bunny's CDN request logs (Logging API v2) as per-minute counts in o11y, plus a sample of the requests in
 * VictoriaLogs.
 *
 * Each cron run reads two closed minutes: the one LAG_MINUTES behind the minute it was scheduled for, and the one before
 * it again (REREAD_LAG_MINUTES), which the previous run read first. Each minute's counts are stamped with its end, and
 * of samples with one timestamp o11y's dedup keeps the biggest (samples.ts), so the second read replaces a first one
 * that was short (logs still arriving) or failed, and no minute is counted twice. No state is kept between runs; a
 * minute only lacks counts when both its reads fail.
 *
 * The edge is the `edge_location` label, not the `region` the spec names: region is one of o11y's identity labels
 * (world for the tiles worker), which samples.ts writes last, so a region of ours would be overwritten.
 */
import { parse as parseDomain } from 'tldts';
import { LogFields, O11yConfig, O11yLogger, sampleRequest } from '../o11y';
import { LogReaderConfig } from './config';
import { Deps, finite, RunResult, text } from './deps';
import { Labels, pushSamples, pushSampleSets, Sample } from './samples';

export const LOGGING_API = 'https://logging.bunnycdn.com/v2/pullzones';

const MINUTE_MS = 60_000;
// Rows are complete about 17s after the request. Reading the minute that ended 2 minutes before the run leaves a wide
// margin for a slow log pipeline or a late cron, and the re-read a minute later another. Both keep the samples' times
// within VictoriaMetrics' 5-minute -search.cacheTimestampOffset, so cached query results don't hide them.
export const LAG_MINUTES = 3;
export const REREAD_LAG_MINUTES = LAG_MINUTES + 1;
// The API's largest page.
export const PAGE_SIZE = 10_000;
// 300k rows a minute (5k requests/s, 50 times today's peak) at about 6.5 MB and a few tens of ms of CPU per page.
// Past it the run counts the first MAX_PAGES pages and reports an error.
export const MAX_PAGES = 30;
// Sampled request lines per minute: 1% of 100 requests/s is 60.
export const MAX_LOG_LINES = 300;
// Failures other than rate limits: 3 tries, 1s and then 2s apart.
const ATTEMPTS = 3;
const RETRY_DELAY_MS = 1000;
// Bunny allows 30 Logging API requests per 10s per pull zone. A run starts its requests at least this far apart (25 per
// 10s), leaving room for anyone else reading the zone's logs, and after a 429 waits out the limit's window unless
// Retry-After asks for less. Rate limits are retried until the read deadline.
export const REQUEST_SPACING_MS = 400;
export const RATE_LIMIT_WAIT_MS = 10_000;
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

/** The closed minute lagMinutes before the minute of scheduledTime: [start, end) in ms. */
export function logWindow(scheduledTime: number, lagMinutes = LAG_MINUTES): Window {
  const start = (Math.floor(scheduledTime / MINUTE_MS) - lagMinutes) * MINUTE_MS;
  return { start, end: start + MINUTE_MS };
}

export type RequestKind = 'tile' | 'tilejson' | 'style' | 'other';

// Tiles sit under the zone's tile prefix, `v1` or, after a full rewrite, a nested one such as `v1/e2` (bunny:sync
// --adopt --replace): any prefix of one or more folders, then z/x/y.mvt.
const TILE_PATH = /^\/(?:[^/]+\/)+(\d+)\/\d+\/\d+\.mvt$/;
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

  /** sampleRate undefined is the default rate; "0" keeps no lines. */
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
    // edge_location, not region: see the module comment.
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

/** Spaces the starts of one run's Logging API requests REQUEST_SPACING_MS apart, whichever read they belong to. */
export class Pacer {
  private next = 0;

  constructor(private deps: Deps) {}

  /** Waits for the next free start, which it returns. */
  async turn(): Promise<number> {
    const now = this.deps.now();
    const at = Math.max(now, this.next);
    this.next = at + REQUEST_SPACING_MS;
    if (at > now) {
      await this.deps.sleep(at - now);
    }
    return at;
  }
}

/** How long to wait after a 429: Retry-After's seconds when it gives a positive number, else the limit's window. */
function rateLimitWaitMs(retryAfter: string | null): number {
  const seconds = retryAfter === null || retryAfter.trim() === '' ? NaN : Number(retryAfter);
  return Number.isFinite(seconds) && seconds > 0 ? Math.max(1000, seconds * 1000) : RATE_LIMIT_WAIT_MS;
}

/**
 * One page, retried on 429 (until the deadline), 5xx, timeouts and unreadable bodies (ATTEMPTS tries); anything else
 * (a bad key, a bad query, a redirect) throws.
 */
async function getLogPage(config: LogReaderConfig, query: PageQuery, deps: Deps, pacer: Pacer): Promise<LogPage> {
  const params = new URLSearchParams({
    from: isoSeconds(query.from),
    to: isoSeconds(query.to),
    limit: String(query.limit),
    offset: String(query.offset),
    order: query.order,
  });
  const url = `${LOGGING_API}/${config.pullZoneId}/logs?${params}`;
  let failures = 0;
  for (let attempt = 1; ; attempt++) {
    await pacer.turn();
    if (deps.now() >= query.deadline) {
      throw new Error(`Logging API not read within ${READ_DEADLINE_MS / 1000}s (offset ${query.offset})`);
    }
    let reason: string;
    let retry = true;
    let rateLimitWait: number | undefined;
    try {
      const response = await fetch(url, {
        headers: { AccessKey: config.apiKey, Accept: 'application/json' },
        // A redirect would carry the account key to wherever it points: it fails the read instead.
        redirect: 'manual',
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
      if (response.ok) {
        const body = (await response.json()) as { data?: unknown; pagination?: { hasMore?: unknown } } | null;
        const hasMore = body?.pagination?.hasMore;
        // data is nullable in the API: null on a last page without rows is an empty page. Missing, or null on a page
        // that claims more rows, is a body that can't be trusted.
        const data = body?.data === null && hasMore === false ? [] : body?.data;
        if (Array.isArray(data) && typeof hasMore === 'boolean') {
          return { data: data as LogRow[], hasMore };
        }
        reason = 'a body without data and pagination.hasMore';
      } else {
        const detail = await response.text().catch(() => '');
        reason = `${response.status} ${detail.slice(0, 300)}`;
        if (response.status === 429) {
          rateLimitWait = rateLimitWaitMs(response.headers.get('Retry-After'));
        }
        retry = response.status === 429 || response.status >= 500;
      }
    } catch (error) {
      reason = error instanceof Error ? error.message : String(error);
    }
    if (rateLimitWait === undefined) {
      failures++;
    }
    const wait = rateLimitWait ?? RETRY_DELAY_MS * failures;
    if (!retry || failures >= ATTEMPTS || deps.now() + wait >= query.deadline) {
      throw new Error(`Logging API answered ${reason} (offset ${query.offset}, attempt ${attempt})`);
    }
    await deps.sleep(wait);
  }
}

/**
 * Every row of the window into the aggregate, page by page; truncated when MAX_PAGES didn't reach the end. Only a page
 * without hasMore ends the window: an empty page that claims more rows can't advance the offset, so it throws rather
 * than pass part of the minute off as all of it.
 */
export async function readWindow(
  config: LogReaderConfig,
  window: Window,
  aggregate: WindowAggregate,
  deps: Deps,
  pacer: Pacer,
  deadline: number,
): Promise<{ pages: number; truncated: boolean }> {
  let offset = 0;
  for (let pages = 1; ; pages++) {
    const page = await getLogPage(
      config,
      { from: window.start, to: window.end, limit: PAGE_SIZE, offset, order: 'asc', deadline },
      deps,
      pacer,
    );
    for (const row of page.data) {
      aggregate.add(row ?? {});
    }
    if (!page.hasMore) {
      return { pages, truncated: false };
    }
    if (page.data.length === 0) {
      throw new Error(`Logging API answered an empty page with hasMore (offset ${offset})`);
    }
    if (pages >= MAX_PAGES) {
      return { pages, truncated: true };
    }
    offset += page.data.length;
  }
}

/**
 * Seconds from the newest row the Logging API has (from the last 10 minutes) to now: how far behind the logs are. Above
 * the 2-minute margin LAG_MINUTES leaves, a minute's first read is short and only its re-read (3 minutes) can be whole.
 * Undefined without a row, which is also what a quiet zone gives, so it doesn't fail the run.
 */
export async function logLagSeconds(config: LogReaderConfig, deps: Deps, pacer: Pacer): Promise<number | undefined> {
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
    pacer,
  );
  const newest = Date.parse(text(page.data[0]?.timestamp) ?? '');
  return Number.isFinite(newest) ? Math.max(0, deps.now() - newest) / 1000 : undefined;
}

const runSample = (result: RunResult): Sample => ({
  name: 'tiles_bunny_log_reader_runs',
  labels: { result },
  value: 1,
});

/** One read of a minute: its counts, or none when it failed (part of a minute would read as a drop in traffic). */
async function readMinute(
  config: LogReaderConfig,
  window: Window,
  aggregate: WindowAggregate,
  logger: O11yLogger,
  fields: LogFields,
  deps: Deps,
  pacer: Pacer,
  deadline: number,
): Promise<{ ok: boolean; samples: Sample[] }> {
  const readFields = { ...fields, window_start: new Date(window.start).toISOString() };
  try {
    const { pages, truncated } = await readWindow(config, window, aggregate, deps, pacer, deadline);
    if (truncated) {
      logger.error('Bunny log window truncated: its counts are low', undefined, {
        ...readFields,
        pages,
        rows: aggregate.rows,
      });
    }
    for (const line of aggregate.lines) {
      logger.info('Request', line);
    }
    return { ok: !truncated, samples: aggregate.samples };
  } catch (error) {
    logger.error('Bunny log reader failed', error, { ...readFields, rows: aggregate.rows });
    return { ok: false, samples: [] };
  }
}

/**
 * Re-reads the minute the previous run read first, then reads the newest closed minute (logWindow), and pushes each
 * minute's counts at its end, with the lag and the run's result at the newest minute's end; then logs the sampled rows,
 * which come from the re-read, each minute's last and fullest. Never rejects. Without config it pushes only
 * result="skipped". The run is an error when either read failed or was truncated.
 */
export async function runLogReader(
  config: LogReaderConfig | undefined,
  o11y: O11yConfig,
  logger: O11yLogger,
  scheduledTime: number,
  sampleRate: string | undefined,
  deps: Deps,
): Promise<RunResult> {
  const latest = logWindow(scheduledTime);
  if (!config) {
    await pushSamples(o11y, [runSample('skipped')], latest.end);
    return 'skipped';
  }
  const reread = logWindow(scheduledTime, REREAD_LAG_MINUTES);
  const fields = { job: 'bunny_logs' };
  const pacer = new Pacer(deps);
  const deadline = deps.now() + READ_DEADLINE_MS;
  const lag = logLagSeconds(config, deps, pacer).catch((error) => {
    logger.warn('Bunny log lag unknown', { ...fields, error: error instanceof Error ? error.message : String(error) });
    return undefined;
  });
  // The re-read first: it is its minute's last chance, while the newest minute gets another next run.
  const second = await readMinute(
    config,
    reread,
    new WindowAggregate(reread, sampleRate, deps.random),
    logger,
    { ...fields, read: 'reread' },
    deps,
    pacer,
    deadline,
  );
  const first = await readMinute(
    config,
    latest,
    new WindowAggregate(latest, '0', deps.random),
    logger,
    { ...fields, read: 'first' },
    deps,
    pacer,
    deadline,
  );
  const result: RunResult = first.ok && second.ok ? 'ok' : 'error';
  const lagSeconds = await lag;
  const latestSamples = [...first.samples];
  if (lagSeconds !== undefined) {
    latestSamples.push({ name: 'tiles_bunny_log_lag_seconds', labels: {}, value: lagSeconds });
  }
  latestSamples.push(runSample(result));
  await pushSampleSets(o11y, [
    { samples: second.samples, timestampMs: reread.end },
    { samples: latestSamples, timestampMs: latest.end },
  ]);
  return result;
}
