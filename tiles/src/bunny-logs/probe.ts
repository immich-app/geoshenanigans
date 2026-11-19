/**
 * The synthetic probe: every few minutes, one Globalping probe per continent requests a popular tile (a cache hit at
 * nearly every edge) and a random z15 position (nearly always a miss, so it measures the trip to storage) from the
 * Bunny-served origin, and the time to first byte goes to o11y as tiles_bunny_probe_*.
 *
 * The run waits for its own measurements (a few seconds, never much more than RESULT_DEADLINE_MS, against the 15 minutes
 * a cron run may take) instead of reading them on a later run: the worker has nowhere to keep a measurement id between
 * runs, as cron runs land on any machine and the worker has no KV or Durable Object.
 *
 * Samples are labelled by `continent` (the probe's), not the spec's `region`, which is an o11y identity label (see
 * samples.ts), and by the `kind` the edge reported (CDN-Cache), so a hit tile a purge or a cold edge turned into a miss
 * counts as a miss.
 */
import { LogFields, O11yConfig, O11yLogger } from '../o11y';
import { ProbeConfig } from './config';
import { Deps, finite, RunResult, text } from './deps';
import { pushSamples, Sample } from './samples';

export const GLOBALPING_API = 'https://api.globalping.io/v1/measurements';
export const PROBE_CONTINENTS = ['EU', 'NA', 'SA', 'AS', 'OC', 'AF'] as const;
// Most of Europe, where most requests come from, at a zoom every world and continent view loads.
export const HIT_TILE = '2/2/1';
const MISS_ZOOM = 15;
const USER_AGENT = 'immich-tiles-probe/1.0 (+https://github.com/immich-app/geoshenanigans)';
// A test may take up to 30s on the probe and the API asks clients to allow 10s more; then the run gives up.
const RESULT_DEADLINE_MS = 45_000;
const FIRST_POLL_MS = 2000;
// The API allows 2 requests a second per measurement.
const POLL_INTERVAL_MS = 1000;
const REQUEST_TIMEOUT_MS = 15_000;

export type ProbeKind = 'hit' | 'miss';

type Target = { kind: ProbeKind; path: string };

type TestResult = {
  status?: unknown;
  statusCode?: unknown;
  headers?: unknown;
  timings?: { firstByte?: unknown; total?: unknown };
  rawOutput?: unknown;
  failureSource?: unknown;
};

type ResultItem = {
  probe?: { continent?: unknown; country?: unknown; city?: unknown; network?: unknown; asn?: unknown };
  result?: TestResult;
};

export type Measurement = { id: string; status?: unknown; results?: ResultItem[] };

/** Whether the run scheduled at scheduledTime probes: on every intervalMinutes'th minute since the epoch. */
export function probeDue(scheduledTime: number, intervalMinutes: number): boolean {
  return Math.floor(scheduledTime / 60_000) % intervalMinutes === 0;
}

/** A random z15 tile path, z/x/y. */
export function missTile(random: () => number): string {
  const size = 2 ** MISS_ZOOM;
  const coordinate = () => Math.min(size - 1, Math.floor(random() * size));
  return `${MISS_ZOOM}/${coordinate()}/${coordinate()}`;
}

function tilePath(publicUrl: URL, tile: string): string {
  return `${publicUrl.pathname.replace(/\/+$/, '')}/v1/${tile}.mvt`;
}

/** The Globalping HTTP measurement of path on the public origin, from locations (or a measurement's probes). */
export function measurementRequest(publicUrl: URL, path: string, locations: unknown) {
  return {
    type: 'http',
    target: publicUrl.hostname,
    locations,
    measurementOptions: {
      protocol: publicUrl.protocol === 'http:' ? 'HTTP' : 'HTTPS',
      ...(publicUrl.port !== '' && { port: Number(publicUrl.port) }),
      // Clients ask for gzip, and the tiles are stored gzipped.
      request: { method: 'GET', path, headers: { 'Accept-Encoding': 'gzip' } },
    },
  };
}

async function createMeasurement(config: ProbeConfig, body: unknown): Promise<string> {
  const response = await fetch(GLOBALPING_API, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${config.token}`,
      'Content-Type': 'application/json',
      'User-Agent': USER_AGENT,
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });
  if (response.status !== 202) {
    // A 429 is the hourly quota (its body says which); a 422 a location no probe matches.
    const detail = await response.text().catch(() => '');
    throw new Error(`Globalping answered ${response.status} ${detail.slice(0, 300)}`);
  }
  const { id } = (await response.json()) as { id?: unknown };
  if (typeof id !== 'string' || id === '') {
    throw new Error('Globalping answered 202 without a measurement id');
  }
  return id;
}

/** The measurement as it stands, or undefined when it couldn't be read this time. */
async function getMeasurement(id: string): Promise<Measurement | undefined> {
  try {
    const response = await fetch(`${GLOBALPING_API}/${encodeURIComponent(id)}`, {
      headers: { 'User-Agent': USER_AGENT },
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
    if (!response.ok) {
      await response.body?.cancel();
      return undefined;
    }
    return { ...((await response.json()) as Measurement), id };
  } catch {
    return undefined;
  }
}

/** Whether a measurement's results are final: any status but in-progress is. */
const settled = (measurement: Measurement | undefined) =>
  measurement !== undefined && measurement.status !== 'in-progress';

/** The measurements once none is in progress, or as they stood at the deadline. */
async function awaitMeasurements(ids: string[], deps: Deps): Promise<Map<string, Measurement>> {
  const deadline = deps.now() + RESULT_DEADLINE_MS;
  const latest = new Map<string, Measurement>();
  await deps.sleep(FIRST_POLL_MS);
  for (;;) {
    const pending = ids.filter((id) => !settled(latest.get(id)));
    for (const measurement of await Promise.all(pending.map(getMeasurement))) {
      if (measurement) {
        latest.set(measurement.id, measurement);
      }
    }
    if (ids.every((id) => settled(latest.get(id))) || deps.now() >= deadline) {
      return latest;
    }
    await deps.sleep(POLL_INTERVAL_MS);
  }
}

const continentLabel = (value: unknown) =>
  typeof value === 'string' && (PROBE_CONTINENTS as readonly string[]).includes(value) ? value : 'other';

function header(result: TestResult, name: string): string | undefined {
  if (!result.headers || typeof result.headers !== 'object') {
    return undefined;
  }
  for (const [key, value] of Object.entries(result.headers as Record<string, unknown>)) {
    if (key.toLowerCase() === name) {
      return text(Array.isArray(value) ? value[0] : value, 128);
    }
  }
  return undefined;
}

/**
 * A test's time to first byte, or undefined when it didn't get a tile a map can read: not finished, not a 200, or
 * without Content-Encoding: gzip. The stored tiles are gzip bytes, so without the header (the gzip middleware detached,
 * or an edge rule not yet everywhere) every client fails to decode them, while the probe would see a 200.
 */
export function testTtfb(result: TestResult | undefined): number | undefined {
  if (!result || result.status !== 'finished' || result.statusCode !== 200) {
    return undefined;
  }
  if (header(result, 'content-encoding')?.trim().toLowerCase() !== 'gzip') {
    return undefined;
  }
  return finite(result.timings?.firstByte);
}

/**
 * Whether a test's outcome says anything about the tiles: a finished test does, and a failed one unless Globalping puts
 * the failure on the probe's DNS resolver or on Globalping itself (failureSource, "best-effort" in its API; `target`
 * covers the target's DNS records and timeouts). An offline probe or a test still in progress at the deadline doesn't.
 */
export function testHasVerdict(result: TestResult | undefined): boolean {
  if (result?.status === 'finished') {
    return true;
  }
  return result?.status === 'failed' && result.failureSource !== 'resolver' && result.failureSource !== 'internal';
}

/**
 * What a test measured: the edge's CDN-Cache when it sent one (HIT or STALE is a hit, anything else a miss), else the
 * target's kind.
 */
export function measuredKind(target: Target, result: TestResult | undefined): ProbeKind {
  const cache = result ? header(result, 'cdn-cache')?.trim().toUpperCase() : undefined;
  if (!cache) {
    return target.kind;
  }
  return cache === 'HIT' || cache === 'STALE' ? 'hit' : 'miss';
}

/**
 * The samples and log lines of a measurement's tests: a TTFB and a 0 for each test that got the tile, a 1 for each
 * other test with a verdict (testHasVerdict), and how many tests had one.
 */
export function measurementSamples(
  target: Target,
  measurement: Measurement | undefined,
): { samples: Sample[]; lines: LogFields[]; tests: number; verdicts: number } {
  const samples: Sample[] = [];
  const lines: LogFields[] = [];
  let verdicts = 0;
  const results = measurement?.results ?? [];
  for (const { probe, result } of results) {
    const labels = { continent: continentLabel(probe?.continent), kind: measuredKind(target, result) };
    const ttfb = testTtfb(result);
    if (ttfb !== undefined) {
      samples.push({ name: 'tiles_bunny_probe_ttfb_ms', labels, value: ttfb });
    }
    const verdict = testHasVerdict(result);
    if (verdict) {
      verdicts++;
      samples.push({ name: 'tiles_bunny_probe_errors', labels, value: ttfb === undefined ? 1 : 0 });
    }
    lines.push({
      kind: 'probe',
      cdn: 'bunny',
      target: target.kind,
      path: target.path,
      measurement_id: measurement?.id,
      continent: text(probe?.continent, 8),
      country: text(probe?.country, 8),
      city: text(probe?.city, 64),
      network: text(probe?.network, 128),
      asn: finite(probe?.asn),
      test_status: text(result?.status, 32),
      status: finite(result?.statusCode),
      ttfb_ms: ttfb,
      total_ms: finite(result?.timings?.total),
      // Bunny's own view: the edge's cache status and the server (PoP) that answered, and what a map needs to read it.
      cache: result ? header(result, 'cdn-cache') : undefined,
      server: result ? header(result, 'server') : undefined,
      content_encoding: result ? header(result, 'content-encoding') : undefined,
      content_type: result ? header(result, 'content-type') : undefined,
      counted: verdict,
      // Why a test failed (a timeout, a TLS error, a body that isn't the gzip it claims to be); a finished test's raw
      // output is the whole response instead.
      ...(result?.status !== 'finished' && {
        failure: text(result?.rawOutput, 200),
        failure_source: text(result?.failureSource, 32),
      }),
    });
  }
  return { samples, lines, tests: results.length, verdicts };
}

/**
 * Probes the hit and miss tiles from one probe per continent (the miss measurement reuses the hit one's probes, so the
 * two compare like for like), waits for the results and pushes them with the run's result at the scheduled minute.
 * Never rejects. Without config it pushes only result="skipped".
 */
export async function runProbe(
  config: ProbeConfig | undefined,
  o11y: O11yConfig,
  logger: O11yLogger,
  scheduledTime: number,
  deps: Deps,
): Promise<RunResult> {
  const minute = Math.floor(scheduledTime / 60_000) * 60_000;
  const runs = (result: RunResult): Sample => ({ name: 'tiles_bunny_probe_runs', labels: { result }, value: 1 });
  if (!config) {
    await pushSamples(o11y, [runs('skipped')], minute);
    return 'skipped';
  }
  const fields = { job: 'bunny_probe', run_minute: new Date(minute).toISOString() };
  const targets: Target[] = [
    { kind: 'hit', path: tilePath(config.publicUrl, HIT_TILE) },
    { kind: 'miss', path: tilePath(config.publicUrl, missTile(deps.random)) },
  ];
  const continents = PROBE_CONTINENTS.map((continent) => ({ continent, limit: 1 }));
  const ids = new Map<ProbeKind, string>();
  let result: RunResult = 'ok';
  for (const target of targets) {
    const locations = ids.get('hit') ?? continents;
    try {
      ids.set(
        target.kind,
        await createMeasurement(config, measurementRequest(config.publicUrl, target.path, locations)),
      );
    } catch (error) {
      result = 'error';
      logger.error('Globalping measurement not created', error, { ...fields, target: target.kind });
    }
  }
  const samples: Sample[] = [];
  if (ids.size > 0) {
    const measurements = await awaitMeasurements([...ids.values()], deps);
    for (const target of targets) {
      const id = ids.get(target.kind);
      if (id === undefined) {
        continue;
      }
      const measurement = measurements.get(id);
      if (!settled(measurement)) {
        result = 'error';
        logger.error('Globalping measurement unfinished at the deadline', undefined, {
          ...fields,
          target: target.kind,
          measurement_id: id,
        });
      }
      const measured = measurementSamples(target, measurement);
      if (settled(measurement) && measured.verdicts === 0) {
        // Every test failed on Globalping's side or its probes went offline: nothing about the tiles was measured.
        result = 'error';
        logger.error('Globalping measurement has no test with a verdict', undefined, {
          ...fields,
          target: target.kind,
          measurement_id: id,
          tests: measured.tests,
        });
      }
      samples.push(...measured.samples);
      for (const line of measured.lines) {
        logger.info('Probe', { ...fields, ...line });
      }
    }
  }
  samples.push(runs(result));
  await pushSamples(o11y, samples, minute);
  return result;
}
