/**
 * The synthetic probe: every few minutes, one Globalping probe per continent requests a popular tile (a cache hit at
 * nearly every edge) and a random z15 position (nearly always a miss, so it measures the trip to storage) from the
 * Bunny-served origin, and the time to first byte goes to o11y as tiles_bunny_probe_*. The tile URLs come from the
 * origin's TileJSON (`/v1.json`), as they do for clients, so after a full rewrite moves the tiles to another prefix
 * (`v1/e2`) the probe measures the new prefix, not the retired one. Both tiles are within the TileJSON's minzoom and
 * maxzoom (probeTiles): a build that stops below z15 or starts above z2 has no tile there to find.
 *
 * The run waits for its own measurements (a few seconds, never much more than RESULT_DEADLINE_MS, against the 15 minutes
 * a cron run may take) instead of reading them on a later run: the worker has nowhere to keep a measurement id between
 * runs, as cron runs land on any machine and the worker has no KV or Durable Object.
 *
 * Samples are labelled by `continent` (the probe's), not the spec's `region`, which is an o11y identity label (see
 * samples.ts), by the `kind` the edge reported (CDN-Cache), so a hit tile a purge or a cold edge turned into a miss
 * counts as a miss, and by the `target` that was asked for (hit or miss). Both targets' samples are stamped at the run's
 * minute, so without `target` a cold hit tile and the random tile from one continent would be one series at one
 * timestamp, of which o11y's dedup keeps only the bigger value (samples.ts): one test's TTFB or error would be lost.
 *
 * The Worker's own read of the TileJSON is a test too, pushed every run as tiles_bunny_probe_errors with
 * continent="worker", kind="tilejson" (TILEJSON_LABELS): 1 when it couldn't be read or gave no usable tile URL, 0 when
 * it did. Every map needs that file, and with no tile URL nothing else is measured, so an outage that takes it down
 * (the CDN, the storage zone, a pull-zone hostname, a deleted v1.json) reaches the critical probe-errors alert like
 * failed tiles do, not only the probe-failing warning.
 */
import { LogFields, O11yConfig, O11yLogger } from '../o11y';
import { ProbeConfig } from './config';
import { Deps, finite, RunResult, text } from './deps';
import { pushSamples, Sample } from './samples';

export const GLOBALPING_API = 'https://api.globalping.io/v1/measurements';
export const PROBE_CONTINENTS = ['EU', 'NA', 'SA', 'AS', 'OC', 'AF'] as const;
// Most of Europe, where most requests come from, at a zoom every world and continent view loads.
export const HIT_TILE = '2/2/1';
// TileJSON 3.0.0's zoom limits, and its minzoom and maxzoom when it gives none.
const TILEJSON_ZOOMS = { min: 0, max: 30 };
// The TileJSON Immich loads (static-files.ts' STATIC_PREFIX), whose tile template says where the live tiles are.
export const TILEJSON_PATH = '/v1.json';
// The labels of the Worker's TileJSON read's verdict: a vantage point and a kind of their own, never a probe's.
export const TILEJSON_LABELS = { continent: 'worker', kind: 'tilejson', target: 'tilejson' } as const;
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

/** What the public origin's TileJSON advertises: its first tile template and its zoom range. */
export type LiveTiles = { template: string; minZoom: number; maxZoom: number };

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

/** A random tile path at zoom (z15 unless told otherwise), z/x/y. */
export function missTile(random: () => number, zoom = MISS_ZOOM): string {
  const size = 2 ** zoom;
  const coordinate = () => Math.min(size - 1, Math.floor(random() * size));
  return `${zoom}/${coordinate()}/${coordinate()}`;
}

/** HIT_TILE at zoom: itself, the tile above it that holds it, or the tile below it that holds its centre. */
export function hitTile(zoom: number): string {
  const [z, x, y] = HIT_TILE.split('/').map(Number);
  if (zoom <= z) {
    const shift = 2 ** (z - zoom);
    return `${zoom}/${Math.floor(x / shift)}/${Math.floor(y / shift)}`;
  }
  const scale = 2 ** (zoom - z);
  return `${zoom}/${x * scale + scale / 2}/${y * scale + scale / 2}`;
}

/**
 * The hit and miss tiles within the TileJSON's zoom range: HIT_TILE and a random z15 position for the planet build
 * (z0-15), else each at the zoom in the range nearest its own. A staged or clipped build that stops below z15 has no
 * z15 file, nor one that starts above z2 a z2 file: probing them would page on 404s that no client gets.
 */
export function probeTiles(live: Pick<LiveTiles, 'minZoom' | 'maxZoom'>, random: () => number) {
  const within = (zoom: number) => Math.min(Math.max(zoom, live.minZoom), live.maxZoom);
  const hitZoom = Number(HIT_TILE.split('/')[0]);
  return { hit: hitTile(within(hitZoom)), miss: missTile(random, within(MISS_ZOOM)) };
}

// The public origin's path, without a trailing slash: '' for https://tiles.immich.cloud.
const basePath = (publicUrl: URL) => publicUrl.pathname.replace(/\/+$/, '');

/**
 * Tile z/x/y's URL from the TileJSON's tile template. Throws unless the template is an absolute http(s) URL under the
 * public URL's path, without a query or fragment, with one each of {z}, {x} and {y}. Its origin can be another of the
 * pull zone's hostnames: from the DNS cutover until the next sync rewrites v1.json, the TileJSON on the public hostname
 * still names the test one (deployment/modules/bunny/tiles/README.md). The probe requests the path from the public
 * origin either way, so a TileJSON never sends Globalping to another host.
 */
export function tileUrl(publicUrl: URL, template: string, tile: string): URL {
  const shown = JSON.stringify(template.slice(0, 200));
  if (!['{z}', '{x}', '{y}'].every((placeholder) => template.split(placeholder).length === 2)) {
    throw new Error(`Tile template ${shown} doesn't have one each of {z}, {x} and {y}`);
  }
  const [z, x, y] = tile.split('/');
  let url: URL | undefined;
  try {
    url = new URL(template.replace('{z}', z).replace('{x}', x).replace('{y}', y));
  } catch {
    url = undefined;
  }
  const base = basePath(publicUrl);
  if (
    !url ||
    (url.protocol !== 'https:' && url.protocol !== 'http:') ||
    url.search !== '' ||
    url.hash !== '' ||
    !url.pathname.startsWith(`${base}/`)
  ) {
    throw new Error(`Tile template ${shown} isn't an http(s) tile URL under ${base || '/'}`);
  }
  return url;
}

/**
 * The tile template the public origin's TileJSON advertises now (its first), which clients follow (a full rewrite
 * moves it to another prefix), and its zoom range. Throws when the TileJSON can't be read, has no tile template, or
 * has a minzoom or maxzoom that isn't a whole zoom from 0 to 30 or a minzoom above its maxzoom (TileJSON 3.0.0; either
 * left out is its default, 0 and 30).
 */
export async function liveTiles(publicUrl: URL): Promise<LiveTiles> {
  const url = `${publicUrl.origin}${basePath(publicUrl)}${TILEJSON_PATH}`;
  const response = await fetch(url, {
    headers: { Accept: 'application/json', 'User-Agent': USER_AGENT },
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });
  if (!response.ok) {
    await response.body?.cancel();
    throw new Error(`${url} answered ${response.status}`);
  }
  const body = (await response.json()) as { tiles?: unknown; minzoom?: unknown; maxzoom?: unknown } | null;
  const template = Array.isArray(body?.tiles) ? body.tiles[0] : undefined;
  if (typeof template !== 'string') {
    throw new Error(`${url} has no tile template`);
  }
  const zoom = (name: 'minzoom' | 'maxzoom', fallback: number) => {
    const value = body?.[name];
    if (value === undefined) {
      return fallback;
    }
    if (
      typeof value !== 'number' ||
      !Number.isInteger(value) ||
      value < TILEJSON_ZOOMS.min ||
      value > TILEJSON_ZOOMS.max
    ) {
      throw new Error(`${url} has ${name} ${JSON.stringify(value)?.slice(0, 40)}, not a zoom from 0 to 30`);
    }
    return value;
  };
  const minZoom = zoom('minzoom', TILEJSON_ZOOMS.min);
  const maxZoom = zoom('maxzoom', TILEJSON_ZOOMS.max);
  if (minZoom > maxZoom) {
    throw new Error(`${url} has minzoom ${minZoom} above its maxzoom ${maxZoom}`);
  }
  return { template, minZoom, maxZoom };
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
    // A redirect would carry the token to wherever it points: it fails like any answer but 202.
    redirect: 'manual',
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
 * other test with a verdict (testHasVerdict), how many tests had one, and the probe continents they came from.
 */
export function measurementSamples(
  target: Target,
  measurement: Measurement | undefined,
): { samples: Sample[]; lines: LogFields[]; tests: number; verdicts: number; continents: Set<string> } {
  const samples: Sample[] = [];
  const lines: LogFields[] = [];
  const continents = new Set<string>();
  let verdicts = 0;
  const results = measurement?.results ?? [];
  for (const { probe, result } of results) {
    // target keeps the two measurements' samples apart when both report the same kind (see the module comment).
    const labels = {
      continent: continentLabel(probe?.continent),
      kind: measuredKind(target, result),
      target: target.kind,
    };
    const ttfb = testTtfb(result);
    if (ttfb !== undefined) {
      samples.push({ name: 'tiles_bunny_probe_ttfb_ms', labels, value: ttfb });
    }
    const verdict = testHasVerdict(result);
    if (verdict) {
      verdicts++;
      continents.add(labels.continent);
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
  return { samples, lines, tests: results.length, verdicts, continents };
}

/**
 * Probes the hit and miss tiles, at the URLs the TileJSON advertises, from one probe per continent (the miss
 * measurement reuses the hit one's probes, so the two compare like for like), waits for the results and pushes them
 * with the TileJSON read's verdict (TILEJSON_LABELS) and the run's result at the scheduled minute. The run is an error
 * when the TileJSON gives no tile URL (its verdict then a 1, for the critical alert), Globalping refuses or doesn't
 * finish a measurement, or a measurement has no verdict from some continent (no probe there, or its test failed on
 * Globalping's side): that continent's tiles went unmeasured. Never rejects. Without config it pushes only
 * result="skipped".
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
  let targets: Target[];
  try {
    const live = await liveTiles(config.publicUrl);
    const tiles = probeTiles(live, deps.random);
    const hit = tileUrl(config.publicUrl, live.template, tiles.hit);
    targets = [
      { kind: 'hit', path: hit.pathname },
      { kind: 'miss', path: tileUrl(config.publicUrl, live.template, tiles.miss).pathname },
    ];
    if (hit.origin !== config.publicUrl.origin) {
      // Expected from a DNS cutover until the next sync rewrites the TileJSON (see tileUrl); anything else is worth a
      // look, as clients load the tiles from there.
      logger.warn('Probe TileJSON names tiles on another origin', { ...fields, tile_origin: hit.origin.slice(0, 200) });
    }
  } catch (error) {
    // Not the old prefix instead: after a full rewrite that is the retired build, deleted a week later. Clients can't
    // load the map either, so the read counts as a failed test (see the module comment), not only as a blind run.
    logger.error('Probe tile URLs unknown', error, fields);
    await pushSamples(
      o11y,
      [{ name: 'tiles_bunny_probe_errors', labels: TILEJSON_LABELS, value: 1 }, runs('error')],
      minute,
    );
    return 'error';
  }
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
  const samples: Sample[] = [{ name: 'tiles_bunny_probe_errors', labels: TILEJSON_LABELS, value: 0 }];
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
      const missing = PROBE_CONTINENTS.filter((continent) => !measured.continents.has(continent));
      if (settled(measurement) && measured.verdicts === 0) {
        // Every test failed on Globalping's side or its probes went offline: nothing about the tiles was measured.
        result = 'error';
        logger.error('Globalping measurement has no test with a verdict', undefined, {
          ...fields,
          target: target.kind,
          measurement_id: id,
          tests: measured.tests,
        });
      } else if (settled(measurement) && missing.length > 0) {
        // Globalping had no probe there, or its test went offline or failed on Globalping's side: the samples that
        // came are kept, but those continents' tiles went unmeasured.
        result = 'error';
        logger.error('Globalping measurement has no verdict from some continents', undefined, {
          ...fields,
          target: target.kind,
          measurement_id: id,
          tests: measured.tests,
          missing: missing.join(','),
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
