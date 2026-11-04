import { handleD1ProxyRequest } from './d1-proxy';
import { IDeferredRepository, IMetricsRepository } from './interface';
import {
  CloudflareDeferredRepository,
  CloudflareMetricsRepository,
  HeaderMetricsProvider,
  InfluxMetricsProvider,
  Metric,
  requestTags,
  warmD1Replica,
} from './repository';
import {
  encodeMetricTags,
  Header,
  innerUrl,
  parseUrl,
  PMTilesParams,
  PMTilesStyleParams,
  PMTilesTileParams,
} from './request';
import { serveInner } from './tiles';

export { parseUrl } from './request';
export { Tiles } from './tiles';

const DAY_SECONDS = 60 * 60 * 24;
// For anything but a found response: a 404 or a failure is kept by no one, as without an explicit policy it is open to
// heuristic caching.
const NO_STORE = 'no-store';
// What points clients at a deployment (TileJSON, styles) is rechecked every few minutes, so a deploy reaches them
// quickly; an unchanged one costs a bodiless 304.
const POINTER_CACHE = 'public, max-age=300';

// HEAD is answered like GET, without the body, and OPTIONS is a CORS preflight.
const ALLOWED_METHODS = 'GET, HEAD, OPTIONS';

// Cache statuses of a response the cache answered from a stored copy, without running Tiles for this request.
// Requests that waited for another request's fill count as HIT too.
const FROM_CACHE = new Set(['hit', 'stale', 'updating']);

// How long the gateway waits for the cache's answer to a tile GET before also reading the tile itself (see raceTile),
// unless RACE_MS says otherwise.
const RACE_MS = 120;

/**
 * What answered a tile GET the gateway could race: the cache, before the race began ("none") or in it ("tier"), or the
 * gateway's own read ("local").
 */
export type Race = 'none' | 'tier' | 'local';

/** RACE_MS in milliseconds: 120 when unset, undefined (no race) when it is negative or not a number. */
export function raceDelay(value: string | undefined): number | undefined {
  const ms = value === undefined || value.trim() === '' ? RACE_MS : Number(value);
  return Number.isFinite(ms) && ms >= 0 ? ms : undefined;
}

export type CachePolicy = { cacheControl: string; etag?: string };

/**
 * How long clients keep a response. A tile URL with ?v=<live key> never changes, so it's immutable. Anything else
 * can change with the next deployment: it carries the live key as its ETag, so clients revalidate it cheaply (a 304
 * while the deployment is unchanged) once it goes stale.
 */
export function cachePolicy(params: PMTilesParams, liveDeploymentKey: string): CachePolicy {
  const etag = `"${liveDeploymentKey}"`;
  switch (params.requestType) {
    case 'tile': {
      const requested = (params as PMTilesTileParams).deploymentKey;
      if (requested === undefined) {
        // Shown straight from cache for up to 30 days, refreshed in the background once a day old.
        return { cacheControl: `public, max-age=${DAY_SECONDS}, stale-while-revalidate=${30 * DAY_SECONDS}`, etag };
      }
      // Another key comes from a client whose TileJSON predates a deploy: it gets the live tile, kept only until that
      // TileJSON is rechecked.
      return requested === liveDeploymentKey
        ? { cacheControl: `public, max-age=${365 * DAY_SECONDS}, immutable` }
        : { cacheControl: POINTER_CACHE };
    }
    case 'json':
    case 'style':
      return { cacheControl: POINTER_CACHE, etag };
    default:
      return { cacheControl: NO_STORE };
  }
}

/** Whether an If-None-Match header names the ETag, weak or strong ("*" is left to a full response). */
export function etagMatches(ifNoneMatch: string | null, etag: string): boolean {
  return (ifNoneMatch ?? '').split(',').some((tag) => tag.trim().replace(/^W\//, '') === etag);
}

/** Whether an Accept-Encoding header takes gzip, which tiles are stored in ("*" is left to the cache). */
export function acceptsGzip(acceptEncoding: string | null): boolean {
  return (acceptEncoding ?? '').split(',').some((coding) => {
    const [name, ...params] = coding.split(';').map((part) => part.trim().toLowerCase());
    return name === 'gzip' && !params.some((param) => /^q=0(\.0*)?$/.test(param));
  });
}

/** The cache_status metric tag: Cf-Cache-Status in lower case, "none" when the cache wasn't involved. */
export function cacheStatus(response: Response): string {
  return response.headers.get(Header.CF_CACHE_STATUS)?.toLowerCase() ?? 'none';
}

const cacheStatusTags = (response: Response) => ({ cache_status: cacheStatus(response) });
// The client's response: its cache status and, for a tile GET, what answered it.
const responseTags = (response: Response) => ({
  ...cacheStatusTags(response),
  race: response.headers.get(Header.RACE) ?? 'none',
});

/**
 * The gateway: runs on every request, uncached, at the colo the client reached. It asks the cached Tiles entrypoint
 * for the deployment's response and gives clients their cache policy, ETag and 304s, CORS and Server-Timing, so those
 * stay per request while the body comes from the cache.
 */
async function handleRequest(
  request: Request<unknown, IncomingRequestCfProperties>,
  env: WorkerEnv,
  ctx: ExecutionContext,
  metrics: IMetricsRepository,
  tags: { [key: string]: string },
  deferred: IDeferredRepository,
): Promise<Response> {
  const respHeaders = new Headers();
  respHeaders.set(Header.ACCESS_CONTROL_ALLOW_ORIGIN, '*');
  respHeaders.set(Header.PMTILES_DEPLOYMENT_KEY, env.DEPLOYMENT_KEY);

  const method = request.method.toUpperCase();
  if (method === 'OPTIONS') {
    respHeaders.set(Header.ACCESS_CONTROL_ALLOW_METHODS, ALLOWED_METHODS);
    return new Response(null, { headers: respHeaders, status: 204 });
  }
  if (method !== 'GET' && method !== 'HEAD') {
    respHeaders.set(Header.ALLOW, ALLOWED_METHODS);
    respHeaders.set(Header.CACHE_CONTROL, NO_STORE);
    return new Response(null, { headers: respHeaders, status: 405 });
  }
  const head = method === 'HEAD';

  const pmTilesParams = parseUrl(request);
  const requestType = pmTilesParams.requestType ?? 'unknown';
  const policy = cachePolicy(pmTilesParams, env.DEPLOYMENT_KEY);

  if (pmTilesParams.requestType === undefined) {
    // Including URLs of an API version that isn't published and tiles that can't exist, which Tiles never sees.
    respHeaders.set(Header.CACHE_CONTROL, NO_STORE);
    return new Response(head ? null : 'Not found', { headers: respHeaders, status: 404 });
  }

  const metricTags = { ...tags };
  if (pmTilesParams.requestType === 'tile') {
    metricTags.url = (pmTilesParams as PMTilesTileParams).deploymentKey ? 'versioned' : 'unversioned';
  }
  // Always a GET, HEAD included: the cache keeps one entry for both, and Tiles only ever sends whole responses.
  const innerRequest = new Request(innerUrl(pmTilesParams, env.DEPLOYMENT_KEY), {
    headers: {
      [Header.CONTINENT]: request.cf?.continent ?? '',
      [Header.METRIC_TAGS]: encodeMetricTags(metricTags),
      // The cache negotiates the body's encoding (tiles are stored gzipped) as it would for the client.
      [Header.ACCEPT_ENCODING]: request.headers.get(Header.ACCEPT_ENCODING) ?? '',
    },
  });
  // The cache's answer, timed as it comes. Its metrics are tagged with what answered the client, so they are pushed once
  // that is known (reportTier).
  const cacheFetch = Metric.create('tiles_cache_fetch').addTag('request_type', requestType).intField('invocation', 1);
  const tier = ctx.exports.Tiles.fetch(innerRequest).then(
    (response) => {
      cacheFetch.durationField('duration').addTags(cacheStatusTags(response));
      return response;
    },
    (error) => {
      console.error(error, 'cache_fetch_errors');
      cacheFetch.durationField('duration').intField('errors', 1);
      throw error;
    },
  );
  const reportTier = async (race: Race) => {
    const answer = await tier.catch(() => undefined);
    metrics.push(cacheFetch.addTag('race', race));
    const status = answer ? cacheStatus(answer) : 'none';
    if (FROM_CACHE.has(status)) {
      // Tiles reports its own runs (tiles_<type>_request_*), so together they count every request. A stored copy counts
      // even when the gateway's own read answered first, as Tiles didn't run for it.
      const metric = Metric.create('tiles_cdn_hit')
        .addTag('deployment_key', env.DEPLOYMENT_KEY)
        .addTag('request_type', requestType)
        .addTag('cache_status', status)
        .addTag('race', race)
        .intField('count', 1);
      if (pmTilesParams.requestType === 'tile') {
        metric.addTag('z', (pmTilesParams as PMTilesTileParams).z).addTag('url', metricTags.url);
      } else if (pmTilesParams.requestType === 'style') {
        metric.addTag('style', (pmTilesParams as PMTilesStyleParams).style);
      }
      metrics.push(metric);
    }
  };

  // A tile GET whose cached answer is late is also read here; never TileJSON, styles or HEAD, nor for a client that
  // doesn't take gzip, which a tile read here is in. Workers get Accept-Encoding as "br, gzip" whatever the client sent;
  // cf has what it did send when that differs.
  const clientEncoding = request.cf?.clientAcceptEncoding ?? request.headers.get(Header.ACCEPT_ENCODING);
  const raceMs =
    pmTilesParams.requestType === 'tile' && !head && acceptsGzip(clientEncoding) ? raceDelay(env.RACE_MS) : undefined;
  const { race, answer } =
    raceMs === undefined
      ? { race: 'none' as Race, answer: tier }
      : await raceTile(tier, innerRequest, request, env, metrics.scoped('tiles_local', metricTags), deferred, raceMs);
  if (raceMs !== undefined) {
    respHeaders.set(Header.RACE, race);
  }
  if (race === 'local') {
    // The cache answers after the response is out; the metrics are sent once it has.
    deferred.waitFor(reportTier(race));
  } else {
    await reportTier(race);
  }
  const response = await answer;

  // Only the body and its type come from Tiles; the edge's own headers (Cache-Control, Age) would mislead clients.
  for (const name of [Header.CONTENT_TYPE, Header.CONTENT_ENCODING]) {
    const value = response.headers.get(name);
    if (value !== null) {
      respHeaders.set(name, value);
    }
  }
  if (response.headers.has(Header.CF_CACHE_STATUS)) {
    respHeaders.set(Header.CF_CACHE_STATUS, response.headers.get(Header.CF_CACHE_STATUS)!);
  }
  // The response with Tiles' body, or for HEAD without it, though with the length it would have had.
  const reply = async (status: number) => {
    if (!head) {
      // A tile read here is as stored, gzipped, which the client takes; the cache's answer is encoded for the client.
      const encodeBody = race === 'local' ? 'manual' : 'automatic';
      return new Response(response.body, { headers: respHeaders, status, encodeBody });
    }
    await response.body?.cancel();
    const length = response.headers.get(Header.CONTENT_LENGTH);
    if (length !== null) {
      respHeaders.set(Header.CONTENT_LENGTH, length);
    }
    return new Response(null, { headers: respHeaders, status });
  };

  if (response.status !== 200) {
    // A missing tile or a failure isn't kept by clients, as it isn't by the edge.
    respHeaders.set(Header.CACHE_CONTROL, NO_STORE);
    return reply(response.status);
  }
  respHeaders.set(Header.CACHE_CONTROL, policy.cacheControl);
  if (policy.etag) {
    respHeaders.set(Header.ETAG, policy.etag);
  }

  // A response the client already has from the live deployment (its ETag) becomes a bodiless 304.
  if (policy.etag && etagMatches(request.headers.get(Header.IF_NONE_MATCH), policy.etag)) {
    await response.body?.cancel();
    metrics.push(
      Metric.create('tiles_not_modified')
        .addTag('deployment_key', env.DEPLOYMENT_KEY)
        .addTag('request_type', requestType)
        .intField('count', 1),
    );
    respHeaders.delete(Header.CONTENT_ENCODING);
    respHeaders.delete(Header.CONTENT_TYPE);
    return new Response(null, { status: 304, headers: respHeaders });
  }
  return reply(200);
}

/**
 * A tile GET's answer: the cache's, unless that takes longer than raceMs. Then the gateway also runs Tiles' miss path
 * itself (serveInner: its D1 lookup and Tigris read, from this colo) and serves whichever finds the tile first; the
 * cache's answer stands if neither does, a failure included. A cached answer that loses is read to its end in the
 * background, so its fill completes; a local one is cancelled. Never rejects: a failure is in the answer.
 */
async function raceTile(
  tier: Promise<Response>,
  innerRequest: Request,
  request: Request<unknown, IncomingRequestCfProperties>,
  env: WorkerEnv,
  localMetrics: IMetricsRepository,
  deferred: IDeferredRepository,
  raceMs: number,
): Promise<{ race: Race; answer: Promise<Response> }> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const late = new Promise<undefined>((resolve) => {
    timer = setTimeout(() => resolve(undefined), raceMs);
  });
  try {
    if (await Promise.race([tier, late])) {
      return { race: 'none', answer: tier };
    }
  } catch {
    // The cache failed before the race began, which fails the request as it would without the race.
    return { race: 'none', answer: tier };
  } finally {
    clearTimeout(timer);
  }

  // Tiles' inner request, with a Cf-Ray naming this colo, which Tiles tags its D1 metrics with.
  const headers = new Headers(innerRequest.headers);
  headers.set(Header.CF_RAY, `${request.headers.get(Header.CF_RAY) ?? 'gateway'}-${request.cf?.colo ?? ''}`);
  const local = serveInner(new Request(innerRequest.url, { headers }), env, localMetrics);
  // Its metrics are pushed when it answers, which may be after the response is out.
  deferred.waitFor(local);

  // Only a 200 wins: a 404 or a failure on one side waits for the other.
  try {
    const winner = await Promise.any([
      tier.then((response) => {
        if (response.status !== 200) {
          throw new Error(`Tiles answered ${response.status}`);
        }
        return { race: 'tier' as Race, response };
      }),
      local.then(async (response) => {
        if (response.status !== 200) {
          await response.body?.cancel();
          throw new Error(`The local read answered ${response.status}`);
        }
        return { race: 'local' as Race, response };
      }),
    ]);
    deferred.runImmediately(
      (winner.race === 'local'
        ? // The cache's fill completes only if its answer is read to the end, so it is never cancelled.
          tier.then((response) => response.body?.pipeTo(new WritableStream()))
        : local.then((response) => response.body?.cancel())
      ).catch(() => {}),
    );
    return { race: winner.race, answer: Promise.resolve(winner.response) };
  } catch {
    // Neither found the tile.
    return { race: 'tier', answer: tier };
  }
}

export default {
  async fetch(request, env, ctx): Promise<Response> {
    const workerEnv = env as WorkerEnv;
    if (workerEnv.WORKER_TYPE === 'D1_PROXY') {
      return handleD1ProxyRequest(request, workerEnv);
    }
    // Tiles' lookups run at the upper-tier colo of this colo's regional pool, which reads from the same nearby D1
    // replica. Warmed on every request, hits included, as those are what keep it awake between misses; never awaited,
    // so no response waits for it.
    ctx.waitUntil(warmD1Replica(workerEnv.D1_GLOBAL));
    const deferredRepository = new CloudflareDeferredRepository(ctx);
    const headerProvider = new HeaderMetricsProvider();
    const influxProvider = new InfluxMetricsProvider(workerEnv.VMETRICS_API_TOKEN, env.ENVIRONMENT);
    deferredRepository.defer(() => influxProvider.flush());
    const tags = requestTags(request);
    const metrics = new CloudflareMetricsRepository('tiles', tags, [influxProvider, headerProvider]);

    try {
      const response = await metrics.monitorAsyncFunction({ name: 'handle_request' }, handleRequest, {
        resultTags: responseTags,
      })(request, workerEnv, ctx, metrics, tags, deferredRepository);
      response.headers.set(Header.SERVER_TIMING, headerProvider.getTimingHeader());
      return response;
    } catch (e) {
      console.error(e);
      return new Response('Internal Server Error', { status: 500 });
    } finally {
      deferredRepository.runDeferred();
    }
  },
} satisfies ExportedHandler<Env>;
