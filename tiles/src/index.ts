import { handleD1ProxyRequest } from './d1-proxy';
import { IMetricsRepository } from './interface';
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

/** The cache_status metric tag: Cf-Cache-Status in lower case, "none" when the cache wasn't involved. */
export function cacheStatus(response: Response): string {
  return response.headers.get(Header.CF_CACHE_STATUS)?.toLowerCase() ?? 'none';
}

const cacheStatusTags = (response: Response) => ({ cache_status: cacheStatus(response) });

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
  const response = await metrics.monitorAsyncFunction(
    { name: 'cache_fetch', tags: { request_type: requestType } },
    (inner: Request) => ctx.exports.Tiles.fetch(inner),
    { resultTags: cacheStatusTags },
  )(innerRequest);

  const status = cacheStatus(response);
  if (FROM_CACHE.has(status)) {
    // Tiles reports its own runs (tiles_<type>_request_*), so together they count every request.
    const metric = Metric.create('tiles_cdn_hit')
      .addTag('deployment_key', env.DEPLOYMENT_KEY)
      .addTag('request_type', requestType)
      .addTag('cache_status', status)
      .intField('count', 1);
    if (pmTilesParams.requestType === 'tile') {
      metric.addTag('z', (pmTilesParams as PMTilesTileParams).z).addTag('url', metricTags.url);
    } else if (pmTilesParams.requestType === 'style') {
      metric.addTag('style', (pmTilesParams as PMTilesStyleParams).style);
    }
    metrics.push(metric);
  }

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
      return new Response(response.body, { headers: respHeaders, status });
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
        resultTags: cacheStatusTags,
      })(request, workerEnv, ctx, metrics, tags);
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
