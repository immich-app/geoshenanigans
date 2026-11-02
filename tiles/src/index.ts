import { handleD1ProxyRequest } from './d1-proxy';
import { IMetricsRepository } from './interface';
import { PMTilesService } from './pmtiles.service';
import {
  CloudflareD1Repository,
  CloudflareDeferredRepository,
  CloudflareMetricsRepository,
  HeaderMetricsProvider,
  InfluxMetricsProvider,
  MemCacheRepository,
  Metric,
  TigrisStorageRepository,
} from './repository';

declare global {
  var memCache: Map<string, unknown>;
}

const URL_MATCHER =
  /^\/v(?<VERSION>[0-9]+)((?=)|\/style\/(?<STYLE>\w+)(?:\.json)?|(?<JSON>\.json)|\/(?<Z>\d+)\/(?<X>\d+)\/(?<Y>\d+).mvt)$/;

type PMTilesParams = {
  requestType: 'tile' | 'json' | 'style' | undefined;
  url: URL;
};

type PMTilesStyleParams = PMTilesParams & {
  requestType: 'style';
  version: string;
  style: string;
};

type PMTilesTileParams = PMTilesParams & {
  requestType: 'tile';
  version: string;
  z: string;
  x: string;
  y: string;
  // From ?v=, which the TileJSON adds so each deployment's tiles have their own URLs.
  deploymentKey?: string;
};

type PMTilesJsonParams = PMTilesParams & {
  requestType: 'json';
  version: string;
};

type StyleJson = {
  version: number;
  name: string;
  id: string;
  sources: Record<string, { type: string; url: string }>;
  layers: unknown[];
  sprite: string;
  glyphs: string;
};

enum Header {
  PMTILES_DEPLOYMENT_KEY = 'PMTiles-Deployment-Key',
  CACHE_CONTROL = 'Cache-Control',
  ACCESS_CONTROL_ALLOW_ORIGIN = 'Access-Control-Allow-Origin',
  VARY = 'Vary',
  CONTENT_TYPE = 'Content-Type',
  CONTENT_ENCODING = 'Content-Encoding',
  SERVER_TIMING = 'Server-Timing',
  ETAG = 'ETag',
  IF_NONE_MATCH = 'If-None-Match',
  CACHE_REVISION = 'Tiles-Cache-Revision',
}

// Bump when what the worker sends for the same URL and deployment changes (e.g. headers or the TileJSON), so the
// CDN cache stops returning responses stored before that change.
const CACHE_REVISION = '2';

const DAY_SECONDS = 60 * 60 * 24;

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
      // Another key comes from a client whose TileJSON predates a deploy: it gets the live tile, kept only briefly.
      return requested === liveDeploymentKey
        ? { cacheControl: `public, max-age=${365 * DAY_SECONDS}, immutable` }
        : { cacheControl: 'public, max-age=3600' };
    }
    case 'json':
    case 'style':
      return { cacheControl: 'public, max-age=3600', etag };
    default:
      return { cacheControl: `public, max-age=${31 * DAY_SECONDS}` };
  }
}

/** Whether an If-None-Match header names the ETag, weak or strong ("*" is left to a full response). */
export function etagMatches(ifNoneMatch: string | null, etag: string): boolean {
  return (ifNoneMatch ?? '').split(',').some((tag) => tag.trim().replace(/^W\//, '') === etag);
}

export function parseUrl(request: Request): PMTilesParams {
  const url = new URL(request.url);
  const matches = URL_MATCHER.exec(url.pathname);
  const version = matches?.groups?.VERSION;
  const style = matches?.groups?.STYLE;
  const z = matches?.groups?.Z;
  const x = matches?.groups?.X;
  const y = matches?.groups?.Y;
  if (version && z && x && y) {
    const deploymentKey = url.searchParams.get('v');
    return {
      requestType: 'tile',
      version,
      z,
      x,
      y,
      url,
      ...(deploymentKey && /^\d{8}$/.test(deploymentKey) && { deploymentKey }),
    } as PMTilesTileParams;
  } else if (style) {
    return { requestType: 'style', style, url, version } as PMTilesStyleParams;
  } else if (version) {
    return { requestType: 'json', version, url } as PMTilesJsonParams;
  }
  return { requestType: undefined, url };
}

async function handleRequest(
  request: Request<unknown, IncomingRequestCfProperties>,
  env: WorkerEnv,
  deferredRepository: CloudflareDeferredRepository,
  metrics: IMetricsRepository,
) {
  const cacheResponse = async (response: Response): Promise<Response> => {
    if (!response.body) {
      throw new Error('Response body is undefined');
    }
    const responseBody = await response.arrayBuffer();
    deferredRepository.defer(() => cache.put(request.url, new Response(responseBody, response)));
    return new Response(responseBody, response);
  };

  const handleTileRequest = async (z: string, x: string, y: string, pmTiles: PMTilesService, respHeaders: Headers) => {
    const tile = await pmTiles.getTile(+z, +x, +y);
    if (!tile) {
      return new Response('Tile not found', { status: 404 });
    }
    respHeaders.set(Header.CONTENT_TYPE, 'application/x-protobuf');
    respHeaders.set(Header.CONTENT_ENCODING, 'gzip');
    return cacheResponse(new Response(tile, { headers: respHeaders, status: 200, encodeBody: 'manual' }));
  };

  async function handleJsonRequest(respHeaders: Headers) {
    const { version, url } = pmTilesParams as PMTilesJsonParams;
    const tileJson = await pmTilesService.getJsonResponse(version, url);
    respHeaders.set(Header.CONTENT_TYPE, 'application/json');
    return cacheResponse(new Response(JSON.stringify(tileJson), { headers: respHeaders, status: 200 }));
  }

  async function handleStyleRequest(respHeaders: Headers) {
    const { style, url, version } = pmTilesParams as PMTilesStyleParams;
    let styleJson = await tigrisStorageRepository.getAsStream('styles/' + style + '.json');
    if (env.ENVIRONMENT !== 'prod') {
      const style = (await new Response(styleJson).json()) as StyleJson;
      style.sources.vector.url = `${url.origin}/v${version}`;
      styleJson = new Response(JSON.stringify(style)).body!;
    }
    if (!styleJson) {
      return cacheResponse(new Response('Style not found', { status: 404 }));
    }
    respHeaders.set(Header.CONTENT_TYPE, 'application/json');
    return cacheResponse(new Response(styleJson, { headers: respHeaders, status: 200 }));
  }

  if (request.method.toUpperCase() !== 'GET') {
    return new Response(undefined, { status: 405 });
  }

  const pmTilesParams = parseUrl(request);

  const policy = cachePolicy(pmTilesParams, env.DEPLOYMENT_KEY);
  const respHeaders = new Headers();
  respHeaders.set(Header.CACHE_CONTROL, policy.cacheControl);
  if (policy.etag) {
    respHeaders.set(Header.ETAG, policy.etag);
  }
  respHeaders.set(Header.ACCESS_CONTROL_ALLOW_ORIGIN, '*');
  respHeaders.set(Header.VARY, 'Origin');
  respHeaders.set(Header.PMTILES_DEPLOYMENT_KEY, env.DEPLOYMENT_KEY);
  respHeaders.set(Header.CACHE_REVISION, CACHE_REVISION);

  // A successful response the client already has from the live deployment (its ETag) becomes a bodiless 304.
  const conditional = (response: Response): Response => {
    if (
      !policy.etag ||
      response.status !== 200 ||
      !etagMatches(request.headers.get(Header.IF_NONE_MATCH), policy.etag)
    ) {
      return response;
    }
    void response.body?.cancel();
    metrics.push(
      Metric.create('tiles_not_modified')
        .addTag('deployment_key', env.DEPLOYMENT_KEY)
        .addTag('request_type', pmTilesParams.requestType ?? 'unknown')
        .intField('count', 1),
    );
    const headers = new Headers(response.headers);
    headers.delete(Header.CONTENT_ENCODING);
    headers.delete(Header.CONTENT_TYPE);
    headers.delete('Content-Length');
    return new Response(null, { status: 304, headers });
  };

  const cache = caches.default;
  const cached = await metrics.monitorAsyncFunction({ name: 'match_request_from_cdn' }, (url) => cache.match(url))(
    request.url,
  );
  if (
    cached &&
    cached.headers.get(Header.PMTILES_DEPLOYMENT_KEY) === env.DEPLOYMENT_KEY &&
    cached.headers.get(Header.CACHE_REVISION) === CACHE_REVISION
  ) {
    const metric = Metric.create('tiles_cdn_hit')
      .addTag('deployment_key', env.DEPLOYMENT_KEY)
      .addTag('request_type', pmTilesParams.requestType ?? 'unknown')
      .intField('count', 1);
    if (pmTilesParams.requestType === 'tile') {
      metric.addTag('z', (pmTilesParams as PMTilesTileParams).z);
    } else if (pmTilesParams.requestType === 'style') {
      metric.addTag('style', (pmTilesParams as PMTilesStyleParams).style);
    }
    metrics.push(metric);
    const cacheHeaders = new Headers(cached.headers);
    const encodeBody = cacheHeaders.has('content-encoding') ? 'manual' : 'automatic';
    return conditional(new Response(cached.body, { headers: cacheHeaders, status: cached.status, encodeBody }));
  }

  if (!globalThis.memCache) {
    globalThis.memCache = new Map<string, unknown>();
  }

  const memCacheRepository = new MemCacheRepository(globalThis.memCache);
  const d1Repository = new CloudflareD1Repository(env.D1_GLOBAL, metrics);

  const tigrisStorageRepository = new TigrisStorageRepository(
    env.TIGRIS_KEY_ID,
    env.TIGRIS_ACCESS_KEY,
    'tiles-geo',
    env.DEPLOYMENT_KEY,
    metrics,
    env.STORAGE_PREFIX,
  );
  const pmTilesService = await metrics.monitorAsyncFunction({ name: 'pmtiles_init' }, PMTilesService.init)(
    tigrisStorageRepository,
    memCacheRepository,
    metrics,
    d1Repository,
  );

  try {
    if (pmTilesParams.requestType === 'tile') {
      const { z, x, y, version, deploymentKey } = pmTilesParams as PMTilesTileParams;
      const tags = { z, version, url: deploymentKey ? 'versioned' : 'unversioned' };
      return conditional(
        await metrics.monitorAsyncFunction({ name: 'tile_request', tags }, handleTileRequest)(
          z,
          x,
          y,
          pmTilesService,
          respHeaders,
        ),
      );
    }

    if (pmTilesParams.requestType === 'json') {
      const { version } = pmTilesParams as PMTilesJsonParams;
      return conditional(
        await metrics.monitorAsyncFunction({ name: 'json_request', tags: { version } }, handleJsonRequest)(respHeaders),
      );
    }

    if (pmTilesParams.requestType === 'style') {
      const { style } = pmTilesParams as PMTilesStyleParams;
      console.log('style');
      return conditional(
        await metrics.monitorAsyncFunction({ name: 'style_request', tags: { style } }, handleStyleRequest)(respHeaders),
      );
    }
  } catch (e: any) {
    console.error(e);
    metrics.push(Metric.create('error').addTag('error', e.message ?? 'unknown'));
    return new Response('Internal Server Error', { status: 500 });
  }

  return cacheResponse(new Response('Invalid URL', { headers: respHeaders, status: 404 }));
}

export default {
  async fetch(request, env, ctx): Promise<Response> {
    const workerEnv = env as WorkerEnv;
    if (workerEnv.WORKER_TYPE === 'D1_PROXY') {
      return handleD1ProxyRequest(request, workerEnv);
    }
    const deferredRepository = new CloudflareDeferredRepository(ctx);
    const headerProvider = new HeaderMetricsProvider();
    const influxProvider = new InfluxMetricsProvider(workerEnv.VMETRICS_API_TOKEN, env.ENVIRONMENT);
    deferredRepository.defer(() => influxProvider.flush());
    const metrics = new CloudflareMetricsRepository('tiles', request, [influxProvider, headerProvider]);

    try {
      const response = await metrics.monitorAsyncFunction({ name: 'handle_request' }, handleRequest)(
        request,
        workerEnv,
        deferredRepository,
        metrics,
      );
      response.headers.set(Header.SERVER_TIMING, headerProvider.getTimingHeader());
      deferredRepository.runDeferred();
      return response;
    } catch (e) {
      console.error(e);
      return new Response('Internal Server Error', { status: 500 });
    }
  },
} satisfies ExportedHandler<Env>;
