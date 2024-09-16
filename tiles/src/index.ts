import { preferredBuckets, R2BucketRegion } from './buckets';
import { IMetricsRepository } from './interface';
import { PMTilesService } from './pmtiles/pmtiles.service';
import {
  CloudflareDeferredRepository,
  CloudflareKVRepository,
  CloudflareMetricsRepository,
  HeaderMetricsProvider,
  InfluxMetricsProvider,
  MemCacheRepository,
  R2StorageRepository,
} from './repository';

/* eslint-disable no-var */
declare global {
  var memCache: Map<string, unknown>;
}
/* eslint-enable no-var */

const URL_MATCHER = /^\/v(?<VERSION>[0-9]+)((?=)|(?<JSON>\.json)|\/(?<Z>\d+)\/(?<X>\d+)\/(?<Y>\d+).mvt)$/;

type PMTilesParams = {
  requestType: 'tile' | 'json' | undefined;
  url: URL;
};

type PMTilesTileParams = PMTilesParams & {
  requestType: 'tile';
  version: string;
  z: string;
  x: string;
  y: string;
};

type PMTilesJsonParams = PMTilesParams & {
  requestType: 'json';
  version: string;
};

enum Header {
  PMTILES_DEPLOYMENT_KEY = 'PMTiles-Deployment-Key',
  CACHE_CONTROL = 'Cache-Control',
  ACCESS_CONTROL_ALLOW_ORIGIN = 'Access-Control-Allow-Origin',
  VARY = 'Vary',
  CONTENT_TYPE = 'Content-Type',
  CONTENT_ENCODING = 'Content-Encoding',
  SERVER_TIMING = 'Server-Timing',
}

export function parseUrl(request: Request): PMTilesParams {
  const url = new URL(request.url);
  const matches = URL_MATCHER.exec(url.pathname);
  const version = matches?.groups?.VERSION;
  const z = matches?.groups?.Z;
  const x = matches?.groups?.X;
  const y = matches?.groups?.Y;
  if (version && z && x && y) {
    return { requestType: 'tile', version, z, x, y, url } as PMTilesTileParams;
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

  if (request.method.toUpperCase() !== 'GET') {
    return new Response(undefined, { status: 405 });
  }

  const cache = caches.default;
  const cached = await metrics.monitorAsyncFunction({ name: 'match_request_from_cdn' }, (url) => cache.match(url))(
    request.url,
  );
  if (cached && cached.headers.get(Header.PMTILES_DEPLOYMENT_KEY) === env.DEPLOYMENT_KEY) {
    const cacheHeaders = new Headers(cached.headers);
    const encodeBody = cacheHeaders.has('content-encoding') ? 'manual' : 'automatic';
    return new Response(cached.body, {
      headers: cacheHeaders,
      status: cached.status,
      encodeBody,
    });
  }

  if (!globalThis.memCache) {
    globalThis.memCache = new Map<string, unknown>();
  }

  const memCacheRepository = new MemCacheRepository(globalThis.memCache);
  const kvRepository = new CloudflareKVRepository(env.KV);
  const bucketMap: Record<R2BucketRegion, R2Bucket> = {
    apac: env.BUCKET_APAC,
    eeur: env.BUCKET_EEUR,
    enam: env.BUCKET_ENAM,
    wnam: env.BUCKET_WNAM,
    weur: env.BUCKET_WEUR,
  };
  const colo = request.cf?.colo || '';
  const buckets: R2BucketRegion[] = preferredBuckets[colo] || ['weur', 'eeur', 'enam', 'wnam', 'apac'];
  console.log('Buckets', buckets);
  const filteredBucketMap = Object.fromEntries(
    Object.entries(bucketMap).filter(([key]) => buckets.includes(key as R2BucketRegion)),
  );

  const storageRepository = new R2StorageRepository(filteredBucketMap, env.DEPLOYMENT_KEY, metrics);
  const pmTilesService = await metrics.monitorAsyncFunction({ name: 'pmtiles_init' }, PMTilesService.init)(
    storageRepository,
    memCacheRepository,
    kvRepository,
    metrics,
  );

  const respHeaders = new Headers();
  respHeaders.set(Header.CACHE_CONTROL, `public, max-age=${60 * 60 * 24 * 31}`);
  respHeaders.set(Header.ACCESS_CONTROL_ALLOW_ORIGIN, '*');
  respHeaders.set(Header.VARY, 'Origin');
  respHeaders.set(Header.PMTILES_DEPLOYMENT_KEY, env.DEPLOYMENT_KEY);

  const pmTilesParams = parseUrl(request);

  try {
    if (pmTilesParams.requestType === 'tile') {
      const { z, x, y, version } = pmTilesParams as PMTilesTileParams;
      return await metrics.monitorAsyncFunction(
        { name: 'tile_request', tags: { z, x, y, version } },
        handleTileRequest,
      )(z, x, y, pmTilesService, respHeaders);
    }

    if (pmTilesParams.requestType === 'json') {
      const { version } = pmTilesParams as PMTilesJsonParams;
      return await metrics.monitorAsyncFunction(
        { name: 'json_request', tags: { version } },
        handleJsonRequest,
      )(respHeaders);
    }
  } catch (e) {
    console.error(e);
    return new Response('Internal Server Error', { status: 500 });
  }

  return cacheResponse(new Response('Invalid URL', { headers: respHeaders, status: 404 }));
}

export default {
  async fetch(request, env, ctx): Promise<Response> {
    const workerEnv = env as WorkerEnv;
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
