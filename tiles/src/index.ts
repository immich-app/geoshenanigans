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

/* eslint-disable no-var */
declare global {
  var memCache: Map<string, unknown>;
}
/* eslint-enable no-var */

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
    return { requestType: 'tile', version, z, x, y, url } as PMTilesTileParams;
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
  const d1Repository = new CloudflareD1Repository(env.D1_GLOBAL, metrics);
  const cacheResponse = async (response: Response): Promise<Response> => {
    if (!response.body) {
      throw new Error('Response body is undefined');
    }
    deferredRepository.runImmediately(cache.put(request.url, response.clone()));
    return response;
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
    let styleJson;
    if (env.ENVIRONMENT !== 'prod') {
      styleJson = (await new Response(
        await tigrisStorageRepository.getAsStream('styles/' + style + '.json'),
      ).json()) as StyleJson;
      styleJson.sources.vector.url = `${url.origin}/v${version}`;
      styleJson = new Response(JSON.stringify(styleJson)).body;
    } else {
      styleJson = await tigrisStorageRepository.getAsStream('styles/' + style + '.json');
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

  const cache = caches.default;
  const cached = await metrics.monitorAsyncFunction({ name: 'match_request_from_cdn' }, (url) => cache.match(url))(
    request.url,
  );
  if (cached && cached.headers.get(Header.PMTILES_DEPLOYMENT_KEY) === env.DEPLOYMENT_KEY) {
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

  const tigrisStorageRepository = new TigrisStorageRepository(
    env.TIGRIS_KEY_ID,
    env.TIGRIS_ACCESS_KEY,
    'tiles-geo',
    env.DEPLOYMENT_KEY,
    metrics,
  );
  const pmTilesService = await metrics.monitorAsyncFunction({ name: 'pmtiles_init' }, PMTilesService.init)(
    tigrisStorageRepository,
    memCacheRepository,
    metrics,
    d1Repository,
  );

  const respHeaders = new Headers();
  respHeaders.set(Header.CACHE_CONTROL, `public, max-age=${60 * 60 * 24 * 31}`);
  respHeaders.set(Header.ACCESS_CONTROL_ALLOW_ORIGIN, '*');
  respHeaders.set(Header.VARY, 'Origin');
  respHeaders.set(Header.PMTILES_DEPLOYMENT_KEY, env.DEPLOYMENT_KEY);

  try {
    if (pmTilesParams.requestType === 'tile') {
      const { z, x, y, version } = pmTilesParams as PMTilesTileParams;
      return await metrics.monitorAsyncFunction({ name: 'tile_request', tags: { z, version } }, handleTileRequest)(
        z,
        x,
        y,
        pmTilesService,
        respHeaders,
      );
    }

    if (pmTilesParams.requestType === 'json') {
      const { version } = pmTilesParams as PMTilesJsonParams;
      return await metrics.monitorAsyncFunction(
        { name: 'json_request', tags: { version } },
        handleJsonRequest,
      )(respHeaders);
    }

    if (pmTilesParams.requestType === 'style') {
      const { style } = pmTilesParams as PMTilesStyleParams;
      console.log('style');
      return await metrics.monitorAsyncFunction(
        { name: 'style_request', tags: { style } },
        handleStyleRequest,
      )(respHeaders);
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
      // const metrics = new CloudflareMetricsRepository('tiles', request, []);
      // const body = (await request.json()) as { sql: string; db: string };
      // const d1Repository = new CloudflareD1Repository(
      //   workerEnv[`D1_${body.db}` as unknown as keyof WorkerEnv] as D1Database,
      //   metrics,
      // );
      // console.log('D1 Proxy', body.sql);
      // const response = await d1Repository.query(body.sql);
      // console.log('D1 Response', response);
      // return new Response(JSON.stringify(response));
      return new Response('get out of here');
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
