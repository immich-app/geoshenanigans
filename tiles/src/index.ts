import { Metrics } from './monitor';
import { PMTiles } from './pmtiles/pmtiles';
import { tileJSON } from './pmtiles/utils';
import { CloudflareKVRepository, MemCacheRepository, R2StorageRepository } from './repository';

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

export function parseUrl(request: Request): PMTilesParams {
  const url = new URL(request.url);
  const version = URL_MATCHER.exec(url.pathname)?.groups?.VERSION;
  const z = URL_MATCHER.exec(url.pathname)?.groups?.Z;
  const x = URL_MATCHER.exec(url.pathname)?.groups?.X;
  const y = URL_MATCHER.exec(url.pathname)?.groups?.Y;
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
  ctx: ExecutionContext,
) {
  const metrics = Metrics.getMetrics();
  const cacheResponse = async (response: Response): Promise<Response> => {
    if (!response.body) throw new Error('Response body is undefined');
    const responseBody = await response.arrayBuffer();
    ctx.waitUntil(cache.put(request.url, new Response(responseBody, response)));
    return new Response(responseBody, response);
  };

  const handleTileRequest = async (z: string, x: string, y: string, pmTiles: PMTiles, respHeaders: Headers) => {
    const tile = await pmTiles.getTile(+z, +x, +y);
    if (!tile) return new Response('Tile not found', { status: 404 });
    respHeaders.set('Content-Type', 'application/x-protobuf');
    respHeaders.set('content-encoding', 'gzip');
    return cacheResponse(new Response(tile, { headers: respHeaders, status: 200, encodeBody: 'manual' }));
  };

  async function handleJsonRequest(respHeaders: Headers) {
    const { version, url } = pmTilesParams as PMTilesJsonParams;
    const header = pmTiles.getHeader();
    const metadata = await pmTiles.getMetadata();
    respHeaders.set('Content-Type', 'application/json');
    const tileJson = tileJSON({ header, metadata, hostname: url.hostname, version });
    return cacheResponse(new Response(JSON.stringify(tileJson), { headers: respHeaders, status: 200 }));
  }

  if (request.method.toUpperCase() !== 'GET') {
    return new Response(undefined, { status: 405 });
  }

  const cache = caches.default;
  const cached = await metrics.monitorAsyncFunction({ name: 'match_request_from_cdn' }, (url) => cache.match(url))(
    request.url,
  );
  if (cached) {
    const cacheHeaders = new Headers(cached.headers);
    const encodeBody = cacheHeaders.has('content-encoding') ? 'manual' : 'automatic';
    return new Response(cached.body, {
      headers: cacheHeaders,
      status: cached.status,
      encodeBody,
    });
  }

  const memCacheRepository = new MemCacheRepository(globalThis.memCache);
  const kvRepository = new CloudflareKVRepository(env.KV);
  const storageRepository = new R2StorageRepository(env.BUCKET, env.PMTILES_FILE_NAME);

  const pmTiles = await metrics.monitorAsyncFunction({ name: 'pmtiles_init' }, PMTiles.init)(
    storageRepository,
    memCacheRepository,
    kvRepository,
    ctx,
  );

  const respHeaders = new Headers();
  respHeaders.set('Cache-Control', `public, max-age=${60 * 60 * 24 * 31}`);
  respHeaders.set('Access-Control-Allow-Origin', '*');
  respHeaders.set('Vary', 'Origin');
  respHeaders.set('PMTiles-File-Identifier', env.PMTILES_FILE_HASH);

  const pmTilesParams = parseUrl(request);

  try {
    if (pmTilesParams.requestType === 'tile') {
      const { z, x, y, version } = pmTilesParams as PMTilesTileParams;
      return await metrics.monitorAsyncFunction(
        { name: 'tile_request', extraTags: { z, x, y, version } },
        handleTileRequest,
      )(z, x, y, pmTiles, respHeaders);
    } else if (pmTilesParams.requestType === 'json') {
      const { version } = pmTilesParams as PMTilesJsonParams;
      return await metrics.monitorAsyncFunction(
        { name: 'json_request', extraTags: { version } },
        handleJsonRequest,
      )(respHeaders);
    }
  } catch (e) {
    console.error(e);
    return new Response('Internal Server Error', { status: 500 });
  }

  return cacheResponse(new Response('Invalid URL', { status: 404 }));
}

export default {
  async fetch(request, env, ctx): Promise<Response> {
    const metrics = Metrics.initialiseMetrics('tiles', request, ctx, env);

    return metrics.monitorAsyncFunction({ name: 'handle_request' }, handleRequest)(request, env, ctx);
  },
} satisfies ExportedHandler<Env>;
