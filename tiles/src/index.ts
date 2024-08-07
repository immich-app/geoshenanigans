import { Metrics } from './monitor';
import { PMTiles } from './pmtiles/pmtiles';
import { Header } from './pmtiles/types';
import { tileJSON } from './pmtiles/utils';
import { R2Source } from './r2';

const URL_MATCHER = /^\/v(?<VERSION>[0-9]+)((?=)|(?<JSON>\.json)|\/(?<Z>\d+)\/(?<X>\d+)\/(?<Y>\d+).mvt)$/;

/* eslint-disable no-var */
declare global {
  var headerCache: Map<string, Header>;
  var source: R2Source;
}
/* eslint-enable no-var */

if (!globalThis.headerCache) {
  globalThis.headerCache = new Map<string, Header>();
}

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

async function handleRequest(request: Request<unknown, IncomingRequestCfProperties>, env: Env, ctx: ExecutionContext) {
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
    return cacheResponse(new Response(tile.data, { headers: respHeaders, status: 200, encodeBody: 'manual' }));
  };

  async function handleJsonRequest() {
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

  const pmTiles = await metrics.monitorAsyncFunction({ name: 'pmtiles_init' }, PMTiles.init)(
    source,
    globalThis.headerCache,
    env.KV,
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
      return await metrics.monitorAsyncFunction({ name: 'json_request', extraTags: { version } }, handleJsonRequest)();
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
    if (!globalThis.source) {
      globalThis.source = new R2Source(env);
    }

    return metrics.monitorAsyncFunction({ name: 'handle_request' }, handleRequest)(request, env, ctx);
  },
} satisfies ExportedHandler<Env>;
