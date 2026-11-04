import { WorkerEntrypoint } from 'cloudflare:workers';
import { IMetricsRepository } from './interface';
import { PMTilesService } from './pmtiles.service';
import { JsonResponse } from './pmtiles/types';
import {
  CloudflareD1Repository,
  CloudflareMetricsRepository,
  InfluxMetricsProvider,
  MemCacheRepository,
  Metric,
  tigrisEndpoint,
  TigrisStorageRepository,
} from './repository';
import {
  decodeMetricTags,
  Header,
  parseInnerUrl,
  PMTilesJsonParams,
  PMTilesParams,
  PMTilesStyleParams,
  PMTilesTileParams,
  rayColo,
} from './request';

declare global {
  var memCache: Map<string, unknown>;
}

// A MapLibre vector source: a link to its TileJSON (url), or the fields MapLibre would read from it.
type VectorSource = { type: string; url?: string } & Partial<
  Pick<JsonResponse, 'tiles' | 'minzoom' | 'maxzoom' | 'bounds' | 'attribution'>
>;

export type StyleJson = {
  version: number;
  name: string;
  id: string;
  sources: Record<string, VectorSource>;
  layers: unknown[];
  sprite: string;
  glyphs: string;
};

// A URL names one deployment's response, which never changes, so the edge keeps it until it's evicted or purged.
// Clients get their own policy from the gateway.
export const EDGE_CACHE = 'public, max-age=31536000, immutable';
// Anything but a 200 might be a passing failure (a missing tile is also what a failed D1 lookup looks like), so it is
// never stored.
const NO_STORE = 'no-store';

/**
 * The style with its tile source, named "vector" by map-styles, inlined: the link to the TileJSON gives way to the
 * fields MapLibre would read there, so clients skip that round trip. The tiles template is the TileJSON's, with the
 * live deployment key, which the style's cache URL and ETag carry too: a data deploy gets clients a style with the new
 * key.
 */
export function inlineTileSource(style: StyleJson, tileJson: JsonResponse): StyleJson {
  if (style.sources?.vector?.type !== 'vector') {
    throw new Error('Style has no "vector" tile source');
  }
  const { tiles, minzoom, maxzoom, bounds, attribution } = tileJson;
  const vector: VectorSource = { ...style.sources.vector, tiles, minzoom, maxzoom, bounds, attribution };
  delete vector.url;
  return { ...style, sources: { ...style.sources, vector } };
}

/** Cache-Tag values a response can be purged by: everything, one deployment, or one type of response. */
export function cacheTags(deploymentKey: string, requestType: PMTilesParams['requestType']): string {
  return `tiles,deploy-${deploymentKey},type-${requestType}`;
}

/**
 * The cached entrypoint, called only by the gateway through ctx.exports with an inner URL (see innerUrl). Runs on
 * cache misses, at the upper-tier colo that fills the response, and sets the edge cache policy. It sends no Vary
 * (every Origin would get its own copy) and no Server-Timing (it would be stored with the response).
 */
export class Tiles extends WorkerEntrypoint<WorkerEnv> {
  async fetch(request: Request): Promise<Response> {
    const influxProvider = new InfluxMetricsProvider(this.env.VMETRICS_API_TOKEN, this.env.ENVIRONMENT);
    const metrics = new CloudflareMetricsRepository(
      'tiles',
      decodeMetricTags(request.headers.get(Header.METRIC_TAGS)),
      [influxProvider],
    );
    try {
      return await serveInner(request, this.env, metrics);
    } finally {
      this.ctx.waitUntil(influxProvider.flush());
    }
  }

  /** Drops cached responses by Cache-Tag (see cacheTags). Only reachable through ctx.exports. */
  async purge(tags: string[]): Promise<CachePurgeResult> {
    if (!this.ctx.cache) {
      return {
        success: false,
        errors: [{ code: 0, message: 'Workers Cache is not enabled for the Tiles entrypoint' }],
      };
    }
    return this.ctx.cache.purge({ tags });
  }
}

/**
 * Tiles' answer to an inner request, from D1 and Tigris: the miss path. The gateway runs it too, in its own colo, for a
 * tile whose cached answer is late (see raceTile in index.ts).
 */
export async function serveInner(request: Request, env: WorkerEnv, metrics: IMetricsRepository): Promise<Response> {
  const { deploymentKey, params } = parseInnerUrl(request.url);
  // The gateway only asks for the deployment it serves; another key would fill the cache from data it doesn't name.
  if (params.requestType === undefined || deploymentKey !== env.DEPLOYMENT_KEY) {
    return new Response('Not found', { status: 404, headers: { [Header.CACHE_CONTROL]: NO_STORE } });
  }

  const respHeaders = new Headers();
  respHeaders.set(Header.CACHE_CONTROL, EDGE_CACHE);
  respHeaders.set(Header.CACHE_TAG, cacheTags(deploymentKey, params.requestType));
  respHeaders.set(Header.PMTILES_DEPLOYMENT_KEY, deploymentKey);

  const uncached = (body: string, status: number) =>
    new Response(body, { status, headers: { [Header.CACHE_CONTROL]: NO_STORE } });

  if (!globalThis.memCache) {
    globalThis.memCache = new Map<string, unknown>();
  }
  const memCacheRepository = new MemCacheRepository(globalThis.memCache);
  // D1's latency depends on the replica nearest to where Tiles runs: on a full miss the upper tier's colo, not the
  // client's (the colo tag).
  const d1Tags = { tiles_colo: rayColo(request.headers.get(Header.CF_RAY)) };
  const d1Repository = new CloudflareD1Repository(env.D1_GLOBAL, metrics, d1Tags);
  const tigrisStorageRepository = new TigrisStorageRepository(
    env.TIGRIS_KEY_ID,
    env.TIGRIS_ACCESS_KEY,
    'tiles-geo',
    deploymentKey,
    metrics,
    env.STORAGE_PREFIX,
    tigrisEndpoint(request.headers.get(Header.CONTINENT)),
  );

  const handleTileRequest = async (z: string, x: string, y: string, pmTiles: PMTilesService) => {
    const tile = await pmTiles.getTile(+z, +x, +y);
    if (!tile) {
      return uncached('Tile not found', 404);
    }
    respHeaders.set(Header.CONTENT_TYPE, 'application/x-protobuf');
    respHeaders.set(Header.CONTENT_ENCODING, 'gzip');
    return new Response(tile, { headers: respHeaders, status: 200, encodeBody: 'manual' });
  };

  const handleJsonRequest = async (pmTiles: PMTilesService) => {
    const { version } = params as PMTilesJsonParams;
    const tileJson = await pmTiles.getJsonResponse(version, env.PUBLIC_URL);
    respHeaders.set(Header.CONTENT_TYPE, 'application/json');
    return new Response(JSON.stringify(tileJson), { headers: respHeaders, status: 200 });
  };

  const handleStyleRequest = async (pmTiles: PMTilesService) => {
    const { style, version } = params as PMTilesStyleParams;
    // Style names aren't known up front: they are whatever the deployment has in the bucket.
    const stored = await tigrisStorageRepository.getAsStream('styles/' + style + '.json');
    if (!stored) {
      return uncached('Style not found', 404);
    }
    // The one body that is parsed rather than streamed, to inline the tile source. Styles are tens of kilobytes, and
    // this runs only when the cache has no copy for the deployment.
    const styleJson = inlineTileSource(
      (await new Response(stored).json()) as StyleJson,
      await pmTiles.getJsonResponse(version, env.PUBLIC_URL),
    );
    respHeaders.set(Header.CONTENT_TYPE, 'application/json');
    return new Response(JSON.stringify(styleJson), { headers: respHeaders, status: 200 });
  };

  try {
    const pmTilesService = new PMTilesService(
      tigrisStorageRepository,
      memCacheRepository,
      metrics,
      d1Repository,
      d1Tags,
    );

    switch (params.requestType) {
      case 'tile': {
        const { z, x, y, version } = params as PMTilesTileParams;
        return await metrics.monitorAsyncFunction({ name: 'tile_request', tags: { z, version } }, handleTileRequest)(
          z,
          x,
          y,
          pmTilesService,
        );
      }
      case 'json': {
        const { version } = params as PMTilesJsonParams;
        return await metrics.monitorAsyncFunction(
          { name: 'json_request', tags: { version } },
          handleJsonRequest,
        )(pmTilesService);
      }
      case 'style': {
        const { style } = params as PMTilesStyleParams;
        console.log('style');
        return await metrics.monitorAsyncFunction(
          { name: 'style_request', tags: { style } },
          handleStyleRequest,
        )(pmTilesService);
      }
    }
  } catch (e: any) {
    console.error(e);
    metrics.push(Metric.create('error').addTag('error', e.message ?? 'unknown'));
    return uncached('Internal Server Error', 500);
  }
}
