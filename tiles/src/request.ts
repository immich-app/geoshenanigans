/**
 * What the gateway (index.ts) and the cached Tiles entrypoint (tiles.ts) share: the public URL scheme and the inner
 * request the gateway sends to Tiles.
 */

const URL_MATCHER =
  /^\/v(?<VERSION>[0-9]+)((?=)|\/style\/(?<STYLE>\w+)(?:\.json)?|(?<JSON>\.json)|\/(?<Z>\d+)\/(?<X>\d+)\/(?<Y>\d+).mvt)$/;

export type PMTilesParams = {
  requestType: 'tile' | 'json' | 'style' | undefined;
  url: URL;
};

export type PMTilesStyleParams = PMTilesParams & {
  requestType: 'style';
  version: string;
  style: string;
};

export type PMTilesTileParams = PMTilesParams & {
  requestType: 'tile';
  version: string;
  z: string;
  x: string;
  y: string;
  // From ?v=, which the TileJSON adds so each deployment's tiles have their own URLs.
  deploymentKey?: string;
};

export type PMTilesJsonParams = PMTilesParams & {
  requestType: 'json';
  version: string;
};

export enum Header {
  PMTILES_DEPLOYMENT_KEY = 'PMTiles-Deployment-Key',
  CACHE_CONTROL = 'Cache-Control',
  CACHE_TAG = 'Cache-Tag',
  CF_CACHE_STATUS = 'Cf-Cache-Status',
  ACCESS_CONTROL_ALLOW_ORIGIN = 'Access-Control-Allow-Origin',
  CONTENT_TYPE = 'Content-Type',
  CONTENT_ENCODING = 'Content-Encoding',
  SERVER_TIMING = 'Server-Timing',
  ETAG = 'ETag',
  IF_NONE_MATCH = 'If-None-Match',
  ACCEPT_ENCODING = 'Accept-Encoding',
  // Set by the gateway on the inner request: the client's continent and the tags for Tiles' metrics, neither of which
  // Tiles can see itself (request.cf is undefined there). Request headers aren't part of the cache key.
  CONTINENT = 'X-Continent',
  METRIC_TAGS = 'X-Metric-Tags',
}

export function parseUrl(request: Request): PMTilesParams {
  return parsePath(new URL(request.url));
}

function parsePath(url: URL): PMTilesParams {
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

// Workers Cache keys entries by path and query, not by host, so the inner URL's host is only a placeholder.
const INNER_ORIGIN = 'https://tiles.internal';

// Bump when the body Tiles sends for the same URL and deployment changes (e.g. the TileJSON): cached responses outlive
// code-only deploys, and a new revision gives every response a new URL.
export const CACHE_REVISION = '2';

/**
 * The URL Tiles serves a request from, and the cache stores it under: the deployment key and the canonical form of
 * the path. Nothing in the client's query changes the body (the tile for ?v=<old key> is the live one too), so every
 * form of a URL shares one cached response per deployment.
 */
export function innerUrl(params: PMTilesParams, deploymentKey: string): string {
  const version = Number((params as PMTilesJsonParams).version);
  let path: string;
  switch (params.requestType) {
    case 'tile': {
      const { z, x, y } = params as PMTilesTileParams;
      path = `/v${version}/${Number(z)}/${Number(x)}/${Number(y)}.mvt`;
      break;
    }
    case 'json':
      path = `/v${version}.json`;
      break;
    case 'style':
      path = `/v${version}/style/${(params as PMTilesStyleParams).style}.json`;
      break;
    default:
      throw new Error(`No inner URL for ${params.url.pathname}`);
  }
  return `${INNER_ORIGIN}/${deploymentKey}${path}?rev=${CACHE_REVISION}`;
}

/** The deployment key and public URL params an inner URL was built from. */
export function parseInnerUrl(url: string): { deploymentKey: string; params: PMTilesParams } {
  const inner = new URL(url);
  const [, deploymentKey = '', ...path] = inner.pathname.split('/');
  return { deploymentKey, params: parsePath(new URL(`/${path.join('/')}`, inner.origin)) };
}

/** Metric tags travel to Tiles as URI-encoded JSON: header values must be Latin-1, AS names needn't be. */
export function encodeMetricTags(tags: { [key: string]: string }): string {
  return encodeURIComponent(JSON.stringify(tags));
}

export function decodeMetricTags(header: string | null): { [key: string]: string } {
  try {
    return JSON.parse(decodeURIComponent(header ?? '')) ?? {};
  } catch {
    return {};
  }
}
