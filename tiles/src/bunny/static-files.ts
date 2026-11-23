/**
 * The files the tiles zone serves besides tiles, as the Worker serves them today:
 * - `v1.json`, the TileJSON: the Worker's JSON (tileJSON() of the archive, compact) with one difference, the tile
 *   template. It becomes `${publicUrl}/v1/{z}/{x}/{y}.mvt` without the Worker's `?v=<deployment>`: tile URLs stay
 *   the same across builds and the daily sync purges the pull zone instead (decision D3d).
 * - `v1/style/{name}.json` and `v1/style/{name}`, the map-styles build: prod's Worker serves those files unchanged at
 *   both paths. Their vector source points at `${publicUrl}/v1.json`; for prod's public URL that is the URL already in
 *   the files, so their bytes stay identical. (The Worker outside prod pointed it at `${origin}/v1`, but on storage
 *   `v1` is the tiles folder, not a file.)
 * Not reproduced: `/v1` as the TileJSON, and other versions (`/v2.json`, `/v2/style/...`), which the Worker's URL
 * pattern accepts but nothing links to.
 *
 * The tiles can sit under another prefix than these files (`prefix`, e.g. `v1/e2` after a full rewrite, plan section
 * 6): Immich keeps loading `/v1/style/*.json`, and only the TileJSON's tile template follows the tiles.
 */
import { createHash } from 'node:crypto';
import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { Header, Metadata } from '../pmtiles/types';
import { tileJSON } from '../pmtiles/utils';

export interface StaticFile {
  /** Relative to the zone root, like tile paths. */
  path: string;
  body: Uint8Array;
  /** Uppercase hex SHA-256 of body, for the storage Checksum header. */
  sha256: string;
  contentType: 'application/json';
}

export interface StyleSource {
  name: string;
  body: Uint8Array;
}

export interface StaticFilesOptions {
  header: Header;
  metadata: Metadata;
  styles: StyleSource[];
  /** e.g. https://tiles.immich.cloud (BUNNY_PUBLIC_URL). */
  publicUrl: string;
  /** Where the tiles live: `${prefix}/{z}/{x}/{y}.mvt` (default v1). */
  prefix?: string;
  /** Where the TileJSON and styles live: `${staticPrefix}.json`, `${staticPrefix}/style/...` (default v1). */
  staticPrefix?: string;
}

/** The TileJSON's and styles' prefix, the URLs Immich loads. */
export const STATIC_PREFIX = 'v1';

const STYLE_NAME = /^\w+$/;

const sha256 = (body: Uint8Array) => createHash('sha256').update(body).digest('hex').toUpperCase();

const file = (path: string, body: Uint8Array): StaticFile => ({
  path,
  body,
  sha256: sha256(body),
  contentType: 'application/json',
});

function checkPrefix(value: string | undefined): string {
  const prefix = (value ?? STATIC_PREFIX).replace(/^\/+|\/+$/g, '');
  if (!prefix || prefix.split('/').some((part) => part === '' || part === '.' || part === '..')) {
    throw new Error(`Invalid prefix ${JSON.stringify(value)}`);
  }
  return prefix;
}

/**
 * What the TileJSON and styles put before their paths: the public URL's origin and path, without a trailing slash.
 * Throws for a URL they could not carry as given: not an absolute http(s) URL, or with credentials, a query or a
 * fragment (an empty `?` or `#` too).
 */
export function publicUrlBase(value: string): string {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error(`Public URL must be an absolute http(s) URL, got ${JSON.stringify(value)}`);
  }
  if (url.protocol !== 'https:' && url.protocol !== 'http:') {
    throw new Error(`Public URL must be http(s), got ${JSON.stringify(value)}`);
  }
  // Not echoed: the value holds a password.
  if (url.username || url.password) {
    throw new Error('Public URL must not have credentials');
  }
  if (/[?#]/.test(url.href)) {
    throw new Error(`Public URL must not have a query or fragment: ${value}`);
  }
  return `${url.origin}${url.pathname.replace(/\/+$/, '')}`;
}

function normalise(options: Pick<StaticFilesOptions, 'publicUrl' | 'prefix' | 'staticPrefix'>) {
  return {
    base: publicUrlBase(options.publicUrl),
    prefix: checkPrefix(options.prefix),
    staticPrefix: checkPrefix(options.staticPrefix),
  };
}

/** The TileJSON object: the Worker's, with stable tile URLs. */
export function tileJson(options: Omit<StaticFilesOptions, 'styles'>) {
  const { base, prefix } = normalise(options);
  return {
    ...tileJSON({ header: options.header, metadata: options.metadata, version: 'v1' }),
    tiles: [`${base}/${prefix}/{z}/{x}/{y}.mvt`],
  };
}

/** A style pointed at this zone's TileJSON; unchanged bytes when it already points there. */
export function rewriteStyle(style: StyleSource, tileJsonUrl: string): Uint8Array {
  let json: { sources?: Record<string, { url?: string } | null> } | null;
  try {
    json = JSON.parse(new TextDecoder().decode(style.body)) as typeof json;
  } catch (error) {
    throw new Error(`Style ${style.name} is not JSON: ${error instanceof Error ? error.message : String(error)}`, {
      cause: error,
    });
  }
  const source = json?.sources?.vector;
  if (!source) {
    throw new Error(`Style ${style.name} has no "vector" source`);
  }
  if (source.url === tileJsonUrl) {
    return style.body;
  }
  source.url = tileJsonUrl;
  // map-styles writes two-space JSON; keep the same form.
  return new TextEncoder().encode(JSON.stringify(json, null, 2));
}

/** Each style's bytes pointed at the TileJSON, refusing a style the zone could not serve. */
function styleBodies(styles: StyleSource[], tileJsonUrl: string): Uint8Array[] {
  if (styles.length === 0) {
    throw new Error('No styles');
  }
  return styles.map((style) => {
    if (!STYLE_NAME.test(style.name)) {
      throw new Error(`Style name ${JSON.stringify(style.name)} isn't one the tile URLs can address`);
    }
    return rewriteStyle(style, tileJsonUrl);
  });
}

/**
 * Throws for anything staticFiles would refuse but the archive: a sync writes these files after the tiles, so it
 * checks them before it writes anything.
 */
export function checkStaticFiles(options: Omit<StaticFilesOptions, 'header' | 'metadata'>): void {
  const { base, staticPrefix } = normalise(options);
  styleBodies(options.styles, `${base}/${staticPrefix}.json`);
}

/** Every non-tile file of the zone, ready to upload. */
export function staticFiles(options: StaticFilesOptions): StaticFile[] {
  const { base, staticPrefix } = normalise(options);
  const bodies = styleBodies(options.styles, `${base}/${staticPrefix}.json`);
  const files = [file(`${staticPrefix}.json`, new TextEncoder().encode(JSON.stringify(tileJson(options))))];
  options.styles.forEach((style, i) => {
    files.push(
      file(`${staticPrefix}/style/${style.name}.json`, bodies[i]),
      file(`${staticPrefix}/style/${style.name}`, bodies[i]),
    );
  });
  return files;
}

/** The styles of a map-styles build (STYLES_DIR), by file name without `.json`. */
export async function readStyles(dir: string): Promise<StyleSource[]> {
  const names = (await readdir(dir)).filter((name) => name.endsWith('.json')).sort();
  return Promise.all(
    names.map(async (name) => ({
      name: name.slice(0, -'.json'.length),
      body: new Uint8Array(await readFile(join(dir, name))),
    })),
  );
}
