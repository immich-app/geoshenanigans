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
  /** Where TileJSON, styles and tiles live: `${prefix}.json`, `${prefix}/style/...`, `${prefix}/{z}/{x}/{y}.mvt`. */
  prefix?: string;
}

const STYLE_NAME = /^\w+$/;

const sha256 = (body: Uint8Array) => createHash('sha256').update(body).digest('hex').toUpperCase();

const file = (path: string, body: Uint8Array): StaticFile => ({
  path,
  body,
  sha256: sha256(body),
  contentType: 'application/json',
});

function normalise(options: Pick<StaticFilesOptions, 'publicUrl' | 'prefix'>) {
  const publicUrl = new URL(options.publicUrl);
  if (publicUrl.search || publicUrl.hash) {
    throw new Error(`Public URL must not have a query or fragment: ${options.publicUrl}`);
  }
  const prefix = (options.prefix ?? 'v1').replace(/^\/+|\/+$/g, '');
  if (!prefix || prefix.split('/').some((part) => part === '' || part === '.' || part === '..')) {
    throw new Error(`Invalid prefix ${JSON.stringify(options.prefix)}`);
  }
  return { base: `${publicUrl.origin}${publicUrl.pathname.replace(/\/+$/, '')}`, prefix };
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
  const json = JSON.parse(new TextDecoder().decode(style.body)) as { sources?: Record<string, { url?: string }> };
  const source = json.sources?.vector;
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

/** Every non-tile file of the zone, ready to upload. */
export function staticFiles(options: StaticFilesOptions): StaticFile[] {
  const { base, prefix } = normalise(options);
  if (options.styles.length === 0) {
    throw new Error('No styles');
  }
  const files = [file(`${prefix}.json`, new TextEncoder().encode(JSON.stringify(tileJson(options))))];
  for (const style of options.styles) {
    if (!STYLE_NAME.test(style.name)) {
      throw new Error(`Style name ${JSON.stringify(style.name)} isn't one the tile URLs can address`);
    }
    const body = rewriteStyle(style, `${base}/${prefix}.json`);
    files.push(file(`${prefix}/style/${style.name}.json`, body), file(`${prefix}/style/${style.name}`, body));
  }
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
