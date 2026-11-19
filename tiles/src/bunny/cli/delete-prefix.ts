/**
 * Deletes the tiles of a prefix the zone no longer serves, e.g. the old one a week after a full rewrite switched to a
 * new prefix (bunny:sync --adopt --replace, plan section 6):
 *
 *   npm run bunny:delete-prefix -- --prefix <p> [--min-zoom 0 --max-zoom 15] [--concurrency 4] [--state-prefix <p>]
 *     [--static-prefix v1] [--yes]
 *
 * Only the tile folders go (`<prefix>/<z>/` for each zoom, named as tile URLs name them: `8`, never `08`), never the
 * TileJSON or styles (`v1.json`, `v1/style/`) or another prefix inside this one (`v1/e2/` when deleting `v1`).
 * It refuses a prefix whose tile folders hold, or sit inside, either the prefix state.json names live or the one the
 * zone's TileJSON (`<static prefix>.json`) points tiles at: a switch that wrote the TileJSON and then failed leaves
 * the two apart, and clients load what the TileJSON says. A state.json or TileJSON that is missing, or a tile template
 * it cannot read, refuses too. Both are read again before every DELETE, so a switch made meanwhile stops it before
 * its next folder (that is no lock: run no switch while it deletes). Without --yes it only says what it would delete.
 * Bunny deletes a folder with everything below it in one DELETE (about 2.4 s for 950 files on the spike zone), so
 * zooms up to 7 go as one folder each and higher ones one `<prefix>/<z>/<x>/` column at a time, at most --concurrency
 * at once, the zoom's own folder once all its columns are gone. Only the folders a listing shows are deleted: run it
 * again after a stop and it carries on where it was.
 * Needs BUNNY_STORAGE_ZONE, BUNNY_STORAGE_PASSWORD, BUNNY_STATE_ZONE and BUNNY_STATE_PASSWORD (and BUNNY_STORAGE_HOST
 * unless it is storage.bunnycdn.com).
 * Exit codes: 0 done (or nothing to do), 1 refused or failed, 2 bad arguments.
 */
import { parseArgs } from 'node:util';
import { forEachLimit } from '../replication';
import { BunnyStateStore, readState, SyncState } from '../state';
import { STATIC_PREFIX } from '../static-files';
import { BunnyStorage, storageOptionsFromEnv } from '../storage';
import { prefixesOverlap } from '../tile-id';

class UsageError extends Error {}

/** Zooms up to this one are deleted as one folder each; their whole zoom is at most 16,384 files. */
const WHOLE_ZOOM_MAX = 7;

/** A zoom or column folder as tile URLs name it: a number without leading zeros. */
const TILE_FOLDER = /^(0|[1-9]\d*)$/;

/** A TileJSON tile template as bunny:sync writes it; group 1 is the path in front of `/{z}/{x}/{y}.mvt`. */
const TEMPLATE = /^https?:\/\/[^/?#]+((?:\/[\w~-][\w.~-]*)+)\/\{z\}\/\{x\}\/\{y\}\.mvt(?:\?[^#]*)?$/;

function integer(name: string, value: string | undefined, fallback: number, min = 0, max = Infinity): number {
  if (value === undefined) {
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < min || parsed > max) {
    throw new UsageError(`--${name} must be an integer from ${min} to ${max}, got ${value}`);
  }
  return parsed;
}

function trimPrefix(name: string, value: string | undefined): string {
  const prefix = (value ?? '').replace(/^\/+|\/+$/g, '');
  if (!prefix || prefix.split('/').some((part) => part === '' || part === '.' || part === '..')) {
    throw new UsageError(`--${name} must be a folder of the zone, e.g. v1, got ${JSON.stringify(value)}`);
  }
  return prefix;
}

/**
 * The tile prefixes a TileJSON may point clients at: for each template, the path in front of `/{z}/{x}/{y}.mvt`
 * and every tail of it, since a public URL with a path of its own puts that path in front of the prefix.
 */
function servedPrefixes(body: Uint8Array | null, file: string): string[] {
  const refuse = (why: string) =>
    new Error(`${file} ${why}: nothing says which prefix clients load, so nothing is deleted`);
  if (!body) {
    throw refuse('is not in the tiles zone');
  }
  let tiles: unknown;
  try {
    tiles = (JSON.parse(new TextDecoder().decode(body)) as { tiles?: unknown }).tiles;
  } catch {
    throw refuse('is not JSON');
  }
  if (!Array.isArray(tiles) || tiles.length === 0) {
    throw refuse('lists no tile templates');
  }
  return tiles.flatMap((template: unknown) => {
    const path = typeof template === 'string' ? TEMPLATE.exec(template)?.[1] : undefined;
    if (!path) {
      throw refuse(`has a tile template this cannot read, ${JSON.stringify(template)}`);
    }
    const segments = path.slice(1).split('/');
    return segments.map((_, i) => segments.slice(i).join('/'));
  });
}

async function main(): Promise<number> {
  const { values } = parseArgs({
    options: {
      prefix: { type: 'string' },
      'min-zoom': { type: 'string' },
      'max-zoom': { type: 'string' },
      concurrency: { type: 'string' },
      'state-prefix': { type: 'string' },
      'static-prefix': { type: 'string' },
      yes: { type: 'boolean', default: false },
    },
    strict: true,
  });
  if (!values.prefix) {
    throw new UsageError('--prefix is required: the tile prefix to delete, e.g. v1');
  }
  const prefix = trimPrefix('prefix', values.prefix);
  const tileJsonPath = `${trimPrefix('static-prefix', values['static-prefix'] ?? STATIC_PREFIX)}.json`;
  const minZoom = integer('min-zoom', values['min-zoom'], 0, 0, 20);
  // The default too must not be below --min-zoom: a range that holds no zoom would delete nothing and exit 0.
  const maxZoom = integer('max-zoom', values['max-zoom'] ?? '15', 15, minZoom, 20);
  const concurrency = integer('concurrency', values.concurrency, 4, 1, 64);

  const stateStore = BunnyStateStore.fromEnv();
  const storage = new BunnyStorage({ ...storageOptionsFromEnv(), concurrency });
  /** Refuses unless both what state.json names live and what the TileJSON serves lie apart from `prefix`'s folders. */
  const checkUnserved = async (): Promise<SyncState> => {
    const [state, tileJson] = await Promise.all([
      readState(stateStore, values['state-prefix']),
      storage.get(tileJsonPath),
    ]);
    if (!state) {
      throw new Error('No state.json in the state zone: nothing says which prefix is live, so nothing is deleted');
    }
    if (prefixesOverlap(prefix, state.prefix)) {
      throw new Error(`${state.liveBuild} is live under ${state.prefix}/: its tiles are under ${prefix}/'s folders`);
    }
    const served = servedPrefixes(tileJson, tileJsonPath).find((other) => prefixesOverlap(prefix, other));
    if (served !== undefined) {
      throw new Error(`${tileJsonPath} points tiles at ${served}/: they are under ${prefix}/'s folders`);
    }
    return state;
  };
  try {
    const state = await checkUnserved();
    const present = (await storage.list(prefix))
      .filter((entry) => entry.isDirectory && TILE_FOLDER.test(entry.name))
      .map((entry) => Number(entry.name))
      .filter((z) => z >= minZoom && z <= maxZoom)
      .sort((a, b) => a - b);
    if (present.length === 0) {
      process.stderr.write(`No tile folders of zooms ${minZoom}-${maxZoom} under ${prefix}/: nothing to delete\n`);
      return 0;
    }
    process.stderr.write(
      `Live: ${state.liveBuild} under ${state.prefix}/. ${values.yes ? 'Deleting' : 'Would delete'} ${prefix}/ ` +
        `zooms ${present.join(', ')}\n`,
    );
    if (!values.yes) {
      process.stderr.write('Nothing deleted: run again with --yes\n');
      return 0;
    }

    // Each DELETE waits for a fresh check; DELETEs due while one is running share it.
    let checking: Promise<unknown> | undefined;
    const recheck = () => (checking ??= checkUnserved().finally(() => (checking = undefined)));
    let deleted = 0;
    const started = performance.now();
    let last = started;
    const remove = async (folder: string) => {
      await recheck();
      await storage.delete(folder);
      deleted++;
      const now = performance.now();
      if (now - last >= 10_000) {
        last = now;
        process.stderr.write(`deleted ${deleted} folders in ${Math.round((now - started) / 1000)} s, at ${folder}\n`);
      }
    };
    for (const z of present) {
      if (z > WHOLE_ZOOM_MAX) {
        // The columns still there: a run stopped part-way deleted the others.
        const columns = (await storage.list(`${prefix}/${z}`))
          .filter((entry) => entry.isDirectory && TILE_FOLDER.test(entry.name) && Number(entry.name) < 2 ** z)
          .map((entry) => `${prefix}/${z}/${entry.name}/`);
        await forEachLimit(columns, concurrency, remove);
      }
      await remove(`${prefix}/${z}/`);
    }
    process.stderr.write(`Deleted ${prefix}/ zooms ${present.join(', ')} (${deleted} folders)\n`);
    return 0;
  } finally {
    await Promise.all([storage.close(), stateStore.close()]);
  }
}

main().then(
  (code) => {
    process.exitCode = code;
  },
  (error: unknown) => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = error instanceof UsageError ? 2 : 1;
  },
);
