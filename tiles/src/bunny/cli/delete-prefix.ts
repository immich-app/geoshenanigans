/**
 * Deletes the tiles of a prefix the zone no longer serves, e.g. the old one a week after a full rewrite switched to a
 * new prefix (bunny:sync --adopt --replace, plan section 6):
 *
 *   npm run bunny:delete-prefix -- --prefix <p> [--min-zoom 0 --max-zoom 15] [--concurrency 4] [--state-prefix <p>]
 *     [--yes]
 *
 * Only the tile folders go (`<prefix>/<z>/` for each zoom), never the TileJSON or styles (`v1.json`, `v1/style/`) or
 * another prefix inside this one (`v1/e2/` when deleting `v1`). It reads state.json first and refuses a prefix whose
 * tile folders hold the live prefix or sit inside it, so it cannot delete what is served. Without --yes it only says
 * what it would delete.
 * Bunny deletes a folder with everything below it in one DELETE (about 2.4 s for 950 files on the spike zone), so
 * zooms up to 7 go as one folder each and higher ones one `<prefix>/<z>/<x>/` column at a time, at most --concurrency
 * at once. Folders already gone are skipped: run it again after a stop.
 * Needs BUNNY_STORAGE_ZONE, BUNNY_STORAGE_PASSWORD, BUNNY_STATE_ZONE and BUNNY_STATE_PASSWORD (and BUNNY_STORAGE_HOST
 * unless it is storage.bunnycdn.com).
 * Exit codes: 0 done (or nothing to do), 1 refused or failed, 2 bad arguments.
 */
import { parseArgs } from 'node:util';
import { forEachLimit } from '../replication';
import { BunnyStateStore, readState } from '../state';
import { BunnyStorage, storageOptionsFromEnv } from '../storage';
import { prefixesOverlap } from '../tile-id';

class UsageError extends Error {}

/** Zooms up to this one are deleted as one folder each; their whole zoom is at most 16,384 files. */
const WHOLE_ZOOM_MAX = 7;

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

/** The folders to delete, in order: each zoom's columns (or the zoom at once), then the zoom's own folder. */
function* prefixFolders(prefix: string, minZoom: number, maxZoom: number): Generator<string> {
  for (let z = minZoom; z <= maxZoom; z++) {
    if (z > WHOLE_ZOOM_MAX) {
      for (let x = 0; x < 2 ** z; x++) {
        yield `${prefix}/${z}/${x}/`;
      }
    }
    yield `${prefix}/${z}/`;
  }
}

async function main(): Promise<number> {
  const { values } = parseArgs({
    options: {
      prefix: { type: 'string' },
      'min-zoom': { type: 'string' },
      'max-zoom': { type: 'string' },
      concurrency: { type: 'string' },
      'state-prefix': { type: 'string' },
      yes: { type: 'boolean', default: false },
    },
    strict: true,
  });
  const prefix = (values.prefix ?? '').replace(/^\/+|\/+$/g, '');
  if (!prefix || prefix.split('/').some((part) => part === '' || part === '.' || part === '..')) {
    throw new UsageError('--prefix is required: the tile prefix to delete, e.g. v1');
  }
  const minZoom = integer('min-zoom', values['min-zoom'], 0, 0, 20);
  const maxZoom = integer('max-zoom', values['max-zoom'], 15, minZoom, 20);
  const concurrency = integer('concurrency', values.concurrency, 4, 1, 64);

  const stateStore = BunnyStateStore.fromEnv();
  const state = await readState(stateStore, values['state-prefix']).finally(() => stateStore.close());
  if (!state) {
    throw new Error('No state.json in the state zone: nothing says which prefix is live, so nothing is deleted');
  }
  if (prefixesOverlap(prefix, state.prefix)) {
    throw new Error(`${state.liveBuild} is live under ${state.prefix}/: its tiles are under ${prefix}/'s folders`);
  }

  const storage = new BunnyStorage({ ...storageOptionsFromEnv(), concurrency });
  try {
    const present = (await storage.list(prefix))
      .filter((entry) => entry.isDirectory && /^\d+$/.test(entry.name))
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
    let deleted = 0;
    const started = performance.now();
    let last = started;
    for (const z of present) {
      await forEachLimit(prefixFolders(prefix, z, z), concurrency, async (folder) => {
        await storage.delete(folder);
        deleted++;
        const now = performance.now();
        if (now - last >= 10_000) {
          last = now;
          process.stderr.write(`deleted ${deleted} folders in ${Math.round((now - started) / 1000)} s, at ${folder}\n`);
        }
      });
    }
    process.stderr.write(`Deleted ${prefix}/ zooms ${present.join(', ')} (${deleted} folders)\n`);
    return 0;
  } finally {
    await storage.close();
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
