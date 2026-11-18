/**
 * Compares two manifests position by position (see diff.ts):
 *
 *   npm run bunny:diff -- --old <manifest> --new <manifest> [--ranges <file.ndjson>]
 *
 * Prints the totals (per kind and per zoom) as JSON on stdout. --ranges also writes every range, one
 * {"start","count","kind"} object per line.
 */
import { once } from 'node:events';
import { createWriteStream } from 'node:fs';
import { finished } from 'node:stream/promises';
import { parseArgs } from 'node:util';
import { diffManifests } from '../diff';

async function main() {
  const { values } = parseArgs({
    options: { old: { type: 'string' }, new: { type: 'string' }, ranges: { type: 'string' } },
    strict: true,
  });
  if (!values.old || !values.new) {
    throw new Error('Usage: bunny:diff --old <manifest> --new <manifest> [--ranges <file.ndjson>]');
  }
  const started = performance.now();
  const diff = await diffManifests(values.old, values.new);
  const out = values.ranges ? createWriteStream(values.ranges) : undefined;
  for await (const range of diff) {
    if (out && !out.write(`${JSON.stringify(range)}\n`)) {
      await once(out, 'drain');
    }
  }
  if (out) {
    out.end();
    await finished(out);
  }
  const { totals } = diff;
  const changedFraction = (totals.changed + totals.added + totals.removed) / Math.max(1, totals.positionsNew);
  console.log(JSON.stringify({ ...totals, changedFraction, seconds: (performance.now() - started) / 1000 }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
