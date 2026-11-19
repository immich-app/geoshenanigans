/**
 * Compares two manifests position by position (see diff.ts):
 *
 *   npm run bunny:diff -- --old <manifest> --new <manifest> [--pending <manifest>]... [--ranges <file.ndjson>]
 *
 * Prints the totals (per kind and per zoom) as JSON on stdout. --pending plans a sync instead (planSync): the
 * manifests of builds state.json lists as pending, whose positions are marked for checking first. --ranges also
 * writes every range, one {"start","count","kind"} object per line (plus "verify" when planning).
 */
import { once } from 'node:events';
import { createWriteStream } from 'node:fs';
import { finished } from 'node:stream/promises';
import { parseArgs } from 'node:util';
import { DiffTotals, planSync } from '../diff';

async function main() {
  const { values } = parseArgs({
    options: {
      old: { type: 'string' },
      new: { type: 'string' },
      pending: { type: 'string', multiple: true, default: [] },
      ranges: { type: 'string' },
    },
    strict: true,
  });
  if (!values.old || !values.new) {
    throw new Error('Usage: bunny:diff --old <manifest> --new <manifest> [--pending <manifest>]... [--ranges <file>]');
  }
  const started = performance.now();
  const planning = values.pending.length > 0;
  const plan = await planSync(values.old, values.new, values.pending);
  const out = values.ranges ? createWriteStream(values.ranges) : undefined;
  for await (const range of plan) {
    const line = planning ? range : { start: range.start, count: range.count, kind: range.kind };
    if (out && !out.write(`${JSON.stringify(line)}\n`)) {
      await once(out, 'drain');
    }
  }
  if (out) {
    out.end();
    await finished(out);
  }
  const totals: DiffTotals = plan.totals.diff;
  const changedFraction = (totals.changed + totals.added + totals.removed) / Math.max(1, totals.positionsNew);
  const { pendingBuilds, verify, verifyOnly, ranges } = plan.totals;
  console.log(
    JSON.stringify(
      {
        ...totals,
        changedFraction,
        ...(planning ? { pendingBuilds, verify, verifyOnly, planRanges: ranges } : {}),
        seconds: (performance.now() - started) / 1000,
      },
      null,
      2,
    ),
  );
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
