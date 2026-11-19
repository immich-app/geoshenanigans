/**
 * Compares two manifests position by position (see diff.ts):
 *
 *   npm run bunny:diff -- --old <manifest> --new <manifest> [--pending <manifest>]... [--ranges <file.ndjson>]
 *
 * Prints the totals (per kind and per zoom) as JSON on stdout. --pending plans a sync instead (planSync): the
 * manifests of builds state.json lists as pending, whose positions are marked for checking first. --ranges also
 * writes every range, one {"start","count","kind"} object per line (plus "verify" when planning), to a file of its
 * own beside it that replaces --ranges only once complete; --ranges must not be one of the manifests.
 */
import { randomBytes } from 'node:crypto';
import { createWriteStream } from 'node:fs';
import { rename, rm, stat } from 'node:fs/promises';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { parseArgs } from 'node:util';
import { DiffTotals, planSync, SyncPlan } from '../diff';

/** Lines are handed to the file in chunks of about this many characters. */
const CHUNK_CHARS = 1 << 16;

/** Refuses an output path that names one of the inputs, also through a hard or symbolic link. */
async function refuseInput(out: string, inputs: string[]) {
  const target = await stat(out).catch((error: NodeJS.ErrnoException) => {
    if (error.code === 'ENOENT') {
      return undefined;
    }
    throw error;
  });
  if (!target) {
    return;
  }
  for (const input of inputs) {
    const { dev, ino } = await stat(input);
    if (dev === target.dev && ino === target.ino) {
      throw new Error(`--ranges ${out} is the manifest ${input}: writing it would destroy the input`);
    }
  }
}

/** The plan's ranges as NDJSON, a chunk of lines at a time. */
async function* rangeLines(plan: SyncPlan, planning: boolean): AsyncGenerator<string> {
  let chunk = '';
  for await (const range of plan) {
    const line = planning ? range : { start: range.start, count: range.count, kind: range.kind };
    chunk += `${JSON.stringify(line)}\n`;
    if (chunk.length >= CHUNK_CHARS) {
      yield chunk;
      chunk = '';
    }
  }
  if (chunk) {
    yield chunk;
  }
}

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
  if (values.ranges) {
    await refuseInput(values.ranges, [values.old, values.new, ...values.pending]);
  }
  const plan = await planSync(values.old, values.new, values.pending);
  if (values.ranges) {
    const partial = `${values.ranges}.${process.pid}-${randomBytes(6).toString('hex')}.partial`;
    try {
      await pipeline(Readable.from(rangeLines(plan, planning)), createWriteStream(partial, { flags: 'wx' }));
      await rename(partial, values.ranges);
    } catch (error) {
      await rm(partial, { force: true });
      throw error;
    }
  } else {
    // No --ranges: the plan only runs for its totals.
    for await (const range of plan) {
      void range;
    }
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
