/**
 * Builds the manifest of a local PMTiles archive (see manifest.ts):
 *
 *   npm run bunny:manifest -- --archive <path> --build <YYYYMMDD> [--out <file>] [--threads N] [--leaves-per-job N]
 *
 * --out defaults to <build>.tman.zst in the current directory and --threads to every core. Prints the result as
 * JSON on stdout and progress every 10 s on stderr.
 */
import { parseArgs } from 'node:util';
import { buildManifest, ManifestProgress } from '../manifest';

const positiveInt = (name: string, value: string | undefined) => {
  if (value === undefined) {
    return undefined;
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`--${name} must be a positive integer, got ${value}`);
  }
  return parsed;
};

async function main() {
  const { values } = parseArgs({
    options: {
      archive: { type: 'string' },
      build: { type: 'string' },
      out: { type: 'string' },
      threads: { type: 'string' },
      'leaves-per-job': { type: 'string' },
    },
    strict: true,
  });
  if (!values.archive || !values.build) {
    throw new Error('Usage: bunny:manifest --archive <path> --build <YYYYMMDD> [--out <file>] [--threads N]');
  }
  const started = Date.now();
  let latest: ManifestProgress | undefined;
  const progress = setInterval(() => {
    if (!latest) {
      return;
    }
    const seconds = (Date.now() - started) / 1000;
    const share = latest.archiveEntries / Math.max(1, latest.archiveEntriesTotal);
    console.error(
      `[${seconds.toFixed(0)} s] ${latest.archiveEntries}/${latest.archiveEntriesTotal} entries (${(share * 100).toFixed(1)}%), ` +
        `${(latest.bytesHashed / 1e9).toFixed(1)} GB hashed, ${(latest.bytesHashed / 1e6 / seconds).toFixed(0)} MB/s`,
    );
  }, 10_000);
  try {
    const out = values.out ?? `${values.build}.tman.zst`;
    const stats = await buildManifest(values.archive, {
      out,
      buildKey: values.build,
      threads: positiveInt('threads', values.threads),
      leavesPerJob: positiveInt('leaves-per-job', values['leaves-per-job']),
      onProgress: (p) => (latest = p),
    });
    console.log(JSON.stringify({ out, ...stats }, null, 2));
  } finally {
    clearInterval(progress);
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
