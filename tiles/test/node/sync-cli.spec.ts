/**
 * bunny:sync refuses what it could only fail on part-way (a malformed command line, a public URL or style the TileJSON
 * and styles can't take, a pull zone the purge can't use) before its first request, with a report that says why.
 * Every case runs against zones a sync with the same command line would write to: the first test shows it does.
 */
import { spawn } from 'node:child_process';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { SyncReport } from '../../src/bunny/sync';
import { zoomEnd } from '../../src/bunny/tile-id';
import { FakeStorage } from './fake-storage';
import { randomBuild, rng, writeArchiveFile } from './manifest-fixtures';
import { scratchDir } from './upload-fixture';

const TILES_DIR = join(import.meta.dirname, '../..');
const STYLE = JSON.stringify(
  { version: 8, sources: { vector: { type: 'vector', url: 'https://tiles.example/v1.json' } } },
  null,
  2,
);

let cleanup: (() => Promise<void> | void)[] = [];

afterEach(async () => {
  for (const step of cleanup.reverse()) {
    await step();
  }
  cleanup = [];
});

/** A build over zooms 0-3, an empty tiles zone and state zone, a purge API and a map-styles build. */
async function world() {
  const { dir, remove } = await scratchDir();
  cleanup.push(remove);
  const archive = join(dir, '20261118.pmtiles');
  const r = rng(7);
  await writeArchiveFile(archive, randomBuild(r, zoomEnd(3)), r, {
    maxZoom: 3,
    metadata: { name: 'test', 'planetiler:osm:osmosisreplicationtime': '2026-11-18T01:02:03Z' },
  });
  const requests: string[] = [];
  const tiles = await FakeStorage.start({
    regions: ['UK'],
    replicationDelayMs: { UK: 5 },
    onRequest: ({ method, path }) => requests.push(`tiles ${method} ${path}`),
  });
  const stateZone = await FakeStorage.start({
    zone: 'state-zone',
    onRequest: ({ method, path }) => requests.push(`state ${method} ${path}`),
  });
  const purges: string[] = [];
  const api = createServer((request, response) => {
    purges.push(`${request.method} ${request.url}`);
    response.writeHead(204).end();
  });
  await new Promise<void>((resolve) => api.listen(0, '127.0.0.1', resolve));
  cleanup.push(
    () => tiles.close(),
    () => stateZone.close(),
    () => new Promise<void>((resolve) => api.close(() => resolve())),
  );
  const styles = join(dir, 'styles');
  await mkdir(styles);
  await writeFile(join(styles, 'light.json'), STYLE);

  let runs = 0;
  /**
   * Runs bunny:sync with --force-full (every position written, no state needed) and the given arguments after the
   * rest, the environment overridden by `env`. Returns its exit code, output and the report it left.
   */
  const sync = async (extra: string[], env: Record<string, string> = {}) => {
    const report = join(dir, `report-${++runs}.json`);
    const args = [
      '--report',
      report,
      '--archive',
      archive,
      '--build',
      '20261118',
      '--force-full',
      '--styles-dir',
      styles,
      '--work-dir',
      join(dir, `work-${runs}`),
      '--threads',
      '1',
      '--regions',
      'UK',
      '--replication-interval-s',
      '1',
      ...extra,
    ];
    // Nothing from the caller's environment: no pinned storage addresses, styles or API of its own.
    const inherited = Object.fromEntries(
      Object.entries(process.env).filter(([name]) => !name.startsWith('BUNNY_') && name !== 'STYLES_DIR'),
    );
    const child = spawn(process.execPath, ['--import=tsx', 'src/bunny/cli/sync.ts', ...args], {
      cwd: TILES_DIR,
      env: {
        ...inherited,
        BUNNY_STORAGE_ZONE: tiles.zone,
        BUNNY_STORAGE_PASSWORD: tiles.password,
        BUNNY_STORAGE_HOST: tiles.url,
        BUNNY_STATE_ZONE: stateZone.zone,
        BUNNY_STATE_PASSWORD: stateZone.password,
        BUNNY_STATE_HOST: stateZone.url,
        BUNNY_API_KEY: 'test-key',
        BUNNY_PULL_ZONE_ID: '123',
        BUNNY_PUBLIC_URL: 'https://tiles.example',
        BUNNY_API_URL: `http://127.0.0.1:${(api.address() as { port: number }).port}`,
        ...env,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    cleanup.push(() => {
      child.kill('SIGKILL');
    });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk) => (stdout += chunk));
    child.stderr.on('data', (chunk) => (stderr += chunk));
    // 'close', not 'exit': the output streams have ended too.
    const code = await new Promise<number | null>((resolve) => child.once('close', (exitCode) => resolve(exitCode)));
    const written = JSON.parse(await readFile(report, 'utf8')) as SyncReport;
    return { code, stdout, stderr, report: written };
  };

  return { tiles, requests, purges, styles, sync };
}

type World = Awaited<ReturnType<typeof world>>;

/** The run failed with `error`, wrote a report saying so, and sent no request to either zone or the purge API. */
async function expectRefused(w: World, run: ReturnType<World['sync']>, error: RegExp) {
  const result = await run;
  const { code, stdout, stderr, report } = result;
  expect(code, stderr).toBe(1);
  expect(report).toMatchObject({ status: 'failed', filesUploaded: null, purged: false });
  expect(report.error).toMatch(error);
  expect(JSON.parse(stdout)).toEqual(report);
  expect(w.requests).toEqual([]);
  expect(w.purges).toEqual([]);
  return result;
}

describe('bunny:sync CLI', () => {
  it('writes every position, purges and publishes the state with the arguments the other cases build on', async () => {
    const w = await world();
    const { code, stderr, report } = await w.sync([]);
    expect(code, stderr).toBe(0);
    expect(report).toMatchObject({ status: 'success', mode: 'force-full', purged: true, error: null });
    expect(report.filesUploaded).toBeGreaterThan(0);
    expect(w.requests).toContain('tiles PUT v1/0/0/0.mvt');
    expect(w.requests).toContain('state PUT state.json');
    expect(w.purges).toEqual(['POST /pullzone/123/purgeCache']);
  });

  it('refuses a malformed command line before any request, with a report', async () => {
    const w = await world();
    await Promise.all([
      // Read leniently, a flag given a value was false and the run wrote: it isn't a dry run.
      expectRefused(w, w.sync(['--dry-run=true']), /'--dry-run' does not take an argument/),
      expectRefused(w, w.sync(['--force-full=false']), /'--force-full' does not take an argument/),
      // Read leniently, an option without its value fell back to its default.
      expectRefused(w, w.sync(['--prefix']), /'--prefix <value>' argument missing/),
      expectRefused(w, w.sync(['--max-zoom', '--plain-puts']), /'--max-zoom' argument is ambiguous/),
      expectRefused(w, w.sync(['--prefix=']), /--prefix needs a value/),
      expectRefused(w, w.sync(['--max-zoom', '']), /--max-zoom needs a value/),
      expectRefused(w, w.sync(['--max-zoom', '0x2']), /--max-zoom must be an integer/),
      expectRefused(w, w.sync(['--max-changed-fraction', '1e-2']), /--max-changed-fraction must be a number/),
      expectRefused(w, w.sync(['--threads', ' 2']), /--threads must be an integer/),
      // The gate starts no DESCRIBE after its timeout: 0 would fail the run after it wrote.
      expectRefused(w, w.sync(['--replication-timeout-min', '0']), /--replication-timeout-min must be more than 0/),
      expectRefused(w, w.sync(['--regions', ' , ']), /--regions needs region codes/),
      expectRefused(w, w.sync(['dry-run']), /Unexpected argument 'dry-run'/),
      expectRefused(w, w.sync(['--dryrun']), /Unknown option '--dryrun'/),
    ]);
  });

  it('gates each region once, as the zone names it, however --regions spells it', async () => {
    const w = await world();
    const { code, stderr, report } = await w.sync(['--regions', ' uk,UK , Uk', '--replication-timeout-min', '0.5']);
    expect(code, stderr).toBe(0);
    expect(report.replicationWaitSeconds).toEqual({ UK: expect.any(Number) });
    const gates = stderr.split('\n').filter((line) => line.includes('replication: waiting for'));
    expect(gates.length).toBeGreaterThan(0);
    for (const line of gates) {
      expect(line).toMatch(/ in UK$/);
    }
  });

  it('refuses a public URL or style the TileJSON and styles cannot take before any request', async () => {
    const w = await world();
    const url = (value: string) => w.sync([], { BUNNY_PUBLIC_URL: value });
    await Promise.all([
      expectRefused(w, url('tiles.example'), /Public URL must be an absolute http\(s\) URL/),
      expectRefused(w, url('ftp://tiles.example'), /Public URL must be http\(s\)/),
      expectRefused(w, url('https://tiles.example/?v=1'), /must not have a query or fragment/),
      expectRefused(w, url('https://tiles.example?'), /must not have a query or fragment/),
      expectRefused(w, url('https://tiles.example/#top'), /must not have a query or fragment/),
      expectRefused(w, url('https://user:secret@tiles.example'), /must not have credentials/).then((result) =>
        expect(result.stdout + result.stderr).not.toContain('secret'),
      ),
      expectRefused(w, w.sync(['--static-prefix', 'v1/../x']), /Invalid prefix/),
    ]);

    await writeFile(join(w.styles, 'bare.json'), '{"version":8,"sources":{}}');
    await expectRefused(w, w.sync([]), /Style bare has no "vector" source/);
    await writeFile(join(w.styles, 'bare.json'), '{"version":8,');
    await expectRefused(w, w.sync([]), /Style bare is not JSON/);
  });

  it('refuses a pull zone id or API URL the purge cannot use before any request', async () => {
    const w = await world();
    await Promise.all([
      expectRefused(w, w.sync([], { BUNNY_PULL_ZONE_ID: 'pz-123' }), /Pull zone id must be numeric, got "pz-123"/),
      expectRefused(w, w.sync([], { BUNNY_PULL_ZONE_ID: ' 123' }), /Pull zone id must be numeric/),
      expectRefused(w, w.sync([], { BUNNY_API_URL: 'api.bunny.net' }), /Bunny API URL must be an absolute http\(s\)/),
    ]);
  });
});
