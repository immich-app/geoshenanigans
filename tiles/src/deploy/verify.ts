/**
 * Checks a published deployment against the archive it came from, reading it back the way the worker does:
 * D1 directory row -> DirectoryString lookup -> byte range of a chunk object. Exits non-zero on any mismatch.
 *
 * Needs the same variables as publish.ts except LIVE_KEY, and optionally VERIFY_SAMPLES and VERIFY_CONCURRENCY.
 */
import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import pLimit from 'p-limit';
import { DirectoryString } from '../pmtiles.service';
import { Entry } from '../pmtiles/types';
import { tileJSON } from '../pmtiles/utils';
import { Archive } from './archive';
import { openArchiveFile } from './archive-file';
import { cacheTableName, intEnv, parseDeploymentKey, requireEnv } from './config';
import { D1ProxyClient } from './d1-proxy-client';
import { DeploymentStorage } from './storage';

const MISSING_TILE_SAMPLES = 50;

type Check = { tileId: number; entry: Entry | undefined };

async function main() {
  const deploymentKey = parseDeploymentKey('DEPLOYMENT_KEY', process.env.DEPLOYMENT_KEY);
  const { archive, close } = await openArchiveFile(requireEnv('PMTILES_FILE_PATH'));
  const d1 = D1ProxyClient.fromEnv();
  const storage = DeploymentStorage.fromEnv();
  const table = cacheTableName(deploymentKey);
  const failures: string[] = [];
  const fail = (message: string) => {
    failures.push(message);
    console.error(message);
  };

  const [tileJsonRow] = await d1.run(`SELECT entry FROM ${table} WHERE startTileId = -1`);
  const expectedTileJson = JSON.stringify(
    tileJSON({ header: archive.header, metadata: archive.metadata, version: 'v1' }),
  );
  if (tileJsonRow?.entry !== expectedTileJson) {
    fail('TileJSON row is missing or differs from the archive');
  }

  const stylesDir = requireEnv('STYLES_DIR');
  for (const name of (await readdir(stylesDir)).filter((file) => file.endsWith('.json'))) {
    const deployed = await storage.get(deploymentKey, `styles/${name}`).catch(() => undefined);
    if (!deployed || !Buffer.from(deployed).equals(await readFile(join(stylesDir, name)))) {
      fail(`Style ${name} is missing or differs from ${stylesDir}`);
    }
  }

  const checks = await sampleTiles(archive, intEnv('VERIFY_SAMPLES', 2000));
  const limit = pLimit(intEnv('VERIFY_CONCURRENCY', 16));
  await Promise.all(
    checks.map((check) =>
      limit(async () => {
        const failure = await checkTile(check, table, archive, d1, storage, deploymentKey);
        if (failure) {
          fail(failure);
        }
      }),
    ),
  );
  await close();

  if (failures.length > 0) {
    throw new Error(`${failures.length} checks failed for ${deploymentKey}`);
  }
  console.log(`Verified ${deploymentKey}: TileJSON, styles and ${checks.length} tile lookups match the archive`);
}

/** Every n-th entry across the whole archive (a random tile within its run), the last one, and a few gaps. */
async function sampleTiles(archive: Archive, samples: number): Promise<Check[]> {
  const stride = Math.max(1, Math.floor(archive.header.numTileEntries / samples));
  const checks: Check[] = [];
  let index = 0;
  let last: Entry | undefined;
  let missing = 0;
  for await (const entries of archive.tileEntries()) {
    for (const entry of entries) {
      if (last && entry.tileId > last.tileId + last.runLength && missing < MISSING_TILE_SAMPLES) {
        checks.push({ tileId: last.tileId + last.runLength, entry: undefined });
        missing++;
      }
      if (index++ % stride === 0) {
        checks.push({ tileId: entry.tileId + Math.floor(Math.random() * entry.runLength), entry });
      }
      last = entry;
    }
  }
  if (last) {
    checks.push({ tileId: last.tileId + last.runLength - 1, entry: last });
  }
  return checks;
}

async function checkTile(
  { tileId, entry }: Check,
  table: string,
  archive: Archive,
  d1: D1ProxyClient,
  storage: DeploymentStorage,
  deploymentKey: string,
): Promise<string | undefined> {
  // Same query and parsing as PMTilesService.getTile.
  const [row] = await d1.run(`SELECT * FROM ${table} WHERE startTileId <= ? ORDER BY startTileId DESC LIMIT 1`, [
    tileId,
  ]);
  const located = row ? await new DirectoryString(row.entry as string).findTile(tileId) : undefined;
  if (!entry) {
    return located ? `Tile ${tileId} isn't in the archive but the deployment has it` : undefined;
  }
  if (!located) {
    return `Tile ${tileId} is in the archive but the deployment can't find it`;
  }
  if (located.length !== entry.length) {
    return `Tile ${tileId}: deployment says ${located.length} bytes, archive ${entry.length}`;
  }
  const deployed = await storage.get(deploymentKey, `chunk_${located.chunkId}.pmtiles`, {
    offset: located.offset,
    length: located.length,
  });
  const expected = new Uint8Array(await archive.readTileData(entry.offset, entry.length));
  if (!Buffer.from(deployed).equals(Buffer.from(expected))) {
    return `Tile ${tileId}: deployed bytes (chunk ${located.chunkId} @ ${located.offset}) differ from the archive`;
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
