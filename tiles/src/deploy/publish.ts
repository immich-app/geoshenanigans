/**
 * Publishes a PMTiles archive and the map styles as deployment DEPLOYMENT_KEY: tile data goes to the bucket as ~1 MB
 * chunks, the styles next to them and the directory to a new D1 table. Nothing live changes; the worker switches over
 * when its DEPLOYMENT_KEY binding does.
 *
 * Needs PMTILES_FILE_PATH, STYLES_DIR (the map-styles build), DEPLOYMENT_KEY, LIVE_KEY (the key the worker serves now,
 * or "none"), D1_PROXY_URL, D1_PROXY_TOKEN, D1_ENV, TIGRIS_ENDPOINT, TIGRIS_BUCKET, TIGRIS_KEY_ID, TIGRIS_ACCESS_KEY
 * and optionally STORAGE_PREFIX, UPLOAD_CONCURRENCY, D1_CONCURRENCY.
 */
import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { setTimeout } from 'node:timers/promises';
import pLimit from 'p-limit';
import { tileJSON } from '../pmtiles/utils';
import { openArchiveFile } from './archive-file';
import { intEnv, parseDeploymentKey, requireEnv } from './config';
import { D1ProxyClient } from './d1-proxy-client';
import { Inventory, INVENTORY_NAME } from './inventory';
import { Chunk, ChunkPlanner, DirectoryRow, toDirectoryRows } from './layout';
import { countStagingRows, insertRows, prepareStagingTable, publishStagingTable, ROWS_PER_BATCH } from './staging';
import { DeploymentStorage } from './storage';

async function main() {
  const deploymentKey = parseDeploymentKey('DEPLOYMENT_KEY', process.env.DEPLOYMENT_KEY);
  const liveKeyValue = requireEnv('LIVE_KEY');
  const liveKey = liveKeyValue === 'none' ? undefined : parseDeploymentKey('LIVE_KEY', liveKeyValue);
  if (deploymentKey === liveKey) {
    throw new Error(`${deploymentKey} is the live deployment; its table can't be rebuilt in place`);
  }

  const stylesDir = requireEnv('STYLES_DIR');
  const styles = (await readdir(stylesDir)).filter((name) => name.endsWith('.json'));
  if (styles.length === 0) {
    throw new Error(`No styles in ${stylesDir}`);
  }
  const { archive, close } = await openArchiveFile(requireEnv('PMTILES_FILE_PATH'));
  const d1 = D1ProxyClient.fromEnv();
  const storage = DeploymentStorage.fromEnv();
  const uploads = pLimit(intEnv('UPLOAD_CONCURRENCY', 64));
  const writes = pLimit(intEnv('D1_CONCURRENCY', 8));
  const { header } = archive;
  console.log(
    `Deploying ${deploymentKey} (live: ${liveKey ?? 'none'}): ${header.numTileEntries} entries, ` +
      `${header.tileDataLength} bytes of tile data`,
  );

  await prepareStagingTable(
    d1,
    liveKey,
    JSON.stringify(tileJSON({ header, metadata: archive.metadata, version: 'v1' })),
  );

  let failure: unknown;
  const pending = new Set<Promise<void>>();
  const track = (task: Promise<void>) => {
    pending.add(task);
    task.then(
      () => pending.delete(task),
      (error) => {
        failure ??= error;
        pending.delete(task);
      },
    );
  };

  const stats = { rowsQueued: 0, rowsWritten: 0, chunksUploaded: 0, bytesUploaded: 0 };
  const inventory: Inventory = { objects: {} };
  const uploadChunk = async ({ chunkId, startByte, endByte }: Chunk) => {
    const data = await archive.readTileData(startByte, endByte - startByte);
    await storage.put(deploymentKey, `chunk_${chunkId}.pmtiles`, new Uint8Array(data));
    inventory.objects[`chunk_${chunkId}.pmtiles`] = data.byteLength;
    stats.chunksUploaded++;
    stats.bytesUploaded += data.byteLength;
  };
  const planner = new ChunkPlanner((chunk) => track(uploads(() => uploadChunk(chunk))));

  let rows: DirectoryRow[] = [];
  const flushRows = (count: number) => {
    const batch = rows.slice(0, count);
    rows = rows.slice(count);
    stats.rowsQueued += batch.length;
    track(
      writes(async () => {
        await insertRows(d1, batch);
        stats.rowsWritten += batch.length;
      }),
    );
  };

  const started = Date.now();
  const progress = setInterval(() => {
    const minutes = ((Date.now() - started) / 60_000).toFixed(1);
    console.log(
      `[${minutes} min] chunks ${stats.chunksUploaded}/${planner.chunkCount} ` +
        `(${(stats.bytesUploaded / 1e9).toFixed(1)} GB), rows ${stats.rowsWritten}/${stats.rowsQueued}`,
    );
  }, 30_000);

  try {
    for await (const entries of archive.tileEntries()) {
      rows.push(...toDirectoryRows(entries, planner));
      while (rows.length >= ROWS_PER_BATCH) {
        flushRows(ROWS_PER_BATCH);
      }
      // Reading the archive is much faster than either sink, so cap what's queued in memory.
      while (!failure && (uploads.pendingCount > 512 || writes.pendingCount > 64)) {
        await setTimeout(50);
      }
      if (failure) {
        throw failure;
      }
    }
    planner.finish();
    if (rows.length > 0) {
      flushRows(rows.length);
    }
    while (pending.size > 0) {
      await Promise.allSettled([...pending]);
    }
    if (failure) {
      throw failure;
    }
  } finally {
    clearInterval(progress);
  }

  for (const name of styles) {
    const style = await readFile(join(stylesDir, name));
    await storage.put(deploymentKey, `styles/${name}`, style, 'application/json');
    inventory.objects[`styles/${name}`] = style.byteLength;
  }
  // Last, so it only exists once everything it lists has been uploaded.
  await storage.put(
    deploymentKey,
    INVENTORY_NAME,
    new TextEncoder().encode(JSON.stringify(inventory)),
    'application/json',
  );

  // Every row went in through an idempotent batch, so a full count means nothing was lost to retries.
  const count = await countStagingRows(d1);
  if (count !== stats.rowsQueued + 1) {
    throw new Error(`Staging table has ${count} rows, expected ${stats.rowsQueued + 1}`);
  }
  await publishStagingTable(d1, deploymentKey);
  await close();
  console.log(
    `Published ${deploymentKey}: ${stats.chunksUploaded} chunks (${stats.bytesUploaded} bytes), ` +
      `${stats.rowsQueued} directory rows and ${styles.length} styles in ${((Date.now() - started) / 60_000).toFixed(1)} min`,
  );
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
