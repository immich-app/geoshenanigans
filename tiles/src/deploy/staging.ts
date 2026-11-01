/**
 * A deployment's directory is written to a staging table and renamed to cache_entries_<key> once complete, so the
 * worker can never find a partial one. Every statement here is safe to repeat, as the proxy client retries.
 */
import { cacheTableName } from './config';
import type { D1ProxyClient } from './d1-proxy-client';
import type { DirectoryRow } from './layout';

export type StagingDatabase = Pick<D1ProxyClient, 'run' | 'batch' | 'tableNames' | 'cacheTables'>;

export const STAGING_TABLE = 'cache_entries_staging';
// Two bound parameters per row and D1 allows 100 per statement.
const ROWS_PER_STATEMENT = 50;
export const ROWS_PER_BATCH = ROWS_PER_STATEMENT * 4;

export async function prepareStagingTable(d1: StagingDatabase, liveKey: string | undefined, tileJson: string) {
  // D1 caps a database at 10 GB and a planet directory is ~3.5 GB, so only the live table survives a new deploy.
  const liveTable = liveKey ? cacheTableName(liveKey) : undefined;
  for (const table of await d1.cacheTables()) {
    if (table !== liveTable) {
      console.log(`Dropping old directory table ${table}`);
      await d1.run(`DROP TABLE IF EXISTS ${table}`);
    }
  }
  await d1.run(`DROP TABLE IF EXISTS ${STAGING_TABLE}`);
  await d1.run(
    `CREATE TABLE IF NOT EXISTS ${STAGING_TABLE} (startTileId INTEGER NOT NULL PRIMARY KEY, entry TEXT NOT NULL) STRICT`,
  );
  // Row -1 holds the TileJSON the worker serves for /v1.json.
  await d1.run(`INSERT OR REPLACE INTO ${STAGING_TABLE} (startTileId, entry) VALUES (-1, ?)`, [tileJson]);
}

export async function insertRows(d1: StagingDatabase, rows: DirectoryRow[]) {
  const statements = [];
  for (let start = 0; start < rows.length; start += ROWS_PER_STATEMENT) {
    const group = rows.slice(start, start + ROWS_PER_STATEMENT);
    statements.push({
      sql: `INSERT OR REPLACE INTO ${STAGING_TABLE} (startTileId, entry) VALUES ${group.map(() => '(?, ?)').join(', ')}`,
      params: group.flatMap((row) => [row.startTileId, row.entry]),
    });
  }
  await d1.batch(statements);
}

export async function countStagingRows(d1: StagingDatabase): Promise<number> {
  const [{ count }] = await d1.run(`SELECT COUNT(*) AS count FROM ${STAGING_TABLE}`);
  return count as number;
}

/** Turns the complete staging table into the deployment's directory table. */
export async function publishStagingTable(d1: StagingDatabase, deploymentKey: string) {
  const table = cacheTableName(deploymentKey);
  await d1.run(`DROP TABLE IF EXISTS ${table}`);
  try {
    await d1.run(`ALTER TABLE ${STAGING_TABLE} RENAME TO ${table}`);
  } catch (error) {
    // If a rename succeeded but its response was lost, the retries find the staging table already gone.
    const tables = await d1.tableNames();
    if (!tables.includes(table) || tables.includes(STAGING_TABLE)) {
      throw error;
    }
  }
}
