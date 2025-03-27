import { S3Client } from '@aws-sdk/client-s3';
import fetchBuilder from 'fetch-retry';
import { gunzipSync } from 'fflate';
import { mkdirSync, readdirSync, rmSync } from 'fs';
import pLimit from 'p-limit';
import { join } from 'path';
import { setTimeout } from 'timers/promises';
import { AsyncFn, IMetricsRepository, IStorageRepository, Operation } from './interface';
import { DirectoryString, PMTilesService } from './pmtiles/pmtiles.service';
import { Compression, Directory, Header } from './pmtiles/types';
import { deserializeIndex } from './pmtiles/utils';
import { CloudflareD1Repository, MemCacheRepository, S3StorageRepository } from './repository';

class FakeMetricsRepository implements IMetricsRepository {
  constructor() {}

  monitorAsyncFunction<T extends AsyncFn>(
    operation: Operation,
    call: T,
  ): (...args: Parameters<T>) => Promise<Awaited<ReturnType<T>>> {
    return call;
  }
  push(): void {}
}

export function decompress(buf: ArrayBuffer, compression: Compression): ArrayBuffer {
  if (compression !== Compression.Gzip) {
    throw new Error('Compression method not supported');
  }
  const result = gunzipSync(new Uint8Array(buf));
  return result;
}

const getDirectory = async (length: number, offset: number, source: IStorageRepository, header: Header) => {
  const resp = await source.getRange({ offset, length });
  const data = decompress(resp, header.internalCompression);
  const entries = deserializeIndex(await new Response(data).arrayBuffer());
  if (entries.length === 0) {
    throw new Error('Empty directory is invalid');
  }
  const directory: Directory = { offsetStart: entries[0].offset, tileIdStart: entries[0].tileId, entries };
  return directory;
};

enum DBS {
  ENAM = 'ENAM',
  WNAM = 'WNAM',
  WEUR = 'WEUR',
  APAC = 'APAC',
  EEUR = 'EEUR',
  OC = 'OC',
  GLOBAL = 'GLOBAL',
}

const handler = async () => {
  const { S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT, CLOUDFLARE_ACCOUNT_ID, BUCKET_KEY, DEPLOYMENT_KEY } = process.env;
  if (!S3_ACCESS_KEY || !S3_SECRET_KEY || !S3_ENDPOINT || !CLOUDFLARE_ACCOUNT_ID || !BUCKET_KEY || !DEPLOYMENT_KEY) {
    throw new Error('Missing environment variables');
  }

  console.log('Starting S3');
  const client = new S3Client({
    region: 'auto',
    endpoint: S3_ENDPOINT,
    credentials: {
      accessKeyId: S3_ACCESS_KEY,
      secretAccessKey: S3_SECRET_KEY,
    },
  });

  const storageRepository = new S3StorageRepository(client, BUCKET_KEY, DEPLOYMENT_KEY);
  const memCacheRepository = new MemCacheRepository(new Map());
  const metricsRepository = new FakeMetricsRepository();
  const pmTilesService = await PMTilesService.init(
    storageRepository,
    memCacheRepository,
    metricsRepository,
    null as unknown as CloudflareD1Repository,
  );

  const runD1Query = async (sql: string, db: DBS) => {
    const body = JSON.stringify({ sql, db });
    //TODO: Handle failures due to previously failed request that still wrote to the database (primary key conflict)
    const fetchRetry = fetchBuilder(fetch);
    const resp = await fetchRetry(`https://tiles-d1-proxy.immich.cloud`, {
      headers: {
        Authorization: `Bearer ${process.env.CLOUDFLARE_API_TOKEN}`,
        ContentType: 'application/json',
      },
      body,
      method: 'POST',
      retryDelay: 1000,
      retries: 5,
      retryOn: (attempt, error, response) => {
        if (!response?.ok && attempt < 5) {
          console.log('Retrying query', attempt, error, response);
          return true;
        }
        return false;
      },
    });

    if (!resp.ok) {
      console.log(body);
      throw Error('Query failed with status ' + resp.status + (await resp.text()));
    }

    const response = (await resp.json()) as {
      success: boolean;
      errors?: any[];
      meta: { duration: number };
      results: [];
    };

    if (response.errors && response.errors.length > 0) {
      throw Error('Query failed with errors ' + JSON.stringify(response.errors));
    }

    if (!response!.success) {
      throw Error('Query failed with success false');
    }

    // console.log(`Query success in ${response.meta.duration}ms`)
    return response;
  };

  const runD1QueryAllDBs = async (sql: string) => {
    const promises: Promise<unknown>[] = [];

    for (const db of Object.values(DBS)) {
      promises.push(runD1Query(sql, db));
    }

    await Promise.all(promises);
  };

  const [header, root] = await pmTilesService.getHeaderAndRootFromSource();
  let countR2 = 0;
  let totalD1 = 0;
  let total = 0;
  let countD1 = 0;
  const promises: Promise<unknown>[] = [];
  const d1Promises: Promise<unknown>[] = [];
  const s3Limit = pLimit(5);
  const d1Limit = pLimit(50);

  const dropTableStatement = `DROP TABLE IF EXISTS tmp`;

  const createTableStatement = `CREATE TABLE IF NOT EXISTS tmp (
    startTileId INTEGER NOT NULL PRIMARY KEY,
    entry TEXT NOT NULL
  ) STRICT;`;

  mkdirSync('sql', { recursive: true });
  const files = readdirSync('sql');
  for (const file of files) {
    rmSync(join('sql', file));
  }

  await runD1QueryAllDBs(dropTableStatement);
  await runD1QueryAllDBs(createTableStatement);

  let queryString = '';
  let d1Queue = 0;

  for (const entry of root.entries) {
    const call = async () => {
      while (d1Queue > 1000) {
        await setTimeout(1000);
      }
      const directory = await getDirectory(
        entry.length,
        entry.offset + header.leafDirectoryOffset,
        storageRepository,
        header,
      );

      const totalChunks = Math.ceil(directory.entries.length / 50);
      for (let chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++) {
        const chunkEntries = directory.entries.slice(chunkIndex * 50, (chunkIndex + 1) * 50);
        const directoryChunk = {
          offsetStart: chunkEntries[0].offset,
          tileIdStart: chunkEntries[0].tileId,
          entries: chunkEntries,
        };

        const startTileId = chunkEntries[0].tileId;

        const stream = DirectoryString.fromDirectory(directoryChunk);
        const entryValue = stream.toString();

        queryString += `INSERT INTO tmp (startTileId, entry) VALUES (${startTileId}, '${entryValue}');`;

        if (queryString.length > 90000) {
          ++totalD1;
          const query = queryString;
          ++d1Queue;
          const d1Call = async () => {
            await runD1QueryAllDBs(query);
            ++countD1;
            --d1Queue;
            if (countD1 % 100 === 0) {
              console.log('D1 Progress: ' + countD1 + '/' + totalD1);
            }
          };
          d1Promises.push(d1Limit(d1Call));
          queryString = '';
        }
      }

      countR2++;
      console.log('R2 Progress: ' + countR2 + '/' + total);
    };

    promises.push(s3Limit(call));
    total++;
  }

  await Promise.all(promises);
  await Promise.all(d1Promises);

  for (const db of Object.values(DBS)) {
    const call = async () => {
      const tables = await runD1Query(`PRAGMA table_list`, db);
      const tableExists = tables.results.some(
        (table: { name: string }) => table.name === `cache_entries_${DEPLOYMENT_KEY}`,
      );
      if (tableExists) {
        await runD1Query(
          `ALTER TABLE cache_entries_${DEPLOYMENT_KEY} RENAME TO cache_entries_${DEPLOYMENT_KEY}_old;`,
          db,
        );
      }
      await runD1Query(`ALTER TABLE tmp RENAME TO cache_entries_${DEPLOYMENT_KEY};`, db);
      console.log('Tables switched', db);
      if (tableExists) {
        console.log('Waiting 10 seconds before dropping old table', db);
        await setTimeout(10000);
        console.log('Dropping old table', db);
        await runD1Query(`DROP TABLE cache_entries_${DEPLOYMENT_KEY}_old;`, db);
        console.log('Dropped old table', db);
      }
    };
    promises.push(call());
  }

  await Promise.all(promises);
};

process.on('uncaughtException', (e) => {
  console.error('UNCAUGHT EXCEPTION');
  console.error('stack', e);
  process.exit(1);
});

handler()
  .then(() => console.log('Done'))
  .catch((e) => {
    console.error('Error', e);
    throw e;
  });
