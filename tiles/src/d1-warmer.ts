import { S3Client } from '@aws-sdk/client-s3';
import { gunzipSync } from 'fflate';
import { appendFileSync, writeFileSync, mkdirSync, readdirSync, rmSync } from 'fs';
import { join } from 'path';
import pLimit from 'p-limit';
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

  const [header, root] = await pmTilesService.getHeaderAndRootFromSource();
  let countR2 = 0;
  let total = 0;
  const promises: Promise<void>[] = [];
  const limit = pLimit(5);

  const dropTableStatement = `DROP TABLE IF EXISTS cache_entries_${DEPLOYMENT_KEY}_tmp;\n`;

  const createTableStatement = `CREATE TABLE IF NOT EXISTS cache_entries_${DEPLOYMENT_KEY}_tmp (
    startTileId INTEGER NOT NULL PRIMARY KEY,
    entry TEXT NOT NULL
  ) STRICT;`;

  mkdirSync('sql', { recursive: true });
  const files = readdirSync('sql');
  for (const file of files) {
    rmSync(join('sql', file));
  }

  writeFileSync('sql/cache_entries.0.sql', dropTableStatement);
  appendFileSync('sql/cache_entries.0.sql', createTableStatement);

  for (const entry of root.entries) {
    const call = async () => {
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

        const insertStatement = `\nINSERT INTO cache_entries_${DEPLOYMENT_KEY}_tmp (startTileId, entry) VALUES (${startTileId}, '${entryValue}');`;
        appendFileSync(`sql/cache_entries.${Math.floor(countR2 / 50) + 1}.sql`, insertStatement);
      }

      countR2++;
      console.log('R2 Progress: ' + countR2 + '/' + total);
    };

    promises.push(limit(call));
    total++;
  }

  await Promise.all(promises);
  appendFileSync(`sql/cache_entries.${Math.floor(countR2 / 50) + 2}.sql`, `ALTER TABLE IF EXISTS cache_entries_${DEPLOYMENT_KEY} RENAME TO cache_entries_${DEPLOYMENT_KEY}_old;\n`);
  appendFileSync(`sql/cache_entries.${Math.floor(countR2 / 50) + 2}.sql`, `ALTER TABLE cache_entries_${DEPLOYMENT_KEY}_tmp RENAME TO cache_entries_${DEPLOYMENT_KEY}\n;`);
  appendFileSync(`sql/cache_entries.${Math.floor(countR2 / 50) + 2}.sql`, `DROP TABLE IF EXISTS cache_entries_${DEPLOYMENT_KEY}_old;\n`);
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
