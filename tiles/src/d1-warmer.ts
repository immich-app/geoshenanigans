import { S3Client } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import fetchBuilder from 'fetch-retry';
import { gunzipSync } from 'fflate';
import { createReadStream as createFileReadStream, mkdirSync, readdirSync, rmSync } from 'fs';
import pLimit, {LimitFunction} from 'p-limit';
import { join } from 'path';
import { setTimeout } from 'timers/promises';
import { AsyncFn, IMetricsRepository, IStorageRepository, Operation } from './interface';
import { DirectoryString, PMTilesService } from './pmtiles.service';
import { Compression, Directory, Header } from './pmtiles/types';
import { deserializeIndex, tileJSON } from './pmtiles/utils';
import { CloudflareD1Repository, MemCacheRepository } from './repository';

export class LocalStorageRepository implements IStorageRepository {
  constructor(private filePath: string) {}

  async getRange(range: { length: number; offset: number }): Promise<ArrayBuffer> {
    const stream = await this.getRangeAsStream(range);
    return new Response(stream).arrayBuffer();
  }

  async getRangeAsStream(range: { length: number; offset: number }): Promise<ReadableStream> {
    const fileStream = createFileReadStream(this.filePath, {
      start: range.offset,
      end: range.offset + range.length - 1,
    });

    return new ReadableStream({
      start(controller) {
        fileStream.on('data', (chunk) => {
          controller.enqueue(chunk);
        });
        fileStream.on('end', () => {
          controller.close();
        });
        fileStream.on('error', (error) => {
          controller.error(error);
        });
      },
      cancel() {
        fileStream.destroy();
      },
    });
  }

  async get(key: string): Promise<ArrayBuffer> {
    throw Error(`get(${key}) is not implemented for LocalStorageRepository`);
  }

  async getAsStream(key: string): Promise<ReadableStream> {
    throw Error(`getAsStream(${key}) is not implemented for LocalStorageRepository`);
  }

  getDeploymentKey(): string {
    return 'local_deployment_key';
  }
}

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
  DEV = 'DEV',
}

const R2_BUCKETS = [
  "tiles-wnam",
  "tiles-enam",
  "tiles-weur",
  "tiles-eeur",
  "tiles-apac",
  "tiles-oc"
]

const handler = async () => {
  const {
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    S3_ENDPOINT,
    CLOUDFLARE_ACCOUNT_ID,
    BUCKET_KEY,
    DEPLOYMENT_KEY,
    PMTILES_FILE_PATH,
  } = process.env;
  if (
    !S3_ACCESS_KEY ||
    !S3_SECRET_KEY ||
    !S3_ENDPOINT ||
    !CLOUDFLARE_ACCOUNT_ID ||
    !BUCKET_KEY ||
    !DEPLOYMENT_KEY ||
    !PMTILES_FILE_PATH
  ) {
    throw new Error('Missing environment variables');
  }

  console.log('Starting S3');
  const client = new S3Client({
    region: 'auto',
    endpoint: S3_ENDPOINT,
    forcePathStyle: true,
    requestStreamBufferSize: 32 * 1024,
    requestHandler: {
      httpsAgent: { maxSockets: 1000 },
    },
    retryMode: 'adaptive',
    maxAttempts: 10,
    credentials: {
      accessKeyId: S3_ACCESS_KEY,
      secretAccessKey: S3_SECRET_KEY,
    },
  });

  const storageRepository = new LocalStorageRepository(PMTILES_FILE_PATH);
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
      retryDelay: (attempt, error, response) => {
        console.log('Server error, retrying query', attempt, error, response);
        return 1000 * Math.pow(1.5, attempt); // Exponential backoff
      },
      retries: 10,
      retryOn: (attempt, error, response) => {
        if (!response?.ok && attempt < 10) {
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

  const r2Promises: {[key: string]: Promise<unknown>[]} = {}
  const r2Limits: { [key: string]: LimitFunction } = {};
  for(const bucketKey of R2_BUCKETS) {
    r2Limits[bucketKey] = pLimit(100);
    r2Promises[bucketKey] = [];
  }

  const uploadToS3 = async (chunk: TileDataChunk) => {
    for(const bucketKey of R2_BUCKETS) {
      const s3Call = async () => {
        const chunkData = await storageRepository.getRangeAsStream({
          length: chunk.endByte - chunk.startByte,
          offset: tileDataOffset + chunk.startByte,
        });
        const fileName = `chunk_${chunk.chunkId}.pmtiles`;
        const filePath = `${DEPLOYMENT_KEY}/${fileName}`;
        try {
          const parallelUploads = new Upload({
            client: client,
            params: {
              Bucket: bucketKey,
              Key: filePath,
              Body: chunkData,
            },
            queueSize: 1, // Concurrently upload 1 part
            partSize: 1024 * 1024 * 5, // Set part size to 256KB
          });

          await parallelUploads.done();

          if (chunk.chunkId % 100 === 0) {
            console.log(`S3 Progress (${bucketKey}): ${chunk.chunkId}/${Object.keys(tileDataChunks).length}`);
          }
        } catch (e) {
          console.error(`Failed to upload chunk ${chunk.chunkId} to S3`, e);
          throw e;
        }
      }
      r2Promises[bucketKey].push(r2Limits[bucketKey](s3Call));
    }
  }

  const [header, root] = await pmTilesService.getHeaderAndRootFromSource();
  const metadata = await pmTilesService.getMetadata(header);
  const json = tileJSON({ header, metadata, version: 'v1' });
  let loadingCount = 0;
  let totalD1 = 0;
  let countD1 = 0;
  const d1Promises: Promise<unknown>[] = [];
  const d1Limit = pLimit(3);

  const dropTableStatement = `DROP TABLE IF EXISTS tmp`;

  const createTableStatement = `CREATE TABLE IF NOT EXISTS tmp (
    startTileId INTEGER NOT NULL PRIMARY KEY,
    entry TEXT NOT NULL
  ) STRICT;`;

  const addJsonStatement = `INSERT INTO tmp (startTileId, entry) VALUES (-1, '${JSON.stringify(json)}');`;

  mkdirSync('sql', { recursive: true });
  const files = readdirSync('sql');
  for (const file of files) {
    rmSync(join('sql', file));
  }

  console.log('Preparing D1 database');
  await runD1QueryAllDBs(dropTableStatement);
  await runD1QueryAllDBs(createTableStatement);
  await runD1QueryAllDBs(addJsonStatement);
  console.log('D1 database prepared');

  let queryString = '';
  let d1Queue = 0;

  /** TODO: Remaining tasks for tile data splitting
   - Store v1.json data in D1 for quick access, cache on startup
   - Current header and root memcache can be removed
   - Pull the pmtiles file as part of the warming process and do processing on disk
   - Push chunks to S3, add header.tileDataOffset to the startByte and endByte for each chunk
   - Alter worker code to have concept of chunking when retrieving the tile data.
     - Needs to pull from correct index and understand different offset
   - Would D1 be smaller if tile offsets were always max 25_000_000 but included their chunk index? Probably?
     - Would remove offset for the chunk, and instead just have every tile offset from the index of the data chunk it is in
   - Should setup D1 to create a new database for each deployment, requires some terraform magic?
   - Race tigris alongside R2 with metrics to see which is faster for tile retrieval, only TTFB possible?
   - Automate warming process in github actions
  **/

  type TileDataChunk = { chunkId: number; startByte: number; endByte: number }

  const tileDataOffset = header.tileDataOffset;
  const tileDataLength = header.tileDataLength;
  const tileDataChunks: { [chunkId: number]: TileDataChunk } = {};
  const tileDataChunkSize = 1_000_000;

  if (!tileDataLength) {
    throw new Error('Tile data length undefined from header');
  }

  let currentChunk: TileDataChunk | undefined;

  for (const entry of root.entries) {
    while (d1Queue > 1000) {
      await setTimeout(1000);
    }

    const directory = await getDirectory(
      entry.length,
      entry.offset + header.leafDirectoryOffset,
      storageRepository,
      header,
    );

    const maxChunk = 0;

    for (const dirEntry of directory.entries) {
      const tileStartByte = dirEntry.offset;
      const tileEndByte = tileStartByte + dirEntry.length;
      const tileChunkId = Math.floor(tileStartByte / tileDataChunkSize);
      if(currentChunk && tileChunkId < currentChunk.chunkId) {
        continue;
      }

      if(tileChunkId != currentChunk?.chunkId) {

        if(currentChunk) {
          await uploadToS3(currentChunk);
        }

        if(!tileDataChunks[tileChunkId]) {
          tileDataChunks[tileChunkId] = {
            chunkId: tileChunkId,
            startByte: tileStartByte,
            endByte: tileEndByte,
          };
        }
        currentChunk = tileDataChunks[tileChunkId];
      }

      if (tileStartByte < currentChunk.startByte) {
        throw new Error('Tile start byte is less than current chunk start byte');
      }
      if (tileEndByte > currentChunk.endByte) {
        if(tileChunkId < maxChunk) {
          console.log('Chunk end byte is greater than current chunk end byte, this should not happen', tileChunkId, tileStartByte, tileEndByte, currentChunk);
        }
        currentChunk.endByte = tileEndByte;
      }
    }

    const totalChunks = Math.ceil(directory.entries.length / 50);
    for (let chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++) {
      const chunkEntries = directory.entries.slice(chunkIndex * 50, (chunkIndex + 1) * 50);
      const newChunkEntries = chunkEntries.map((entry) => {
        const chunkId = Math.floor((entry.offset) / tileDataChunkSize);
        return {
          tileId: entry.tileId,
          offset: entry.offset - tileDataChunks[chunkId].startByte,
          chunkId,
          length: entry.length,
          runLength: entry.runLength,
        }});
      const directoryChunk = {
        offsetStart: newChunkEntries[0].offset,
        tileIdStart: newChunkEntries[0].tileId,
        entries: newChunkEntries,
      };

      const startTileId = chunkEntries[0].tileId;

      const entryValue = DirectoryString.fromDirectory(directoryChunk).toString();

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

    loadingCount++;
    console.log('Loading Progress: ' + loadingCount + '/' + root.entries.length);
  }

  for(const promises of Object.values(r2Promises)) {
    await Promise.all(promises);
  }
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
    d1Promises.push(call());
  }

  await Promise.all(d1Promises);
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
