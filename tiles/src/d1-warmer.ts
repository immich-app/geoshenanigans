import { S3Client } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import fetchBuilder from 'fetch-retry';
import { createReadStream as createFileReadStream } from 'fs';
import pLimit, { LimitFunction } from 'p-limit';
import { setTimeout } from 'timers/promises';
import { DirectoryString } from './pmtiles.service';
import { Compression, Header, JsonResponse, Metadata } from './pmtiles/types';
import { bytesToHeader, decompress, deserializeIndex, tileJSON } from './pmtiles/utils';

/** TODO: Remaining tasks for tile data splitting
   - Should setup D1 to create a new database for each deployment, requires some terraform magic?
   - Race tigris alongside R2 with metrics to see which is faster for tile retrieval, only TTFB possible?
   - Automate warming process in github actions
  **/

const db = 'DEV';

type Chunk = { chunkId: number; startByte: number; endByte: number };

const HEADER_SIZE_BYTES = 127;
const MAX_CHUNK_FILE_SIZE_BYTES = 1_000_000;
const BUCKETS = [
  // { key: 'tiles-wnam', client: 'r2' },
  // { key: 'tiles-enam', client: 'r2' },
  // { key: 'tiles-weur', client: 'r2' },
  // { key: 'tiles-eeur', client: 'r2' },
  // { key: 'tiles-apac', client: 'r2' },
  // { key: 'tiles-oc', client: 'r2' },
  { key: 'geo', client: 'tigris' },
];

const {
  S3_ACCESS_KEY,
  S3_SECRET_KEY,
  S3_ENDPOINT,
  TIGRIS_KEY_ID,
  TIGRIS_ACCESS_KEY,
  TIGRIS_ENDPOINT,
  CLOUDFLARE_ACCOUNT_ID,
  DEPLOYMENT_KEY,
  PMTILES_FILE_PATH,
} = process.env;
if (
  !S3_ACCESS_KEY ||
  !S3_SECRET_KEY ||
  !S3_ENDPOINT ||
  !TIGRIS_KEY_ID ||
  !TIGRIS_ACCESS_KEY ||
  !TIGRIS_ENDPOINT ||
  !CLOUDFLARE_ACCOUNT_ID ||
  !DEPLOYMENT_KEY ||
  !PMTILES_FILE_PATH
) {
  throw new Error('Missing environment variables');
}

const r2Client = new S3Client({
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

const tigrisClient = new S3Client({
  region: 'auto',
  endpoint: TIGRIS_ENDPOINT,
  forcePathStyle: false,
  requestHandler: {
    httpsAgent: { maxSockets: 1000 },
  },
  retryMode: 'adaptive',
  maxAttempts: 10,
  credentials: {
    accessKeyId: TIGRIS_KEY_ID,
    secretAccessKey: TIGRIS_ACCESS_KEY,
  },
});

const d1Promises: Promise<unknown>[] = [];
const d1Limit = pLimit(25);
const r2Promises: { [key: string]: Promise<unknown>[] } = {};
const r2Limits: { [key: string]: LimitFunction } = {};

const getRangeAsStream = (range: { length: number; offset: number }): ReadableStream => {
  const fileStream = createFileReadStream(PMTILES_FILE_PATH, {
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
};

const getRange = async (range: { length: number; offset: number }) =>
  new Response(getRangeAsStream(range)).arrayBuffer();

const getMetadata = async (header: Header): Promise<Metadata> => {
  const resp = await getRange({
    offset: header.jsonMetadataOffset,
    length: header.jsonMetadataLength,
  });
  const decompressed = await decompress(resp, header.internalCompression);
  const dec = new TextDecoder('utf-8');
  return JSON.parse(dec.decode(decompressed));
};

const getEntriesFromRange = async (offset: number, length: number, compression: Compression) => {
  const bytes = await getRange({ offset, length });
  const entries = deserializeIndex(await new Response(await decompress(bytes, compression)).arrayBuffer());
  if (entries.length === 0) {
    throw new Error('Empty directory is invalid');
  }

  return entries;
};

const runD1Query = async (sql: string) => {
  const body = JSON.stringify({ sql, db });
  //TODO: Handle failures due to previously failed request that still wrote to the database (primary key conflict)
  const fetchRetry = fetchBuilder(fetch);
  const response = await fetchRetry(`https://tiles-d1-proxy.immich.cloud`, {
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

  if (!response.ok) {
    console.log(body);
    throw Error('Query failed with status ' + response.status + (await response.text()));
  }

  const responseBody = (await response.json()) as {
    success: boolean;
    errors?: any[];
    meta: { duration: number };
    results: [];
  };

  if (responseBody.errors && responseBody.errors.length > 0) {
    throw Error('Query failed with errors ' + JSON.stringify(responseBody.errors));
  }

  if (!responseBody!.success) {
    throw Error('Query failed with success false');
  }

  // console.log(`Query success in ${response.meta.duration}ms`)
  return responseBody;
};

const prepareDatabase = async (json: JsonResponse) => {
  await runD1Query(`DROP TABLE IF EXISTS tmp`);
  await runD1Query(`
    CREATE TABLE IF NOT EXISTS tmp (
      startTileId INTEGER NOT NULL PRIMARY KEY,
      entry TEXT NOT NULL
    ) STRICT;`);
  await runD1Query(`INSERT INTO tmp (startTileId, entry) VALUES (-1, '${JSON.stringify(json)}');`);
};

const finalizeDatabase = async () => {
  const tableName = `cache_entries_${DEPLOYMENT_KEY}`;
  const tables = await runD1Query(`PRAGMA table_list`);
  const tableExists = tables.results.some((table: { name: string }) => table.name === tableName);
  if (tableExists) {
    await runD1Query(`ALTER TABLE ${tableName} RENAME TO ${tableName}_old;`);
  }

  await runD1Query(`ALTER TABLE tmp RENAME TO ${tableName};`);
  console.log('Tables switched');

  if (tableExists) {
    console.log('Waiting 10 seconds before dropping old table');
    await setTimeout(10000);
    console.log('Dropping old table');
    await runD1Query(`DROP TABLE ${tableName}_old;`);
    console.log('Dropped old table');
  }
};

const uploadToS3 = async (chunk: Chunk, header: Header, chunkMap: object) => {
  for (const bucket of BUCKETS) {
    const s3Call = async () => {
      const bytes = getRangeAsStream({
        offset: header.tileDataOffset + chunk.startByte,
        length: chunk.endByte - chunk.startByte,
      });

      try {
        const parallelUploads = new Upload({
          client: bucket.client === 'r2' ? r2Client : tigrisClient,
          params: {
            Bucket: bucket.key,
            Key: `${DEPLOYMENT_KEY}/chunk_${chunk.chunkId}.pmtiles`,
            Body: bytes,
          },
          queueSize: 1,
          partSize: 1024 * 1024 * 5,
        });

        await parallelUploads.done();

        if (chunk.chunkId % 100 === 0) {
          console.log(`S3 Progress (${bucket.key}): ${chunk.chunkId}/${Object.keys(chunkMap).length}`);
        }
      } catch (e) {
        console.error(`Failed to upload chunk ${chunk.chunkId} to S3`, e);
        throw e;
      }
    };

    r2Promises[bucket.key].push(r2Limits[bucket.key](s3Call));
  }
};

const handler = async () => {
  for (const bucket of BUCKETS) {
    r2Limits[bucket.key] = pLimit(bucket.client === 'r2' ? 100 : 1000);
    r2Promises[bucket.key] = [];
  }

  const headerBytes = await getRange({ offset: 0, length: HEADER_SIZE_BYTES });
  if (new DataView(headerBytes).getUint16(0, true) !== 0x4d50) {
    throw new Error('Wrong magic number for PMTiles archive');
  }

  const header = await bytesToHeader(headerBytes);
  const rootEntries = await getEntriesFromRange(
    header.rootDirectoryOffset,
    header.rootDirectoryLength,
    header.internalCompression,
  );

  console.log('Preparing D1 database');

  const metadata = await getMetadata(header);
  await prepareDatabase(tileJSON({ header, metadata, version: 'v1' }));

  let queryString = '';
  let loadingCount = 0;
  let totalD1 = 0;
  let countD1 = 0;
  let d1Queue = 0;
  let currentChunk: Chunk | undefined;

  const chunkMap: { [chunkId: number]: Chunk } = {};

  for (const rootEntry of rootEntries) {
    while (d1Queue > 1000) {
      await setTimeout(1000);
    }

    const entries = await getEntriesFromRange(
      header.leafDirectoryOffset + rootEntry.offset,
      rootEntry.length,
      header.internalCompression,
    );

    for (const entry of entries) {
      const startByte = entry.offset;
      const endByte = startByte + entry.length;
      const chunkId = Math.floor(startByte / MAX_CHUNK_FILE_SIZE_BYTES);

      // some entries reference bytes that are in previous chunks (but never change the boundaries)
      if (currentChunk && chunkId < currentChunk.chunkId) {
        continue;
      }

      if (chunkId != currentChunk?.chunkId) {
        if (!chunkMap[chunkId]) {
          chunkMap[chunkId] = {
            chunkId: chunkId,
            startByte: startByte,
            endByte: endByte,
          };
        }

        if (currentChunk) {
          await uploadToS3(currentChunk, header, chunkMap);
        }

        currentChunk = chunkMap[chunkId];
      }

      if (startByte < currentChunk.startByte) {
        throw new Error('Tile start byte is less than current chunk start byte');
      }

      if (endByte > currentChunk.endByte) {
        currentChunk.endByte = endByte;
      }
    }

    await uploadToS3(currentChunk!, header, chunkMap);

    const totalChunks = Math.ceil(entries.length / 50);
    for (let chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++) {
      const chunkEntries = entries.slice(chunkIndex * 50, (chunkIndex + 1) * 50);
      const newChunkEntries = chunkEntries.map((entry) => {
        const chunkId = Math.floor(entry.offset / MAX_CHUNK_FILE_SIZE_BYTES);
        return {
          tileId: entry.tileId,
          offset: entry.offset - chunkMap[chunkId].startByte,
          chunkId,
          length: entry.length,
          runLength: entry.runLength,
        };
      });

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
          await runD1Query(query);
          ++countD1;
          --d1Queue;
          if (countD1 % 25 === 0) {
            console.log('D1 Progress: ' + countD1 + '/' + totalD1);
          }
        };
        d1Promises.push(d1Limit(d1Call));
        queryString = '';
      }
    }

    loadingCount++;
    console.log('Loading Progress: ' + loadingCount + '/' + rootEntries.length);
  }

  for (const promises of Object.values(r2Promises)) {
    await Promise.all(promises);
  }

  await Promise.all(d1Promises);

  await finalizeDatabase();
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
