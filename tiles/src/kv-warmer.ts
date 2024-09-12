import { S3Client } from '@aws-sdk/client-s3';
import fetch_retry from 'fetch-retry';
import { gunzipSync } from 'fflate';
import pLimit from 'p-limit';
import { AsyncFn, IKeyValueRepository, IMetricsRepository, IStorageRepository, Operation } from './interface';
import { DirectoryStream, PMTilesService } from './pmtiles/pmtiles.service';
import { Compression, Directory, Header } from './pmtiles/types';
import { deserializeIndex, getDirectoryCacheKey } from './pmtiles/utils';
import { MemCacheRepository, S3StorageRepository } from './repository';

const fetch = fetch_retry(global.fetch);

class FakeKVRepository implements IKeyValueRepository {
  constructor() {}

  async get(): Promise<string | undefined> {
    return undefined;
  }

  async getAsStream(): Promise<ReadableStream | undefined> {
    return undefined;
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
  const resp = await source.get({ offset, length });
  const data = decompress(resp, header.internalCompression);
  const entries = deserializeIndex(await new Response(data).arrayBuffer());
  if (entries.length === 0) {
    throw new Error('Empty directory is invalid');
  }
  const directory: Directory = { offsetStart: entries[0].offset, tileIdStart: entries[0].tileId, entries };
  return directory;
};

const handler = async () => {
  const {
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    S3_ENDPOINT,
    KV_API_KEY,
    CLOUDFLARE_ACCOUNT_ID,
    KV_NAMESPACE_ID,
    BUCKET_KEY,
    FILE_NAME,
  } = process.env;
  if (
    !S3_ACCESS_KEY ||
    !S3_SECRET_KEY ||
    !S3_ENDPOINT ||
    !KV_API_KEY ||
    !CLOUDFLARE_ACCOUNT_ID ||
    !KV_NAMESPACE_ID ||
    !BUCKET_KEY ||
    !FILE_NAME
  ) {
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

  const storageRepository = new S3StorageRepository(client, BUCKET_KEY, FILE_NAME);
  const memCacheRepository = new MemCacheRepository(new Map());
  const kvRepository = new FakeKVRepository();
  const metricsRepository = new FakeMetricsRepository();
  const pmTilesService = await PMTilesService.init(
    storageRepository,
    memCacheRepository,
    kvRepository,
    metricsRepository,
  );

  console.log('Checking if already warmed');
  const kvCheckKey = encodeURIComponent(`${FILE_NAME}|kv-warmed`);
  console.log(kvCheckKey);
  console.log(
    'url',
    `https://api.cloudflare.com/client/v4/accounts/${CLOUDFLARE_ACCOUNT_ID}/storage/kv/namespaces/${KV_NAMESPACE_ID}/values/${kvCheckKey}`,
  );
  const kvCheckResponse = await fetch(
    `https://api.cloudflare.com/client/v4/accounts/${CLOUDFLARE_ACCOUNT_ID}/storage/kv/namespaces/${KV_NAMESPACE_ID}/values/${kvCheckKey}`,
    {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${KV_API_KEY}`,
        'Content-Type': 'application/json',
      },
    },
  );
  if (kvCheckResponse.status === 200) {
    console.log('Already warmed');
    return;
  }

  if (kvCheckResponse.status !== 404) {
    console.error('KV Check Failed');
    throw new Error('KV Check Failed, status code ' + kvCheckResponse.status);
  }

  console.log('Not warmed', kvCheckResponse.status);

  const [header, root] = await pmTilesService.getHeaderAndRootFromSource();
  let countR2 = 0;
  let total = 0;
  let countKV = 0;
  const promises: Promise<void>[] = [];
  const toPushToKV: { value: string; key: string }[] = [];
  const kvPromises: Promise<void>[] = [];
  const limit = pLimit(10);
  const kvLimit = pLimit(30);

  const bulkKVPush = async (toPushOverride?: object[]) => {
    if (toPushToKV.length < 10 && !toPushOverride) {
      return;
    }
    const toPush = toPushOverride ?? toPushToKV.splice(0, 10);
    const kvResponse = await fetch(
      `https://api.cloudflare.com/client/v4/accounts/${CLOUDFLARE_ACCOUNT_ID}/storage/kv/namespaces/${KV_NAMESPACE_ID}/bulk`,
      {
        retries: 5,
        retryDelay: 2000,
        retryOn: function (attempt, error, response) {
          // retry on any network error, or 4xx or 5xx status codes
          if (error !== null || (!!response && response.status >= 400)) {
            console.log(`retrying, attempt number ${attempt + 1}`);
            return true;
          }
          return false;
        },
        method: 'PUT',
        headers: {
          Authorization: `Bearer ${KV_API_KEY}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(toPush),
      },
    );
    if (!kvResponse.ok) {
      console.error('KV Put Failed', kvResponse);
      throw new Error('KV Put Failed with non-200 status code');
    }
    const kvResponseBody = (await kvResponse.json()) as { success: boolean };
    if (!kvResponseBody.success) {
      console.error('KV Put Failed', kvResponseBody);
      throw new Error('KV Put Failed');
    }
    if (toPushToKV.length !== 0) {
      console.log('Remaining in KV queue', toPushToKV.length);
    }

    countKV += toPush.length;
    console.log('KV Progress: ' + countKV + '/' + total);
  };

  for (const entry of root.entries) {
    const call = async () => {
      const directory = await getDirectory(
        entry.length,
        entry.offset + header.leafDirectoryOffset,
        storageRepository,
        header,
      );
      const stream = DirectoryStream.fromDirectory(directory);
      const cacheKey = getDirectoryCacheKey(FILE_NAME, {
        offset: entry.offset + header.leafDirectoryOffset,
        length: entry.length,
      });

      toPushToKV.push({ key: cacheKey, value: await stream.toString() });
      countR2++;
      console.log('R2 Progress: ' + countR2 + '/' + total);
      kvPromises.push(kvLimit(bulkKVPush));
      console.log('Remaining in KV queue', toPushToKV.length);
    };
    promises.push(limit(call));
    total++;
  }

  await Promise.all(promises);
  while (toPushToKV.length > 0) {
    const kvToPush = toPushToKV.splice(0, 10);
    kvPromises.push(kvLimit(() => bulkKVPush(kvToPush)));
    console.log('Remaining in KV queue', toPushToKV.length);
  }
  await Promise.all(kvPromises);
  console.log('Done');
  const kvResponse = await fetch(
    `https://api.cloudflare.com/client/v4/accounts/${CLOUDFLARE_ACCOUNT_ID}/storage/kv/namespaces/${KV_NAMESPACE_ID}/values/${kvCheckKey}`,
    {
      method: 'PUT',
      headers: {
        Authorization: `Bearer ${KV_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ value: 'warmed' }),
    },
  );
  if (!kvResponse.ok) {
    console.error('Write KV Success Failed');
    throw new Error('Write KV Success Failed');
  }
};

process.on('uncaughtException', (e) => {
  console.error('UNCAUGHT EXCEPTION');
  console.error('stack', e);
  process.exit(1);
});

handler()
  .then(() => console.log('Done'))
  .catch((e) => console.error(e));
