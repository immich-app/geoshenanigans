import { S3Client } from '@aws-sdk/client-s3';
import { gunzipSync } from 'fflate';
import pLimit from 'p-limit';
import { AsyncFn, IKeyValueRepository, IMetricsRepository, IStorageRepository, Operation } from './interface';
import { DirectoryStream, PMTilesService } from './pmtiles/pmtiles.service';
import { Compression, Directory, Header } from './pmtiles/types';
import { deserializeIndex, getDirectoryCacheKey } from './pmtiles/utils';
import { MemCacheRepository, S3StorageRepository } from './repository';

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
    FILE_HASH,
  } = process.env;
  if (
    !S3_ACCESS_KEY ||
    !S3_SECRET_KEY ||
    !S3_ENDPOINT ||
    !KV_API_KEY ||
    !CLOUDFLARE_ACCOUNT_ID ||
    !KV_NAMESPACE_ID ||
    !BUCKET_KEY ||
    !FILE_NAME ||
    !FILE_HASH
  ) {
    throw new Error('Missing environment variables');
  }

  const client = new S3Client({
    region: 'auto',
    endpoint: S3_ACCESS_KEY,
    credentials: {
      accessKeyId: S3_ACCESS_KEY,
      secretAccessKey: S3_SECRET_KEY,
    },
  });

  const storageRepository = new S3StorageRepository(client, BUCKET_KEY, FILE_NAME, FILE_HASH);
  const memCacheRepository = new MemCacheRepository(new Map());
  const kvRepository = new FakeKVRepository();
  const metricsRepository = new FakeMetricsRepository();
  const pmTilesService = await PMTilesService.init(
    storageRepository,
    memCacheRepository,
    kvRepository,
    metricsRepository,
  );

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
        method: 'PUT',
        headers: {
          Authorization: `Bearer ${KV_API_KEY}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(toPush),
      },
    );
    const kvResponseBody = (await kvResponse.json()) as { success: boolean };
    if (!kvResponseBody.success || kvResponse.status !== 200) {
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
      const cacheKey = getDirectoryCacheKey('v1.pmtiles', 'prodv1', {
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
};

handler()
  .then(() => console.log('Done'))
  .catch((e) => console.error(e));
