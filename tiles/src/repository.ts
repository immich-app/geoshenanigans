import { IKeyValueRepository, IMemCacheRepository, IStorageRepository } from './interface';
import { Metrics } from './monitor';

export class CloudflareKVRepository implements IKeyValueRepository {
  constructor(private KV: KVNamespace) {}

  async put(key: string, value: string): Promise<void> {
    await Metrics.getMetrics().monitorAsyncFunction({ name: 'kv-put-value', extraTags: { key } }, this.KV.put)(
      key,
      value,
    );
  }

  async get(key: string): Promise<string | undefined> {
    const value = await Metrics.getMetrics().monitorAsyncFunction(
      { name: 'kv-get-string', extraTags: { key } },
      (key) => this.KV.get(key, { type: 'text', cacheTtl: 2678400 }),
    )(key);
    return value ?? undefined;
  }

  async getAsStream(key: string): Promise<ReadableStream | undefined> {
    const stream = await Metrics.getMetrics().monitorAsyncFunction(
      { name: 'kv-get-stream', extraTags: { key } },
      (key) => this.KV.get(key, { type: 'stream', cacheTtl: 2678400 }),
    )(key);

    return stream ?? undefined;
  }
}

/* eslint-disable no-var */
declare global {
  var memCache: Map<string, unknown>;
}
/* eslint-enable no-var */

if (!globalThis.memCache) {
  globalThis.memCache = new Map<string, unknown>();
}

export class MemCacheRepository implements IMemCacheRepository {
  constructor(private globalCache: Map<string, unknown>) {}
  set<T>(key: string, value: T): void {
    this.globalCache.set(key, value);
  }

  get<T>(key: string): T | undefined {
    return this.globalCache.get(key) as T;
  }
}

export class R2StorageRepository implements IStorageRepository {
  constructor(
    private bucket: R2Bucket,
    private fileName: string,
  ) {}

  getFileName(): string {
    return this.fileName;
  }

  async get(range: { offset: number; length: number }): Promise<ArrayBuffer> {
    const { offset, length } = range;
    const resp = await this.bucket.get(this.fileName, {
      range: { offset: offset, length: length },
    });
    if (!resp) {
      throw new Error('Archive not found');
    }

    const o = resp as R2ObjectBody;

    return o.arrayBuffer();
  }
}
