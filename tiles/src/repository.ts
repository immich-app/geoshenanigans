import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { Point } from '@influxdata/influxdb-client';
import {
  AsyncFn,
  IDeferredRepository,
  IKeyValueRepository,
  IMemCacheRepository,
  IMetricsRepository,
  IStorageRepository,
  Operation,
  Options,
} from './interface';
import { monitorAsyncFunction } from './monitor';

export class CloudflareKVRepository implements IKeyValueRepository {
  constructor(private KV: KVNamespace) {}

  async get(key: string): Promise<string | undefined> {
    const value = await this.KV.get(key, { type: 'text', cacheTtl: 2678400 });
    return value ?? undefined;
  }

  async getAsStream(key: string): Promise<ReadableStream | undefined> {
    const stream = await this.KV.get(key, { type: 'stream', cacheTtl: 2678400 });
    return stream ?? undefined;
  }
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
    private fileHash: string,
  ) {}

  getFileHash(): string {
    return this.fileHash;
  }

  getFileName(): string {
    return this.fileName;
  }

  private async getR2Object(range: { offset: number; length: number }) {
    const { offset, length } = range;
    console.log(this.fileName);
    const resp = await this.bucket.get(this.fileName, {
      range: { offset, length },
    });
    if (!resp) {
      throw new Error('Archive not found');
    }
    return resp;
  }

  async get(range: { offset: number; length: number }): Promise<ArrayBuffer> {
    return (await this.getR2Object(range)).arrayBuffer();
  }

  async getAsStream(range: { offset: number; length: number }): Promise<ReadableStream> {
    return (await this.getR2Object(range)).body;
  }
}

export class S3StorageRepository implements IStorageRepository {
  constructor(
    private client: S3Client,
    private bucketKey: string,
    private fileName: string,
    private fileHash: string,
  ) {}

  private async getS3Object(range: { offset: number; length: number }) {
    const command = new GetObjectCommand({
      Bucket: this.bucketKey,
      Key: this.fileName,
      Range: `bytes=${range.offset}-${range.offset + range.length - 1}`,
    });
    const response = await this.client.send(command);
    const data = response.Body;
    if (!data) {
      throw new Error('Data not found for range ' + JSON.stringify(range));
    }
    return data;
  }

  async get(range: { length: number; offset: number }): Promise<ArrayBuffer> {
    const data = await this.getS3Object(range);
    return (await data.transformToByteArray()).buffer;
  }

  async getAsStream(range: { length: number; offset: number }): Promise<ReadableStream> {
    const data = await this.getS3Object(range);
    return data.transformToWebStream();
  }

  getFileName(): string {
    return this.fileName;
  }

  getFileHash(): string {
    return this.fileHash;
  }
}

export class CloudflareDeferredRepository implements IDeferredRepository {
  deferred: AsyncFn[] = [];
  constructor(private ctx: ExecutionContext) {}

  defer(call: AsyncFn): void {
    this.deferred.push(call);
  }

  runDeferred() {
    for (const call of this.deferred) {
      this.ctx.waitUntil(call());
    }
  }
}

export class CloudflareMetricsRepository implements IMetricsRepository {
  private readonly defaultTags: { [key: string]: string };

  constructor(
    private operationPrefix: string,
    request: Request<unknown, IncomingRequestCfProperties>,
    private deferredRepository: IDeferredRepository,
    private env: WorkerEnv,
  ) {
    this.defaultTags = {
      continent: request.cf?.continent ?? '',
      colo: request.cf?.colo ?? '',
      asOrg: request.cf?.asOrganization ?? '',
      scriptTag: env.CF_VERSION_METADATA.tag,
      scriptId: env.CF_VERSION_METADATA.id,
    };
  }

  monitorAsyncFunction<T extends AsyncFn>(
    operation: Operation,
    call: T,
    options: Options = {},
  ): (...args: Parameters<T>) => Promise<Awaited<ReturnType<T>>> {
    const callback = (point: Point) => {
      const influxLineProtocol = point.toLineProtocol()?.toString();
      if (this.env.ENVIRONMENT === 'production') {
        this.deferredRepository.defer(async () => {
          const response = await fetch('https://cf-workers.monitoring.immich.cloud/write', {
            method: 'POST',
            body: influxLineProtocol,
            headers: {
              Authorization: `Token ${this.env.VMETRICS_API_TOKEN}`,
            },
          }),
        );
      } else {
        console.log(influxLineProtocol);
      }
    };

    return monitorAsyncFunction(this.operationPrefix, operation, call, callback, options);
  }
}
