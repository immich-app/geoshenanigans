import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { Point } from '@influxdata/influxdb-client';
import {
  AsyncFn,
  IDeferredRepository,
  IKeyValueRepository,
  IMemCacheRepository,
  IMetricsProviderRepository,
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

export class HeaderMetricsProvider implements IMetricsProviderRepository {
  private _metrics: string[] = [];
  constructor() {}

  pushMetric(metric: Metric) {
    for (const [_, { value, type }] of metric.fields) {
      if (type === 'duration') {
        this._metrics.push(`${metric.name};dur=${value}`);
      }
    }
  }

  getTimingHeader() {
    return this._metrics.join(', ');
  }

  flush() {
    console.log(this._metrics.join(', '));
  }
}

export class InfluxMetricsProvider implements IMetricsProviderRepository {
  private metrics: string[] = [];
  constructor(
    private influxApiToken: string,
    private environment: string,
  ) {}

  pushMetric(metric: Metric) {
    const point = new Point(metric.name);
    for (const [key, value] of metric.tags) {
      point.tag(key, value);
    }
    for (const [key, { value, type }] of metric.fields) {
      if (type === 'duration') {
        point.intField(key, value);
      } else if (type === 'int') {
        point.intField(key, value);
      }
    }
    const influxLineProtocol = point.toLineProtocol()?.toString();
    if (influxLineProtocol) {
      this.metrics.push(influxLineProtocol);
    }
  }

  async flush() {
    if (this.metrics.length === 0) {
      return;
    }
    const metrics = this.metrics.join('\n');
    if (this.environment === 'production') {
      const response = await fetch('https://cf-workers.monitoring.immich.cloud/write', {
        method: 'POST',
        body: this.metrics.join('\n'),
        headers: {
          Authorization: `Token ${this.influxApiToken}`,
        },
      });
      await response.body?.cancel();
    } else {
      console.log(metrics);
    }
  }
}

export class Metric {
  private _tags: Map<string, string> = new Map();
  private _timestamp = performance.now();
  private _fields = new Map<string, { value: any; type: 'duration' | 'int' }>();
  private constructor(private _name: string) {}

  static create(name: string) {
    return new Metric(name);
  }

  get tags() {
    return this._tags;
  }

  get timestamp() {
    return this._timestamp;
  }

  get fields() {
    return this._fields;
  }

  get name() {
    return this._name;
  }

  addTag(key: string, value: string) {
    this._tags.set(key, value);
    return this;
  }

  addTags(tags: { [key: string]: string }) {
    for (const [key, value] of Object.entries(tags)) {
      this._tags.set(key, value);
    }
    return this;
  }

  durationField(key: string, duration?: number) {
    this._fields.set(key, { value: duration ?? performance.now() - this._timestamp, type: 'duration' });
    return this;
  }

  intField(key: string, value: number) {
    this._fields.set(key, { value, type: 'int' });
    return this;
  }
}

export class CloudflareMetricsRepository implements IMetricsRepository {
  private readonly defaultTags: { [key: string]: string };

  constructor(
    private operationPrefix: string,
    request: Request<unknown, IncomingRequestCfProperties>,
    private deferredRepository: IDeferredRepository,
    private env: WorkerEnv,
    private metricsProviders: IMetricsProviderRepository[],
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
    operation = { ...operation, tags: { ...operation.tags, ...this.defaultTags } };
    const callback = (metric: Metric) => {
      for (const provider of this.metricsProviders) {
        provider.pushMetric(metric);
      }
    };

    return monitorAsyncFunction(this.operationPrefix, operation, call, callback, options);
  }
}
