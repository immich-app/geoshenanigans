import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { Point } from '@influxdata/influxdb-client';
import {
  AsyncFn,
  IDatabaseRepository,
  IDeferredRepository,
  IMemCacheRepository,
  IMetricsProviderRepository,
  IMetricsRepository,
  IStorageRepository,
  Operation,
  Options,
} from './interface';
import { monitorAsyncFunction } from './monitor';

export class CloudflareD1Repository implements IDatabaseRepository {
  constructor(
    private database: D1Database,
    private metrics: IMetricsRepository,
  ) {
    void database.withSession('first-unconstrained').prepare('SELECT 1').run();
  }

  async query(query: string, ...values: unknown[]) {
    const metric = Metric.create('d1_query');
    const resp = await this.database
      .withSession('first-unconstrained')
      .prepare(query)
      .bind(...values)
      .run();

    metric
      .durationField('duration')
      .addTag('query', query)
      .addTag('served_by_region', resp.meta.served_by_region as string)
      .addTag('served_by_primary', (resp.meta.served_by_primary as boolean).toString())
      .intField('rows_read', resp.meta.rows_read)
      .intField('sql_duration', resp.meta.duration)
      .intField('count', 1);
    this.metrics.push(metric);

    return resp;
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

export class TigrisStorageRepository implements IStorageRepository {
  private client: S3Client;
  constructor(
    keyId: string,
    accessKey: string,
    private bucketKey: string,
    private deploymentKey: string,
    private metrics: IMetricsRepository,
  ) {
    this.client = new S3Client({
      region: 'auto',
      credentials: {
        accessKeyId: keyId,
        secretAccessKey: accessKey,
      },
      endpoint: 'https://fly.storage.tigris.dev',
      forcePathStyle: false,
    });
  }

  private async getS3Object(args: { key?: string; range?: { offset: number; length: number } }) {
    const { range } = args;
    const metric = Metric.create('tigris_storage_get');
    const command = new GetObjectCommand({
      Bucket: this.bucketKey,
      Key: this.deploymentKey + '/' + args.key,
      Range: range ? `bytes=${range.offset}-${range.offset + range.length - 1}` : undefined,
    });
    const response = await this.client.send(command);
    metric.durationField(`duration`);
    metric.intField('count', 1);
    this.metrics.push(metric);
    const data = response.Body;
    if (!data) {
      throw new Error('Data not found for range ' + JSON.stringify(range));
    }
    return data;
  }

  async getRange(range: { length: number; offset: number }): Promise<ArrayBuffer> {
    const data = await this.getS3Object({ range });
    return (await data.transformToByteArray()).buffer;
  }

  async getRangeAsStream(range: { length: number; offset: number }, key?: string): Promise<ReadableStream> {
    const data = await this.getS3Object({ range, key });
    return data.transformToWebStream();
  }

  async get(key: string): Promise<ArrayBuffer> {
    const data = await this.getS3Object({ key });
    return (await data.transformToByteArray()).buffer;
  }

  async getAsStream(key: string): Promise<ReadableStream> {
    const data = await this.getS3Object({ key });
    return data.transformToWebStream();
  }

  getDeploymentKey(): string {
    return this.deploymentKey;
  }
}

export class CloudflareDeferredRepository implements IDeferredRepository {
  deferred: AsyncFn[] = [];
  constructor(private ctx: ExecutionContext) {}

  defer(call: AsyncFn): void {
    this.deferred.push(call);
  }

  runImmediately(promise: Promise<unknown>): void {
    this.ctx.waitUntil(promise);
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
    for (const [label, { value, type }] of metric.fields) {
      if (type === 'duration') {
        const suffix = label === 'duration' ? '' : `_${label.replace('_duration', '')}`;
        this._metrics.push(`${metric.name}${suffix};dur=${value}`);
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
    if (this.environment === 'prod') {
      const response = await fetch('https://cf-workers.monitoring.immich.cloud/write', {
        method: 'POST',
        body: this.metrics.join('\n'),
        headers: {
          Authorization: `Token ${this.influxApiToken}`,
        },
      });
      if (!response.ok) {
        console.error('Failed to push metrics', response.status, response.statusText);
      }
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
    private metricsProviders: IMetricsProviderRepository[],
  ) {
    const url = new URL(request.headers.get('origin') ?? 'http://missing.url');
    const apexDomain = url.hostname.split('.').splice(-2).join('.');
    const protocol = url.protocol;

    this.defaultTags = {
      continent: request.cf?.continent ?? '',
      colo: request.cf?.colo ?? '',
      asOrg: request.cf?.asOrganization ?? '',
      apexDomain,
      protocol,
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

  push(metric: Metric) {
    metric.addTags(this.defaultTags);
    for (const provider of this.metricsProviders) {
      provider.pushMetric(metric);
    }
  }
}
