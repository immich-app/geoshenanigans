import { Point } from '@influxdata/influxdb-client';
import { AwsClient } from 'aws4fetch';
import { parse as parseDomain } from 'tldts';
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

// Every D1 read goes to the nearest replica, without waiting for it to catch up with the primary.
const D1_SESSION = 'first-unconstrained';

export class CloudflareD1Repository implements IDatabaseRepository {
  constructor(
    private database: D1Database,
    private metrics: IMetricsRepository,
  ) {}

  async query(query: string, ...values: unknown[]) {
    const metric = Metric.create('d1_query');
    const resp = await this.database
      .withSession(D1_SESSION)
      .prepare(query)
      .bind(...values)
      .run();

    metric
      .durationField('duration')
      .addTag('query', query)
      .addTag('served_by_region', String(resp.meta.served_by_region))
      .addTag('served_by_primary', String(resp.meta.served_by_primary))
      .intField('rows_read', resp.meta.rows_read)
      .intField('sql_duration', resp.meta.duration)
      .intField('count', 1);
    this.metrics.push(metric);

    return resp;
  }
}

/**
 * Keeps the D1 read replica nearest to this colo awake. Replicas spin down soon after their last read, and while one
 * spins back up reads go to the primary, which can be on another continent. A trivial read in the lookups' session
 * mode, made on every request, keeps the replica they use warm. Never rejects: a failed warm-up is only logged.
 */
export async function warmD1Replica(database: D1Database): Promise<void> {
  try {
    await database.withSession(D1_SESSION).prepare('SELECT 1').run();
  } catch (error) {
    console.error('D1 warm-up failed', error);
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

export enum TigrisEndpoint {
  T3 = 'https://t3.storage.dev',
  FLY = 'https://fly.storage.tigris.dev',
}

// Continents where t3.storage.dev reaches Tigris faster; elsewhere fly.storage.tigris.dev does.
const T3_CONTINENTS = new Set(['EU', 'NA', 'AS']);

/** The Tigris endpoint for a request from the given continent (unknown ones get the Fly endpoint). */
export function tigrisEndpoint(continent: string | null | undefined): TigrisEndpoint {
  return continent && T3_CONTINENTS.has(continent) ? TigrisEndpoint.T3 : TigrisEndpoint.FLY;
}

// One signer per endpoint and key for the isolate's lifetime: it keeps the day's signing key, so a read after the
// first is signed with one HMAC.
const tigrisClients = new Map<string, AwsClient>();

export function tigrisClient(endpoint: TigrisEndpoint, keyId: string, accessKey: string): AwsClient {
  const cacheKey = `${endpoint}|${keyId}`;
  let client = tigrisClients.get(cacheKey);
  if (!client) {
    // Retries are TigrisStorageRepository's, which lets go of each failed response.
    client = new AwsClient({
      accessKeyId: keyId,
      secretAccessKey: accessKey,
      service: 's3',
      region: 'auto',
      retries: 0,
    });
    tigrisClients.set(cacheKey, client);
  }
  return client;
}

// As the AWS SDK did: up to 3 attempts while Tigris answers 5xx or 429, after a jittered, doubling backoff.
const TIGRIS_ATTEMPTS = 3;
const TIGRIS_RETRY_BASE_MS = 100;

const retryable = (status: number) => status >= 500 || status === 429;

/** Reads deployment objects from the tiles bucket with SigV4-signed GETs. */
export class TigrisStorageRepository implements IStorageRepository {
  private bucketUrl: URL;
  constructor(
    private keyId: string,
    private accessKey: string,
    bucketKey: string,
    private deploymentKey: string,
    private metrics: IMetricsRepository,
    private storagePrefix = '',
    private endpoint = TigrisEndpoint.FLY,
  ) {
    // Virtual-hosted style, the bucket in the host name.
    this.bucketUrl = new URL(endpoint);
    this.bucketUrl.hostname = `${bucketKey}.${this.bucketUrl.hostname}`;
  }

  /** GETs an object, or a range of it; the response is Tigris' last, whatever its status. */
  private async get(key: string, range?: { offset: number; length: number }): Promise<Response> {
    const metric = Metric.create('tigris_storage_get').addTag('endpoint', new URL(this.endpoint).hostname);
    const path = `${this.storagePrefix}${this.deploymentKey}/${key}`.split('/').map(encodeURIComponent).join('/');
    const url = new URL(path, this.bucketUrl).toString();
    const headers = range ? { Range: `bytes=${range.offset}-${range.offset + range.length - 1}` } : undefined;
    // Taken when a read is made, so that a missing key fails the read rather than the request.
    const client = tigrisClient(this.endpoint, this.keyId, this.accessKey);
    for (let attempt = 1; ; attempt++) {
      const response = await fetch(await client.sign(url, { method: 'GET', headers }));
      if (!retryable(response.status) || attempt === TIGRIS_ATTEMPTS) {
        if (response.ok) {
          metric.durationField('duration').intField('count', 1);
          this.metrics.push(metric);
        }
        return response;
      }
      await response.body?.cancel();
      await new Promise((resolve) => setTimeout(resolve, Math.random() * TIGRIS_RETRY_BASE_MS * 2 ** (attempt - 1)));
    }
  }

  // Reads are only ever handed on as streams, so a tile's bytes go from Tigris to the client as they arrive.
  async getRangeAsStream(range: { length: number; offset: number }, key: string): Promise<ReadableStream> {
    const response = await this.get(key, range);
    // A 200 is the whole object, which is the range only if the range is all of it.
    const whole =
      response.status === 200 && range.offset === 0 && response.headers.get('Content-Length') === String(range.length);
    if ((response.status !== 206 && !whole) || !response.body) {
      await response.body?.cancel();
      throw new Error(`Tigris answered a range read with ${response.status}`);
    }
    return response.body;
  }

  async getAsStream(key: string): Promise<ReadableStream | undefined> {
    const response = await this.get(key);
    if (response.status === 404) {
      await response.body?.cancel();
      return;
    }
    if (response.status !== 200 || !response.body) {
      await response.body?.cancel();
      throw new Error(`Tigris answered a read with ${response.status}`);
    }
    return response.body;
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

/** Tags for every metric of a client request: where it reached Cloudflare and which site it came from. */
export function requestTags(request: Request<unknown, IncomingRequestCfProperties>): { [key: string]: string } {
  // Origin is "null" from sandboxed or file:// pages, which isn't a URL.
  const url = URL.parse(request.headers.get('origin') ?? '') ?? new URL('http://missing.url');
  const apexDomain = parseDomain(url.hostname).domain ?? url.hostname;
  const protocol = url.protocol;

  return {
    continent: request.cf?.continent ?? '',
    colo: request.cf?.colo ?? '',
    asOrg: request.cf?.asOrganization ?? '',
    apexDomain,
    protocol,
  };
}

export class CloudflareMetricsRepository implements IMetricsRepository {
  constructor(
    private operationPrefix: string,
    private readonly defaultTags: { [key: string]: string },
    private metricsProviders: IMetricsProviderRepository[],
  ) {}

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
