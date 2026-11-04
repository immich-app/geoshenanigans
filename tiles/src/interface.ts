import { Metric } from './repository';

export interface IDatabaseRepository {
  query(query: string, ...values: unknown[]): Promise<D1Result<Record<string, unknown>>>;
}

export interface IMemCacheRepository {
  set<T>(key: string, value: T): void;
  get<T>(key: string): T | undefined;
}

export interface IStorageRepository {
  getRangeAsStream(range: { length: number; offset: number }, key: string): Promise<ReadableStream>;
  // Undefined if there is no such object.
  getAsStream(key: string): Promise<ReadableStream | undefined>;
  getDeploymentKey(): string;
}

export interface IDeferredRepository {
  defer(promise: AsyncFn): void;
  runImmediately(promise: Promise<unknown>): void;
  // Holds the deferred calls back until the promise settles, e.g. for metrics pushed after the response is out.
  waitFor(promise: Promise<unknown>): void;
  runDeferred(): void;
}

export type AsyncFn = (...args: any[]) => Promise<any>;
export type Class = { new (...args: any[]): any };
export type Operation = { name: string; tags?: { [key: string]: string } };
export type Options = {
  monitorInvocations?: boolean;
  acceptedErrors?: Class[];
  // Tags that depend on what the call returned, e.g. the cache status of a response.
  resultTags?: (result: any) => { [key: string]: string };
};

export interface IMetricsRepository {
  monitorAsyncFunction<T extends AsyncFn>(
    operation: Operation,
    call: T,
    options?: Options,
  ): (...args: Parameters<T>) => Promise<Awaited<ReturnType<T>>>;
  push(metric: Metric): void;
  // The same providers and tags, plus more tags, under another operation prefix.
  scoped(operationPrefix: string, tags: { [key: string]: string }): IMetricsRepository;
}

export interface IMetricsProviderRepository {
  pushMetric(metric: Metric): void;
  flush(): void;
}
