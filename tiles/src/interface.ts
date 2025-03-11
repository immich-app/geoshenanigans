import { Metric } from './repository';

export interface IDatabaseRepository {
  query(query: string, ...values: unknown[]): Promise<D1Result<Record<string, unknown>>>;
}

export interface IMemCacheRepository {
  set<T>(key: string, value: T): void;
  get<T>(key: string): T | undefined;
}

export interface IStorageRepository {
  getRange(range: { length: number; offset: number }): Promise<ArrayBuffer>;
  getRangeAsStream(range: { length: number; offset: number }): Promise<ReadableStream>;
  get(key: string): Promise<ArrayBuffer>;
  getAsStream(key: string): Promise<ReadableStream>;
  getDeploymentKey(): string;
}

export interface IDeferredRepository {
  defer(promise: AsyncFn): void;
  runDeferred(): void;
}

export type AsyncFn = (...args: any[]) => Promise<any>;
export type Class = { new (...args: any[]): any };
export type Operation = { name: string; tags?: { [key: string]: string } };
export type Options = { monitorInvocations?: boolean; acceptedErrors?: Class[] };

export interface IMetricsRepository {
  monitorAsyncFunction<T extends AsyncFn>(
    operation: Operation,
    call: T,
    options?: Options,
  ): (...args: Parameters<T>) => Promise<Awaited<ReturnType<T>>>;
  push(metric: Metric): void;
}

export interface IMetricsProviderRepository {
  pushMetric(metric: Metric): void;
  flush(): void;
}
