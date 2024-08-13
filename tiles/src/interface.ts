export interface IKeyValueRepository {
  put(key: string, value: string): Promise<void>;
  get(key: string): Promise<string | undefined>;
  getAsStream(key: string): Promise<ReadableStream | undefined>;
}

export interface IMemCacheRepository {
  set<T>(key: string, value: T): void;
  get<T>(key: string): T | undefined;
}

export interface IStorageRepository {
  get(range: { length: number; offset: number }): Promise<ArrayBuffer>;
  getAsStream(range: { length: number; offset: number }): Promise<ReadableStream>;
  getFileName(): string;
}

export interface IDeferredRepository {
  defer(promise: Promise<any>): void;
}
