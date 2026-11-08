interface WorkerEnv extends Omit<Env, 'ENVIRONMENT' | 'DEPLOYMENT_KEY'> {
  ENVIRONMENT: string;
  DEPLOYMENT_KEY: string;
  // Prepended to object keys in the tiles-geo bucket; unset in prod, "dev/" in dev.
  STORAGE_PREFIX?: string;
  // The o11y gateway's origin (https://vmauth.futostatus.com in prod, the staging store's in dev) and its bearer token.
  // Without both the worker ships no metrics or logs (see o11y.ts).
  O11Y_URL?: string;
  O11Y_TOKEN?: string;
  // The fraction of requests logged to o11y, 0.01 when unset.
  REQUEST_LOG_SAMPLE_RATE?: string;
  WORKER_TYPE: string | undefined;
  TIGRIS_KEY_ID: string;
  TIGRIS_ACCESS_KEY: string;
  // D1 proxy bindings, see d1-proxy.ts.
  D1_PROXY_TOKEN_PROD?: string;
  D1_PROXY_TOKEN_DEV?: string;
  D1_PROD?: D1Database;
  D1_DEV?: D1Database;
}
