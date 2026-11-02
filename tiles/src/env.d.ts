interface WorkerEnv extends Omit<Env, 'ENVIRONMENT' | 'DEPLOYMENT_KEY'> {
  ENVIRONMENT: string;
  DEPLOYMENT_KEY: string;
  // Prepended to object keys in the tiles-geo bucket; unset in prod, "dev/" in dev.
  STORAGE_PREFIX?: string;
  VMETRICS_API_TOKEN: string;
  WORKER_TYPE: string | undefined;
  TIGRIS_KEY_ID: string;
  TIGRIS_ACCESS_KEY: string;
  // D1 proxy bindings, see d1-proxy.ts.
  D1_PROXY_TOKEN_PROD?: string;
  D1_PROXY_TOKEN_DEV?: string;
  D1_PROD?: D1Database;
  D1_DEV?: D1Database;
}
