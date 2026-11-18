interface WorkerEnv extends Omit<Env, 'ENVIRONMENT' | 'DEPLOYMENT_KEY'> {
  ENVIRONMENT: string;
  DEPLOYMENT_KEY: string;
  // Prepended to object keys in the tiles-geo bucket; unset in prod, "dev/" in dev.
  STORAGE_PREFIX?: string;
  // The o11y gateway's origin (https://vmauth.futostatus.com in prod, the staging store's in dev) and its bearer token.
  // Without both the worker ships no metrics or logs (see o11y.ts).
  O11Y_URL?: string;
  O11Y_TOKEN?: string;
  // The fraction of requests logged to o11y, 0.01 when unset. Also applies to the Bunny request logs' sample.
  REQUEST_LOG_SAMPLE_RATE?: string;
  // Bunny's request logs (src/bunny-logs): the account API key and the pull zone's numeric id. Without both the cron's
  // log reader is skipped.
  BUNNY_API_KEY?: string;
  BUNNY_PULL_ZONE_ID?: string;
  // The synthetic probe: the Bunny-served origin it requests tiles from (e.g. https://tiles.immich.cloud), the
  // Globalping token it measures with and how often it runs (minutes, 10 when unset). Without the URL and token it is
  // skipped.
  BUNNY_PUBLIC_URL?: string;
  GLOBALPING_TOKEN?: string;
  BUNNY_PROBE_INTERVAL_MINUTES?: string;
  WORKER_TYPE: string | undefined;
  TIGRIS_KEY_ID: string;
  TIGRIS_ACCESS_KEY: string;
  // D1 proxy bindings, see d1-proxy.ts.
  D1_PROXY_TOKEN_PROD?: string;
  D1_PROXY_TOKEN_DEV?: string;
  D1_PROD?: D1Database;
  D1_DEV?: D1Database;
}
