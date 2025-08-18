interface WorkerEnv extends Omit<Env, 'ENVIRONMENT' | 'DEPLOYMENT_KEY'> {
  ENVIRONMENT: string;
  DEPLOYMENT_KEY: string;
  VMETRICS_API_TOKEN: string;
  WORKER_TYPE: string | undefined;
  TIGRIS_KEY_ID: string;
  TIGRIS_ACCESS_KEY: string;
}
