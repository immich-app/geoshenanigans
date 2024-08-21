interface WorkerEnv extends Omit<Env, 'ENVIRONMENT' | 'PMTILES_FILE_NAME'> {
  ENVIRONMENT: string;
  PMTILES_FILE_NAME: string;
  VMETRICS_API_TOKEN: string;
}
