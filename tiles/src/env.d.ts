interface WorkerEnv extends Omit<Env, 'ENVIRONMENT' | 'PMTILES_FILE_NAME' | 'PMTILES_FILE_HASH'> {
  ENVIRONMENT: string;
  PMTILES_FILE_NAME: string;
  PMTILES_FILE_HASH: string;
}
