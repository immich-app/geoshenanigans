// Deployment keys are protomaps build dates (YYYYMMDD). They become D1 table names, so nothing else gets through.
const DEPLOYMENT_KEY_PATTERN = /^\d{8}$/;

export function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing environment variable ${name}`);
  }
  return value;
}

export function parseDeploymentKey(name: string, value: string | undefined): string {
  if (!value || !DEPLOYMENT_KEY_PATTERN.test(value)) {
    throw new Error(`${name} must be a YYYYMMDD build key, got ${JSON.stringify(value)}`);
  }
  return value;
}

export const cacheTableName = (deploymentKey: string) => `cache_entries_${parseDeploymentKey('key', deploymentKey)}`;

export const CACHE_TABLE_PATTERN = /^cache_entries_\d{8}$/;

export function intEnv(name: string, fallback: number): number {
  const value = process.env[name];
  if (value === undefined || value === '') {
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer, got ${value}`);
  }
  return parsed;
}
