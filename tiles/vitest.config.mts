import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config';

export default defineWorkersConfig({
  test: {
    globals: true,
    poolOptions: {
      workers: {
        wrangler: { configPath: './wrangler.toml' },
      },
    },
    globalSetup: './test/globalSetup.ts',
    coverage: {
      provider: 'istanbul',
    },
  },
});
