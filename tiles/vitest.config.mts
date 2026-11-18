import { cloudflareTest } from '@cloudflare/vitest-plugin';
import { configDefaults, defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    // Node-side tooling tests run under vitest.node.config.mts.
    exclude: [...configDefaults.exclude, 'test/node/**'],
    globalSetup: './test/globalSetup.ts',
    coverage: {
      provider: 'istanbul',
    },
  },
  plugins: [
    cloudflareTest({
      wrangler: { configPath: './wrangler.toml' },
    }),
  ],
});
