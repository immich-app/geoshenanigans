import { defineConfig } from 'vitest/config';

// Node-side tooling (src/bunny, run on CI runners and servers, not in the Worker) is tested in plain Node.
export default defineConfig({
  test: {
    environment: 'node',
    include: ['test/node/**/*.spec.ts'],
    testTimeout: 30_000,
  },
});
