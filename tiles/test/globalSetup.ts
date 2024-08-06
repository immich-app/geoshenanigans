import express from 'express';
import { AddressInfo } from 'node:net';
import * as Path from 'node:path';
import type { GlobalSetupContext } from 'vitest/node';

export default function setup({ provide }: GlobalSetupContext) {
  const app = express();
  console.log(Path.join(__dirname, '/assets'));
  app.use(express.static(Path.join(__dirname, '/assets')));
  const listener = app.listen(0, () => {
    const addressInfo = listener.address() as AddressInfo;
    const port = addressInfo.port;
    provide('port', port);
  });
}

// You can also extend `ProvidedContext` type
// to have type safe access to `provide/inject` methods:
declare module 'vitest' {
  export interface ProvidedContext {
    port: number;
  }
}
