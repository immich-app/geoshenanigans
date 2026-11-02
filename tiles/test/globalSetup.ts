import express from 'express';
import { AddressInfo } from 'node:net';
import * as path from 'node:path';
import type { GlobalSetupContext } from 'vitest/node';

export default function setup({ provide }: GlobalSetupContext) {
  const app = express();
  console.log(path.join(__dirname, '/assets'));
  app.use(express.static(path.join(__dirname, '/assets')));

  // A D1 proxy whose first response stalls halfway through its body and whose second one answers.
  const stalled = new Set<string>();
  app.post('/stall-once/:id/:env', (req, res) => {
    if (!stalled.has(req.params.id)) {
      stalled.add(req.params.id);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.write('{"results":');
      return;
    }
    res.json({ results: [[{ ok: 1 }]] });
  });

  const listener = app.listen(0, () => {
    const addressInfo = listener.address() as AddressInfo;
    const port = addressInfo.port;
    provide('port', port);
  });

  return () => {
    listener.closeAllConnections();
    listener.close();
  };
}

// You can also extend `ProvidedContext` type
// to have type safe access to `provide/inject` methods:
declare module 'vitest' {
  export interface ProvidedContext {
    port: number;
  }
}
