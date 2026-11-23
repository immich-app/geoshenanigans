/**
 * Runs the replication gate over `argv[2]` synthetic tile files against a storage stand-in that answers at once, and
 * prints the result: replication.spec.ts runs it under a small --max-old-space-size to bound the gate's memory.
 */
import { REPLICATION_REGIONS, TileGateList, waitForReplication } from '../../src/bunny/replication';
import { BunnyStorage, sha256Hex } from '../../src/bunny/storage';

const files = Number(process.argv[2]);
const sha256 = sha256Hex(new TextEncoder().encode('tile'));
const items = new TileGateList('v1');
for (let i = 0; i < files; i++) {
  items.push(i * 3, sha256);
}
let describes = 0;
const storage = {
  describe: async () => {
    describes++;
    // Half the files lack the last region on the first pass.
    const replicatedZones =
      describes <= files && describes % 2 === 0 ? REPLICATION_REGIONS.slice(0, -1) : REPLICATION_REGIONS;
    return { checksum: sha256, length: 4, replicatedZones, lastChanged: '' };
  },
} as unknown as BunnyStorage;
void waitForReplication(storage, items, REPLICATION_REGIONS, { timeoutMs: 600_000, intervalMs: 1 }).then((result) => {
  const { heapUsed } = process.memoryUsage();
  process.stdout.write(
    `${JSON.stringify({ complete: result.complete, passes: result.passes, describes, heapUsed })}\n`,
  );
});
