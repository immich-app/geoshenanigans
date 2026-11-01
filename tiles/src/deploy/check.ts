/**
 * Confirms a published deployment is still in place right before the worker switches to it: its D1 table, which
 * publishing another build drops, and every object its inventory lists, which cleanup removes.
 *
 * Needs DEPLOYMENT_KEY, D1_PROXY_URL, D1_PROXY_TOKEN, D1_ENV, TIGRIS_ENDPOINT, TIGRIS_BUCKET, TIGRIS_KEY_ID,
 * TIGRIS_ACCESS_KEY and optionally STORAGE_PREFIX.
 */
import { cacheTableName, parseDeploymentKey } from './config';
import { D1ProxyClient } from './d1-proxy-client';
import { Inventory, INVENTORY_NAME, inventoryProblems } from './inventory';
import { DeploymentStorage } from './storage';

async function main() {
  const deploymentKey = parseDeploymentKey('DEPLOYMENT_KEY', process.env.DEPLOYMENT_KEY);
  const d1 = D1ProxyClient.fromEnv();
  const storage = DeploymentStorage.fromEnv();
  const table = cacheTableName(deploymentKey);

  if (!(await d1.tableNames()).includes(table)) {
    throw new Error(`${table} no longer exists; publish ${deploymentKey} again`);
  }
  const [tileJson] = await d1.run(`SELECT startTileId FROM ${table} WHERE startTileId = -1`);
  const [firstRow] = await d1.run(`SELECT startTileId FROM ${table} WHERE startTileId >= 0 LIMIT 1`);
  if (!tileJson || !firstRow) {
    throw new Error(`${table} is incomplete; publish ${deploymentKey} again`);
  }
  const inventoryBytes = await storage.get(deploymentKey, INVENTORY_NAME).catch((error) => {
    throw new Error(
      `${storage.key(deploymentKey, INVENTORY_NAME)} is missing (${error}); publish ${deploymentKey} again`,
    );
  });
  const inventory = JSON.parse(new TextDecoder().decode(inventoryBytes)) as Inventory;
  const problems = inventoryProblems(inventory, await storage.list(deploymentKey));
  if (problems.length > 0) {
    throw new Error(
      `${problems.length} of ${Object.keys(inventory.objects).length} objects of ${deploymentKey} have changed ` +
        `(${problems.slice(0, 5).join('; ')}); publish it again`,
    );
  }
  console.log(`${deploymentKey} is still published`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
