/**
 * The live build in the state zone, for the sync watchdog:
 *
 *   npm run bunny:status -- [--json] [--state-prefix <p>]
 *
 * Needs BUNNY_STATE_ZONE, BUNNY_STATE_PASSWORD and optionally BUNNY_STORAGE_HOST. --json prints
 * {"liveBuild","updatedAt","osmDate","buildAgeHours","osmAgeHours"}; buildAgeHours counts from 00:00 UTC of the build
 * date. Exits 1 when there is no state.json yet (the JSON then has nulls).
 */
import { parseArgs } from 'node:util';
import { BunnyStateStore, liveStatus, readState } from '../state';

async function main() {
  const { values } = parseArgs({
    options: { json: { type: 'boolean', default: false }, 'state-prefix': { type: 'string' } },
    strict: true,
  });
  const state = await readState(BunnyStateStore.fromEnv(), values['state-prefix']);
  const status = liveStatus(state);
  if (values.json) {
    console.log(JSON.stringify(status));
  } else if (state) {
    console.log(`live build ${status.liveBuild}, ${status.buildAgeHours} h old (prefix ${state.prefix})`);
    console.log(`updated ${status.updatedAt}, ${state.positions} positions, manifest ${state.manifest}`);
    console.log(status.osmDate ? `OSM data from ${status.osmDate}, ${status.osmAgeHours} h old` : 'OSM date unknown');
  }
  if (!state) {
    console.error('No state.json in the state zone: nothing has been synced yet');
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
