/**
 * The live build in the state zone, for the sync watchdog:
 *
 *   npm run bunny:status -- [--json] [--state-prefix <p>]
 *
 * Needs BUNNY_STATE_ZONE, BUNNY_STATE_PASSWORD and optionally BUNNY_STATE_HOST (else BUNNY_STORAGE_HOST). --json prints
 * {"liveBuild","updatedAt","osmDate","buildAgeHours","osmAgeHours","pending"}; buildAgeHours counts from 00:00 UTC of the
 * build date, and pending lists builds a sync started and didn't make live (the zone may hold their bytes; a sync of
 * the live build puts its bytes back). Exits 1 when there is no state.json yet (the JSON then has nulls), and within about a minute when the state
 * zone doesn't answer.
 */
import { parseArgs } from 'node:util';
import { BunnyStateStore, liveStatus, readState } from '../state';

async function main() {
  const { values } = parseArgs({
    options: { json: { type: 'boolean', default: false }, 'state-prefix': { type: 'string' } },
    strict: true,
  });
  const store = BunnyStateStore.fromEnv();
  const state = await readState(store, values['state-prefix']).finally(() => store.close());
  const status = liveStatus(state);
  if (values.json) {
    console.log(JSON.stringify(status));
  } else if (state) {
    console.log(`live build ${status.liveBuild}, ${status.buildAgeHours} h old (prefix ${state.prefix})`);
    console.log(`updated ${status.updatedAt}, ${state.positions} positions, manifest ${state.manifest}`);
    console.log(status.osmDate ? `OSM data from ${status.osmDate}, ${status.osmAgeHours} h old` : 'OSM date unknown');
    for (const pending of state.pending) {
      console.log(`pending ${pending.build} since ${pending.startedAt}: a sync started it and didn't finish`);
    }
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
