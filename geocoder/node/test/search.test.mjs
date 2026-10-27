// Place search through the binding: an index shipped beside its dataset is used as is, one
// shipped for other files is rebuilt, a missing one is built on the device, and files replaced
// under a loaded geocoder are refused.
//
//   cargo build --release && node --test test/
//
// GEOCODER_NODE overrides the addon path (default: this crate's release build).
import assert from 'node:assert/strict';
import { cpSync, mkdirSync, mkdtempSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { after, test } from 'node:test';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const addon = { exports: {} };
process.dlopen(addon, process.env.GEOCODER_NODE ?? join(here, '../target/release/libgeocoder_node.so'));
const { Geocoder } = addon.exports;

const root = mkdtempSync(join(tmpdir(), 'geocoder-node-search-'));
after(() => rmSync(root, { recursive: true, force: true }));

const u32 = (v) => { const b = Buffer.alloc(4); b.writeUInt32LE(v); return b; };
const f32 = (v) => { const b = Buffer.alloc(4); b.writeFloatLE(v); return b; };

// A dataset as the builder writes one: a country boundary and two places named `names`.
function writeDataset(dir, names) {
  const strings = [];
  let size = 0;
  const string = (s) => {
    const at = size;
    const bytes = Buffer.from(`${s}\0`);
    strings.push(bytes);
    size += bytes.length;
    return at;
  };
  const country = string('Testland');
  const places = names.map(string);
  const ring = [[10, 20], [10, 21], [11, 21], [11, 20], [10, 20]];
  const vertices = Buffer.concat([Buffer.from([4, 0]), f32(10), f32(20), ...ring.flatMap(([lat, lng]) => [f32(lat), f32(lng)])]);
  const polygons = Buffer.concat([u32(0), u32(ring.length), u32(country), Buffer.from([2, 0, 0, 0]), f32(1), Buffer.from('tl'), Buffer.from([0, 0])]);
  const nodes = Buffer.concat(places.map((name, i) => Buffer.concat([f32(10.2 + i * 0.5), f32(20.5), u32(name), Buffer.from([1, 0, 0, 0]), u32(0)])));
  const tiers = Array.from({ length: 5 }, (_, t) => ({ start: t === 0 ? 0 : size, end: size }));
  const files = {
    'admin_polygons.bin': polygons,
    'admin_vertices.bin': vertices,
    'admin_cells.bin': Buffer.alloc(0),
    'admin_entries.bin': Buffer.alloc(0),
    'place_nodes.bin': nodes,
    'place_cells.bin': Buffer.alloc(0),
    'place_entries.bin': Buffer.alloc(0),
    'strings_core.bin': Buffer.concat(strings),
    'strings_layout.json': JSON.stringify({ tiers }),
  };
  for (const [name, bytes] of Object.entries(files)) {
    // As a patch replaces a file: a new one renamed over it.
    writeFileSync(join(dir, `${name}.new`), bytes);
    renameSync(join(dir, `${name}.new`), join(dir, name));
  }
  return dir;
}

let datasets = 0;
const dataset = (names) => {
  const dir = join(root, `ds${datasets++}`);
  mkdirSync(dir);
  return writeDataset(dir, names);
};

const names = async (geocoder, query) => JSON.parse(await geocoder.searchPlaces(query)).map((r) => r.name);

test('a missing index is built on the device', async () => {
  const geocoder = Geocoder.load(dataset(['Testville', 'Otherton']));

  assert.equal(await geocoder.openPlaceSearch(), 'built');

  assert.deepEqual(await names(geocoder, 'testville'), ['Testville']);
});

test('an index shipped for these files is used as is', async () => {
  const ci = dataset(['Testville', 'Otherton']);
  await Geocoder.load(ci).openPlaceSearch();
  const dir = dataset(['Testville', 'Otherton']);
  cpSync(join(ci, 'search'), join(dir, 'search'), { recursive: true });
  const geocoder = Geocoder.load(dir);

  assert.equal(await geocoder.openPlaceSearch(), 'current');

  assert.deepEqual(await names(geocoder, 'otherton'), ['Otherton']);
});

test('an index shipped for other files is rebuilt', async () => {
  const ci = dataset(['Testville', 'Otherton']);
  await Geocoder.load(ci).openPlaceSearch();
  const dir = dataset(['Nestville', 'Otherton']);
  cpSync(join(ci, 'search'), join(dir, 'search'), { recursive: true });
  const geocoder = Geocoder.load(dir);

  assert.equal(await geocoder.openPlaceSearch(), 'built');

  assert.deepEqual(await names(geocoder, 'nestville'), ['Nestville']);
  assert.deepEqual(await names(geocoder, 'testville'), []);
});

test('an index in another dir is built there', async () => {
  const dir = dataset(['Testville', 'Otherton']);
  const indexDir = join(root, 'elsewhere', 'search');
  const geocoder = Geocoder.load(dir);

  assert.equal(await geocoder.openPlaceSearch({ indexDir }), 'built');
  assert.equal(await Geocoder.load(dir).openPlaceSearch({ indexDir }), 'current');

  assert.deepEqual(await names(geocoder, 'otherton'), ['Otherton']);
});

test('files replaced under a loaded geocoder are refused', async () => {
  const dir = dataset(['Testville', 'Otherton']);
  const geocoder = Geocoder.load(dir);
  writeDataset(dir, ['Nestville', 'Otherton']);

  await assert.rejects(geocoder.openPlaceSearch(), /changed since it was loaded/);

  const patched = Geocoder.load(dir);
  assert.equal(await patched.openPlaceSearch(), 'built');
  assert.deepEqual(await names(patched, 'nestville'), ['Nestville']);
});

test('search before opening is refused', () => {
  const geocoder = Geocoder.load(dataset(['Testville', 'Otherton']));

  assert.throws(() => geocoder.searchPlaces('testville'), /Place search is not open/);
});
