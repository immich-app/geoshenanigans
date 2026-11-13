// The binding: reverse geocoding finds a place in its cell, a boundary by its arcs and nothing
// off the globe. Place search uses an index shipped beside its dataset as is,
// rebuilds one shipped for other files, builds a missing one on the device, and refuses files
// replaced under a loaded geocoder.
//
//   cargo build --release && node --test test/*.test.mjs
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

const u16 = (v) => { const b = Buffer.alloc(2); b.writeUInt16LE(v); return b; };
const u32 = (v) => { const b = Buffer.alloc(4); b.writeUInt32LE(v); return b; };
const i32 = (v) => { const b = Buffer.alloc(4); b.writeInt32LE(v); return b; };
const f32 = (v) => { const b = Buffer.alloc(4); b.writeFloatLE(v); return b; };
const varint = (v) => {
  const out = [];
  for (; v >= 0x80; v = Math.floor(v / 0x80)) out.push((v % 0x80) | 0x80);
  out.push(v);
  return Buffer.from(out);
};
const zigzag = (v) => (v >= 0 ? 2 * v : -2 * v - 1);

// The admin cell (S2, level 10) of (10.5, 20.5), inside the country: the server's
// cell_id_at_level(10.5, 20.5, DEFAULT_ADMIN_CELL_LEVEL).
const COUNTRY_CELL = 0x1129af0000000000n;
// The keys (cell id >> 32) of the admin cells of the places at (10.2, 20.5) and (10.7, 20.5).
const PLACE_CELL_KEYS = [0x1129cf00, 0x11290700];

// The admin cell index the builder writes (builder/src/cell_pairs.h) of one level-10 cell listing
// one id: the cell's parent record (its number, its leaf, the offset of its one block), the end of
// the records, the parent bitmap up to the parent's word, the ranks, the directory and the footer;
// the block's codes (one PLAIN row) and the list: one id of group 0 against 0, none of group 1.
function adminIndex(cell, id) {
  const number = cell >> 41n;
  const parent = Number(number >> 10n);
  const child = Number(number & 1023n);
  const leaf = Buffer.alloc(128);
  leaf.writeBigUInt64LE(1n << BigInt(child % 64), 8 * Math.floor(child / 64));
  const words = Math.floor(parent / 64) + 1;
  const bitmap = Buffer.alloc(8 * words);
  bitmap.writeBigUInt64LE(1n << BigInt(parent % 64), 8 * (words - 1));
  const cells = Buffer.concat([Buffer.from('GCP1'), Buffer.from([1, 1, 10, 0]), u32(parent), leaf, u32(0),
    u32(0xFFFFFFFF), bitmap, Buffer.alloc(4 * words), u32(8), u32(1), u32(words)]);
  const entries = Buffer.concat([u16(0), varint(1 << 1), varint(0), varint(zigzag(id))]);
  return [cells, entries];
}

// A string tier as the builder writes it (builder/src/string_files.h): FCC2, the strings
// front-coded in buckets cut by content, the bucket table and the footer after them.
function fcc2(strings) {
  const fnv = (b) => {
    let h = 0xcbf29ce484222325n;
    for (const c of b) h = ((h ^ BigInt(c)) * 0x100000001b3n) & 0xffffffffffffffffn;
    return h;
  };
  const vint = (v) => {
    const o = [];
    for (; v >= 0x80; v >>>= 7) o.push((v & 0x7f) | 0x80);
    o.push(v);
    return Buffer.from(o);
  };
  const body = [];
  const table = [];
  let bucket = [];
  let span = 0;
  let prev = null;
  let buckets = 0;
  let raw = 0;
  const flush = () => {
    const b = Buffer.concat(bucket);
    body.push(b);
    table.push(u16(span), u16(b.length));
    bucket = [];
    span = 0;
    buckets++;
  };
  for (const s of strings) {
    if (span && (span + s.length + 1 > 512 || (span >= 128 && (fnv(s) >> 32n) % 8n === 0n))) flush();
    if (!span) {
      bucket.push(vint(s.length), s);
    } else {
      let l = 0;
      while (l < Math.min(s.length, prev.length) && s[l] === prev[l]) l++;
      bucket.push(vint(l), vint(s.length - l), s.subarray(l));
    }
    prev = s;
    span += s.length + 1;
    raw += s.length + 1;
  }
  if (span) flush();
  return Buffer.concat([...body, ...table, u32(buckets), u32(raw), Buffer.from('FCC2')]);
}

// A dataset as the builder writes one: a country boundary, listed in the admin cell of
// COUNTRY_CELL, and two places named `names`.
function writeDataset(dir, names) {
  const strings = [];
  let size = 0;
  const string = (s) => {
    const at = size;
    const bytes = Buffer.from(s);
    strings.push(bytes);
    size += bytes.length + 1;
    return at;
  };
  const country = string('Testland');
  const places = names.map(string);
  // The country's ring as one arc in e7 units (builder/src/arc_blocks.h): a flags byte (grid
  // exponent 0, first-order deltas, base width b over 2 in bits 3-6, no box), the first vertex,
  // then the zigzag deltas in groups of 16: a byte of the group's widths over b (lat << 4 |
  // lng), then its lat and its lng deltas at those widths, LSB first. A short group's widths
  // sum to 8 or more. Its ring block is one chunk of one ref, to slot 0, that ends the ring.
  const ring = [[10, 20], [10, 21], [11, 21], [11, 20], [10, 20]].map(([lat, lng]) => [lat * 1e7, lng * 1e7]);
  const width = (v) => (v ? v.toString(2).length : 0);
  const zl = ring.slice(1).map(([lat], i) => zigzag(lat - ring[i][0]));
  const zg = ring.slice(1).map(([, lng], i) => zigzag(lng - ring[i][1]));
  const b = 2 * Math.floor((Math.max(0, Math.max(...zl.map(width), ...zg.map(width)) - 15) + 1) / 2);
  const groups = [];
  for (let g = 0; g < zl.length; g += 16) {
    const lat = zl.slice(g, g + 16), lng = zg.slice(g, g + 16);
    let wl = Math.max(b, ...lat.map(width)), wg = Math.max(b, ...lng.map(width));
    while (lat.length < 16 && wl + wg < 8) {
      if (wl - b < 15) wl++;
      else wg++;
    }
    let bits = 0n, n = 0n;
    for (const [v, w] of [...lat.map((v) => [v, wl]), ...lng.map((v) => [v, wg])]) {
      bits |= BigInt(v) << n;
      n += BigInt(w);
    }
    const bytes = Buffer.alloc(Number((n + 7n) / 8n));
    for (let i = 0; i < bytes.length; i++) bytes[i] = Number((bits >> BigInt(8 * i)) & 0xffn);
    groups.push(Buffer.from([((wl - b) << 4) | (wg - b)]), bytes);
  }
  const arc = Buffer.concat([Buffer.from([(b / 2) << 3]), i32(ring[0][0]), i32(ring[0][1]), ...groups]);
  const rings = Buffer.from([(1 << 2) | 1, 0]);
  // Its record: its ring block at 0 (to the end of admin_rings.bin), its name, area, country
  // code and admin level 2 (level << 4 | place type).
  const polygons = Buffer.concat([u32(0), u32(country), f32(1), Buffer.from('tl'), Buffer.from([2 << 4, 0])]);
  // The places, towns, as the builder lays them out (builder/src/place_files.h): visible nodes
  // by their admin cell's key, in one block (of 64 rows at most) of a row per node, LSB first:
  // its lat and lng order keys (f32 bit patterns in the floats' order) less the block's least
  // ones, its place type (3 bits) and its name, at the block's widths; then the block table and
  // the footer. place_cells.bin lists each cell's key and first row; none are hidden.
  const f32bits = (v) => f32(v).readUInt32LE(0);
  const orderKey = (bits) => (bits & 0x80000000 ? ~bits : bits | 0x80000000) >>> 0;
  const rows = places
    .map((name, i) => ({ key: PLACE_CELL_KEYS[i], lat: orderKey(f32bits(10.2 + i * 0.5)), lng: orderKey(f32bits(20.5)), name }))
    .sort((a, c) => a.key - c.key);
  const latBase = Math.min(...rows.map((r) => r.lat)), lngBase = Math.min(...rows.map((r) => r.lng));
  const wl = width(Math.max(...rows.map((r) => r.lat - latBase))), wg = width(Math.max(...rows.map((r) => r.lng - lngBase)));
  const wn = width(Math.max(...rows.map((r) => r.name)));
  let rowBits = 0n, at = 0n;
  for (const r of rows) {
    for (const [v, w] of [[r.lat - latBase, wl], [r.lng - lngBase, wg], [1, 3], [r.name, wn]]) {
      rowBits |= BigInt(v) << at;
      at += BigInt(w);
    }
  }
  const block = Buffer.alloc(Number((at + 7n) / 8n));
  for (let i = 0; i < block.length; i++) block[i] = Number((rowBits >> BigInt(8 * i)) & 0xffn);
  const nodes = Buffer.concat([block, u32(latBase), u32(lngBase), u32(0), Buffer.from([wl, wg, wn, 0]), u32(rows.length), Buffer.from('PNL3')]);
  const placeCells = Buffer.concat(rows.map((r, i) => Buffer.concat([u32(r.key), u32(i)])));
  const tiers = Array.from({ length: 5 }, (_, t) => ({ start: t === 0 ? 0 : size, end: size }));
  const [adminCells, adminEntries] = adminIndex(COUNTRY_CELL, 0);
  const files = {
    'admin_polygons.bin': polygons,
    'admin_rings.bin': rings,
    'arc_index.bin': u32(0),
    'arc_vertices.bin': arc,
    // One cell listing the country (slot 0), not interior, so a lookup there tests the point
    // against the country's arc.
    'admin_cells.bin': adminCells,
    'admin_entries.bin': adminEntries,
    'place_cells.bin': placeCells,
    'place_nodes.bin': nodes,
    'place_hidden.bin': Buffer.alloc(0),
    'strings_core.bin': fcc2(strings),
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

test('reverse geocoding finds a place in its cell', async () => {
  const geocoder = Geocoder.load(dataset(['Testville', 'Otherton']));

  assert.equal(JSON.parse(await geocoder.reverseGeocode(10.2, 20.5)).address.town, 'Testville');
  assert.equal(JSON.parse(await geocoder.reverseGeocode(10.7, 20.5)).address.town, 'Otherton');
});

test('reverse geocoding inside the country finds it by its arcs', async () => {
  const geocoder = Geocoder.load(dataset(['Testville', 'Otherton']));

  assert.equal(JSON.parse(await geocoder.reverseGeocode(10.5, 20.5)).address.country, 'Testland');
});

test('reverse geocoding off the globe finds nothing', async () => {
  const geocoder = Geocoder.load(dataset(['Testville', 'Otherton']));

  for (const [lat, lng] of [[NaN, 20.5], [10.5, NaN], [Infinity, 20.5], [10.5, -Infinity], [90.5, 20.5], [-91, 20.5], [10.5, 180.5], [10.5, -181], [10.5, 380.5]]) {
    assert.equal(await geocoder.reverseGeocode(lat, lng), '{}', `(${lat}, ${lng})`);
  }
});

test('reverse geocoding the edges of the globe looks them up', async () => {
  const geocoder = Geocoder.load(dataset(['Testville', 'Otherton']));

  // The dataset's one admin cell is far from the poles, so a lookup finds an empty address.
  for (const [lat, lng] of [[90, 180], [-90, -180]]) {
    assert.equal(await geocoder.reverseGeocode(lat, lng), '{"address":{}}', `(${lat}, ${lng})`);
  }
});

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
