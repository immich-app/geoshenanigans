// The test page's logic: index.html's <script id="core">, which reads no DOM
// (the link, the columns and their datasets, the pacing of requests, the
// comparison of answers, the place search and its matrix, and the HTML of
// each part), run in a context of its own as the page runs it. The page's
// other scripts, which wire it to the DOM and the map, are checked to parse,
// and its style for the layout of the side panel. /datasets is that of a
// build of the template, as the portal lists it
// (server/src/portal/catalog.rs).
//
//   node --test geocoder/test-portal/page.test.mjs
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import vm from 'node:vm';

const html = readFileSync(new URL('./index.html', import.meta.url), 'utf8');
const template = JSON.parse(readFileSync(new URL('../builder/configurations.template.json', import.meta.url), 'utf8'));

// The inline scripts of the page, by id.
const scripts = [...html.matchAll(/<script(?:\s+id="([\w-]+)")?>([\s\S]*?)<\/script>/g)].map((m) => ({ id: m[1], body: m[2] }));

// The declarations of the page's first style rule for `selector` (those of
// @media come after), by property.
function style(selector) {
  const css = html.match(/<style>([\s\S]*?)<\/style>/)[1].replace(/\/\*[\s\S]*?\*\//g, '');
  const rule = [...css.matchAll(/([^{}]+)\{([^{}]*)\}/g)].find((m) => m[1].split(',').map((s) => s.trim()).includes(selector));
  assert.ok(rule, `index.html has no style for ${selector}`);
  return Object.fromEntries(rule[2].split(';').filter((d) => d.includes(':'))
    .map((d) => [d.slice(0, d.indexOf(':')).trim(), d.slice(d.indexOf(':') + 1).trim()]));
}

// The core script's top-level bindings, by name.
function core() {
  const found = scripts.find((s) => s.id === 'core');
  assert.ok(found, 'index.html has no <script id="core">');
  const context = vm.createContext({ URLSearchParams, setTimeout, clearTimeout, AbortController });
  vm.runInContext(found.body, context, { filename: 'index.html#core' });
  return new Proxy({}, { get: (_, name) => vm.runInContext(String(name), context) });
}

// What /datasets answers for a build of the template as `edit` changes it:
// the axes (of region, planet only) and their values in order, the presets,
// each planet selection the constraints allow as a dataset (the preset's
// label and description for a preset's, of size its place in GiB on disk and
// half that to download) and those they rule out.
function catalog(edit = () => {}) {
  const t = structuredClone(template);
  edit(t);
  const values = (axis) => Object.entries(t.axes[axis].values).sort((a, b) => a[1].order - b[1].order || a[0].localeCompare(b[0]));
  const axes = ['region', 'mode', 'quality', 'poi_tier'].map((id) => ({
    id,
    label: t.axes[id].label,
    description: t.axes[id].description ?? null,
    values: values(id).filter(([v]) => id !== 'region' || v === 'planet').map(([v, x]) => ({ id: v, label: x.label, description: x.description ?? null })),
  }));
  const datasets = [];
  const excluded = [];
  for (const [mode, m] of values('mode')) {
    for (const [quality, q] of values('quality')) {
      for (const [poi_tier, p] of values('poi_tier')) {
        const selection = { region: 'planet', mode, quality, poi_tier };
        const id = `${mode}.${quality}.${poi_tier}`;
        const has = (o) => Object.entries(o).every(([axis, v]) => selection[axis] === v);
        const broken = t.constraints.find((c) => has(c.when) && !has(c.requires));
        if (broken) {
          excluded.push({ id, selection, constraint: broken.id, message: broken.message, requires: broken.requires });
          continue;
        }
        // A preset names the dataset of its selection but for its water,
        // which the portal serves no dataset of.
        const preset = t.presets.find((x) => has(Object.fromEntries(Object.entries(x.selections).filter(([axis]) => axis !== 'water'))));
        const n = datasets.length + 1;
        datasets.push({
          id,
          label: preset ? preset.label : [m, q, p].map((v) => v.label).join(' · '),
          description: preset ? preset.description : null,
          preset: preset ? preset.id : null,
          selection,
          size_disk: n * 2 ** 30,
          size_download: n * 2 ** 29,
        });
      }
    }
  }
  const presets = t.presets.map((p) => ({
    id: p.id, label: p.label, description: p.description, dataset: `${p.selections.mode}.${p.selections.quality}.${p.selections.poi_tier}`,
  }));
  return { build: { date: '2026-11-20', version: 24, id: ID, dir: '2026-11-20-1' }, default: 'full.simplified.all', axes, presets, datasets, excluded };
}

// The publication IDs of two builds of 2026-11-20, the second published
// again under its date (a rerun of the nightly): the date and the first 12
// hex digits of its manifest's sha256. Each fills the dir 2026-11-20-1 of a
// volume of its own.
const ID = '2026-11-20-9bc04e7d1a25';
const AGAIN = '2026-11-20-3fa2b1c4d5e6';

const MESSAGE = template.constraints[0].message;
const ADMIN_MINIMAL = { region: 'planet', mode: 'admin-minimal', quality: 'simplified', poi_tier: 'none' };
const FULL_EXACT = { region: 'planet', mode: 'full', quality: 'exact', poi_tier: 'all' };

// A template whose every string the page shows is markup: labels,
// descriptions and the constraint's message, a preset's id, and a value's
// id, which the ids of its datasets take.
function hostile(t) {
  t.presets[0].label = '<img src=x onerror=alert(1)>';
  t.presets[0].description = '"><script>alert(2)</script>';
  t.presets[1].id = '"><img src=x onerror=alert(4)>';
  t.axes.mode.label = '<u>mode</u>';
  t.axes.mode.description = '" onmouseover="alert(3)';
  t.axes.mode.values.full.label = '<b>Full</b>';
  t.axes.quality.values.exact.description = '<i>every point</i>';
  t.axes.poi_tier.values[MAJOR] = t.axes.poi_tier.values.major;
  delete t.axes.poi_tier.values.major;
  for (const p of t.presets.filter((x) => x.selections.poi_tier === 'major')) p.selections.poi_tier = MAJOR;
  t.constraints[0].message = '<em>no</em> & "quotes"';
}
const MAJOR = 'major"><u>';

// `c` with the id of its axis `from` made `to`, in the axes and in each
// selection.
function renameAxis(c, from, to) {
  c.axes.find((a) => a.id === from).id = to;
  for (const x of [...c.datasets, ...c.excluded]) {
    x.selection = Object.fromEntries(Object.entries(x.selection).map(([axis, value]) => [axis === from ? to : axis, value]));
  }
  return c;
}

// No markup of `hostile` comes through, and its text does, escaped.
function assertEscaped(out) {
  for (const tag of ['<img', '<script', '<u>', '<b>', '<i>', '<em>', '" onmouseover']) {
    assert.ok(!out.includes(tag), `${tag} came through: ${out}`);
  }
}

// A value of the page's context as a plain value of this one (deepEqual
// holds objects of two contexts unequal by their prototypes).
const plain = (x) => JSON.parse(JSON.stringify(x));

// A /reverse answer of a dataset.
function reply(status, body, headers = {}) {
  return { status, headers: { get: (name) => headers[name.toLowerCase()] ?? null }, text: async () => body };
}

// Lets the page's promises that can go on go on.
async function settle() {
  for (let i = 0; i < 5; i++) await new Promise((resolve) => setImmediate(resolve));
}

// A clock that moves only when the test moves it: now() is `t`, and a sleep
// lasts until wake() moves `t` to its end.
function handClock() {
  const clock = {
    t: 0,
    sleeping: [],
    now: () => clock.t,
    sleep: (ms) => new Promise((resolve) => clock.sleeping.push({ ms, until: clock.t + ms, resolve })),
    // The lengths of the sleeps not over, in the order they began.
    lengths: () => clock.sleeping.map((s) => s.ms),
    // Moves `t` to the end of every sleep not over, and ends them.
    wake() {
      clock.t = Math.max(clock.t, ...clock.sleeping.map((s) => s.until));
      for (const s of clock.sleeping.splice(0)) s.resolve();
    },
  };
  return clock;
}

// A Pacer of the page's on `clock`, its first pause 100 ms (and `options`
// besides), whose requests are answered when the test answers them: each in
// `sent` once it goes, with its URL, the time it went, the signal it was
// given and answer(status).
function handPacer(Pacer, clock, options = {}) {
  const sent = [];
  const pacer = new Pacer({
    firstPause: 100,
    now: clock.now,
    sleep: clock.sleep,
    fetch: (url, signal) => new Promise((resolve) => sent.push({ url, at: clock.t, signal, answer: (status) => resolve(reply(status, url)) })),
    ...options,
  });
  return { pacer, sent };
}

// The URL of each of `sent` and the time it went.
const went = (sent) => sent.map((s) => [s.url, s.at]);

test('the link names a dataset per column, the points and the build', () => {
  const { parseLink, linkSearch } = core();
  const link = parseLink('?d=with-addresses&d=admin.simplified.major&at=48.8584,2.2945&at=-33.85,151.2&b=2026-11-19');
  assert.deepEqual(plain(link), {
    datasets: ['with-addresses', 'admin.simplified.major'],
    points: [{ lat: 48.8584, lng: 2.2945 }, { lat: -33.85, lng: 151.2 }],
    build: '2026-11-19',
    q: '',
  });
  assert.equal(
    linkSearch({ datasets: ['with-addresses', 'europe/full.simplified.all'], point: { lat: 48.8584, lng: 2.2945 }, build: '2026-11-20' }),
    '?d=with-addresses&d=europe/full.simplified.all&at=48.8584,2.2945&b=2026-11-20',
  );
  assert.deepEqual(plain(parseLink(linkSearch({ datasets: ['smallest'], point: { lat: 1.000000123, lng: -2.5 }, build: null }))),
    { datasets: ['smallest'], points: [{ lat: 1.000000123, lng: -2.5 }], build: null, q: '' });
  assert.equal(linkSearch({ datasets: [], point: null, build: null }), '');
  assert.equal(parseLink(`?d=smallest&b=${AGAIN}`).build, AGAIN);
  assert.equal(linkSearch({ datasets: ['smallest'], point: null, build: AGAIN }), `?d=smallest&b=${AGAIN}`);
});

test('a link keeps 4 columns and the points on the globe', () => {
  const { parseLink } = core();
  const link = parseLink('?d=a&d=b&d=c&d=d&d=e&d=&at=91,0&at=0,181&at=x,1&at=1&at=,&at=0,0');
  assert.deepEqual(plain(link.datasets), ['a', 'b', 'c', 'd']);
  assert.deepEqual(plain(link.points), [{ lat: 0, lng: 0 }]);
});

// q= is the search box's text as it was typed: its spaces, commas and
// marks come back as they went, whatever URLSearchParams makes of a + or a #
// left bare.
test('the link carries the search box\'s text, which comes back as it went', () => {
  const { parseLink, linkSearch } = core();
  for (const q of ['paris, texas', 'a&b=c', 'rue #5', 'x+y', '50% off', 'Zürich', '東京タワー', '  spaced  ', 'd=smallest&at=0,0']) {
    const search = linkSearch({ datasets: ['with-addresses'], point: { lat: 1, lng: 2 }, build: ID, q });
    assert.ok(search.startsWith(`?d=with-addresses&at=1,2&b=${ID}&q=`), search);
    assert.equal((search.match(/&/g) || []).length, 3, `the text is one parameter: ${search}`);
    const link = parseLink(search);
    assert.equal(link.q, q);
    assert.deepEqual(plain(link.datasets), ['with-addresses']);
    assert.deepEqual(plain(link.points), [{ lat: 1, lng: 2 }]);
  }
  assert.equal(linkSearch({ datasets: [], point: null, build: null, q: 'eiffel' }), '?q=eiffel');
  assert.equal(linkSearch({ datasets: ['smallest'], point: null, build: null, q: '' }), '?d=smallest');
  assert.equal(parseLink('?q=paris+texas').q, 'paris texas');
  assert.equal(parseLink('?d=smallest').q, '');
});

test('a bare link opens with with-addresses and smallest', () => {
  const { openingTokens } = core();
  assert.deepEqual(plain(openingTokens(catalog())), ['with-addresses', 'smallest']);
  assert.deepEqual(plain(openingTokens(catalog((t) => { t.presets = t.presets.filter((p) => p.id !== 'smallest'); }))), ['with-addresses']);
  const bare = catalog();
  bare.presets = [];
  assert.deepEqual(plain(openingTokens(bare)), ['full.simplified.all']);
});

test('a column added shows a preset whose dataset no column shows', () => {
  const { addedToken, resolveColumn } = core();
  const c = catalog();
  const columns = (tokens) => tokens.map((token) => resolveColumn(c, token));
  assert.equal(addedToken(c, columns(['with-addresses', 'admin-minimal.simplified.none'])), 'immich-recommended');
  assert.equal(addedToken(c, columns(['smallest', 'admin.simplified.major', 'full.simplified.all'])), 'max-quality');
  assert.equal(addedToken(c, columns(['smallest', 'immich-recommended', 'with-addresses', 'max-quality'])), 'full.exact.none');
  assert.equal(addedToken(c, columns(['everything'])), 'smallest');
});

test('/datasets decides how the page answers: in columns, in one, or waiting for the build', () => {
  const { catalogMode } = core();
  assert.equal(catalogMode(200, catalog()), 'catalog');
  assert.equal(catalogMode(404, null), 'single');
  assert.equal(catalogMode(405, null), 'single');
  assert.equal(catalogMode(200, null), 'single');
  assert.equal(catalogMode(200, { status: 'ok' }), 'single');
  assert.equal(catalogMode(503, null), 'waiting');
  assert.equal(catalogMode(0, null), 'waiting');
});

test('a column names its dataset by a preset id or its own, and says why it has none', () => {
  const { resolveColumn } = core();
  const c = catalog();
  const preset = resolveColumn(c, 'with-addresses');
  assert.equal(preset.dataset.id, 'full.simplified.all');
  assert.equal(preset.custom, false);
  assert.equal(preset.error, null);
  const custom = resolveColumn(c, 'admin.simplified.major');
  assert.equal(custom.dataset.id, 'admin.simplified.major');
  assert.equal(custom.dataset.label, 'Immich recommended');
  assert.equal(custom.custom, true);
  const excluded = resolveColumn(c, 'admin-minimal.exact.none');
  assert.equal(excluded.dataset, null);
  assert.equal(excluded.error, `dataset admin-minimal.exact.none is not offered: ${MESSAGE}`);
  assert.equal(excluded.selection.quality, 'exact');
  const unknown = resolveColumn(c, 'everything');
  assert.equal(unknown.dataset, null);
  assert.match(unknown.error, /^unknown dataset "everything"/);
  assert.deepEqual(plain(unknown.selection), { region: 'planet', mode: 'full', quality: 'simplified', poi_tier: 'all' });
});

test('a custom pick greys out a value the constraints exclude, with its message', () => {
  const { axisChoices } = core();
  const c = catalog();
  const quality = axisChoices(c, ADMIN_MINIMAL, 'quality');
  assert.deepEqual(plain(quality.map((v) => [v.id, v.state, v.message])), [['exact', 'excluded', MESSAGE], ['simplified', 'offered', null]]);
  const mode = axisChoices(c, FULL_EXACT, 'mode');
  assert.deepEqual(plain(mode.map((v) => [v.id, v.state])), [['full', 'offered'], ['no-addresses', 'offered'], ['admin', 'offered'], ['admin-minimal', 'moves']]);
  assert.equal(mode[3].message, MESSAGE);
  assert.equal(mode[3].selection.quality, 'simplified');
});

test('a value whose constraint requires another moves that axis and says why', () => {
  const { pick } = core();
  const c = catalog();
  const moved = pick(c, FULL_EXACT, 'mode', 'admin-minimal');
  assert.equal(moved.dataset.id, 'admin-minimal.simplified.all');
  assert.equal(moved.note, `Boundary precision moves to Simplified: ${MESSAGE}`);
  const plain = pick(c, FULL_EXACT, 'poi_tier', 'none');
  assert.equal(plain.dataset.id, 'full.exact.none');
  assert.equal(plain.note, null);
  assert.equal(pick(c, ADMIN_MINIMAL, 'quality', 'exact'), null);
});

test('the picker offers the presets with their sizes, and the axes of a custom pick', () => {
  const { pickerHtml, resolveColumn } = core();
  const c = catalog();
  const preset = pickerHtml(c, resolveColumn(c, 'smallest'), 0, 2);
  for (const p of c.presets) {
    const size = (c.datasets.find((d) => d.id === p.dataset).size_disk / 2 ** 30).toFixed(2);
    assert.match(preset, new RegExp(`<option value="${p.id}"[^>]*>${p.label} · ${size} GiB</option>`), p.id);
  }
  assert.match(preset, /<option value="smallest" selected[^>]*>Smallest useful/);
  assert.ok(preset.includes('25.00 GiB on disk · 12.50 GiB download'), preset);
  assert.ok(preset.includes('data-action="remove"'));
  assert.ok(!preset.includes('data-axis='));
  assert.ok(!pickerHtml(c, resolveColumn(c, 'smallest'), 0, 1).includes('data-action="remove"'));

  const custom = pickerHtml(c, resolveColumn(c, 'admin-minimal.simplified.all'), 1, 2);
  assert.match(custom, /<option value="" selected>Custom/);
  for (const axis of ['mode', 'quality', 'poi_tier']) {
    assert.ok(custom.includes(`data-axis="${axis}"`), axis);
  }
  assert.ok(!custom.includes('data-axis="region"'), 'a region axis of one value is not offered');
  assert.match(custom, /<option value="exact" disabled title="[^"]*">Exact \(not offered\)<\/option>/);
  assert.ok(custom.includes(`Exact: ${MESSAGE}`), custom);
});

// The pickers are drawn anew on each change: the control that had the focus
// gets it back by its key (of the data-* attributes of its element), which
// each control of the pickers has of its own.
test('each control of the pickers has a key of its own, which finds it once they are drawn anew', () => {
  const { pickerHtml, resolveColumn, controlKey } = core();
  const c = catalog();
  const columns = [resolveColumn(c, 'smallest'), resolveColumn(c, 'admin.exact.major')];
  const controls = (html) => [...html.matchAll(/<(?:select|button)\b([^>]*)>/g)]
    .map((m) => Object.fromEntries([...m[1].matchAll(/data-(\w+)="([^"]*)"/g)].map((a) => [a[1], a[2]])));
  const keys = (cols) => controls(cols.map((col, i) => pickerHtml(c, col, i, cols.length)).join('')).map((data) => controlKey(data));
  const drawn = keys(columns);
  assert.equal(drawn.length, 7, 'a preset and a remove button each, and three axes of the custom pick');
  assert.ok(drawn.every((key) => key !== null));
  assert.equal(new Set(drawn).size, drawn.length);
  // The second column's mode moved to admin-minimal: its controls keep their keys.
  const moved = keys([columns[0], resolveColumn(c, 'admin-minimal.simplified.major')]);
  assert.deepEqual(moved, drawn);
  assert.equal(controlKey({}), null);
});

// A column of a link's dataset the build doesn't offer is labelled by the
// link's token, as the page labels it (columnLabel).
test('every string and id of the manifest, and a link\'s dataset, is escaped where the page shows it', () => {
  const { pickerHtml, resolveColumn, pick, detailHtml, cardHtml, statusHtml } = core();
  const c = renameAxis(catalog(hostile), 'poi_tier', 'poi"><b>');
  const moved = pick(c, c.datasets.find((d) => d.id === 'full.exact.all').selection, 'mode', 'admin-minimal');
  assert.ok(moved.note.includes('<em>no</em>'), moved.note);
  const linked = resolveColumn(c, '<script>alert(5)</script>');
  const columns = [resolveColumn(c, 'smallest'), resolveColumn(c, 'admin-minimal.exact.none'), resolveColumn(c, 'full.exact.all'),
    { ...resolveColumn(c, moved.dataset.id), note: moved.note }, resolveColumn(c, `admin.simplified.${MAJOR}`), linked];
  const picker = columns.map((col, i) => pickerHtml(c, col, i, columns.length)).join('');
  assertEscaped(picker);
  assert.ok(picker.includes('&lt;img src=x onerror=alert(1)&gt;'));
  assert.ok(picker.includes('&lt;em&gt;no&lt;/em&gt; &amp; &quot;quotes&quot;'));
  assert.ok(picker.includes('<div class="picker-note">Boundary precision moves to Simplified: &lt;em&gt;no&lt;/em&gt;'), 'the note of a pick');
  assert.ok(picker.includes('<option value="&quot;&gt;&lt;img src=x onerror=alert(4)&gt;"'), 'a preset id');
  assert.ok(picker.includes('data-axis="poi&quot;&gt;&lt;b&gt;"'), 'an axis id');
  assert.ok(picker.includes('<option value="major&quot;&gt;&lt;u&gt;"'), 'a value id');
  assert.ok(picker.includes(' · admin.simplified.major&quot;&gt;&lt;u&gt;</div>'), 'a dataset id');
  assert.ok(picker.includes('unknown dataset &quot;&lt;script&gt;alert(5)&lt;/script&gt;&quot;'), "a link's dataset");

  const dataset = c.datasets.find((d) => d.id === 'admin-minimal.simplified.none');
  const answer = { data: { display_name: '<img src=y>', address: { road: '<b>x</b>' }, places: [{ name: '<i>p</i>', category: '<u>c</u>', distance_m: 3 }] }, build: '<s>', ms: 3 };
  const views = [{ label: dataset.label, color: '#fff', dataset, answer }, { label: linked.token, color: '#000', dataset: null, answer: { error: linked.error } }];
  const detail = detailHtml(views, '2026-11-20');
  const card = cardHtml({ id: 1, lat: 1, lng: 2, source: '<img src=s>' }, views, false);
  for (const out of [detail, card]) {
    assertEscaped(out);
    assert.ok(!out.includes('<s>'));
  }
  assert.ok(card.includes('<div class="result-error">&lt;script&gt;alert(5)&lt;/script&gt;: unknown dataset'), card);
  assertEscaped(statusHtml({ status: 'unavailable', reason: '<img src=r>', sync: { running: '<b>r</b>', refused: [{ date: '<i>', version: 1, reason: '<script>', at: '' }], last_result: null } }));
});

// The build is its publication ID (X-Geocoder-Build-Id), which tells a build
// published again under its date from the one before, on any volume; not
// the portal's dir of it (X-Geocoder-Build-Dir), which is its volume's.
test('an answer keeps the dataset and build that answered and the time it took', () => {
  const { answerOf } = core();
  const headers = { 'x-geocoder-build': '2026-11-20', 'x-geocoder-build-id': AGAIN, 'x-geocoder-build-dir': '2026-11-20-1', 'x-geocoder-dataset': 'full.simplified.all' };
  const header = (name) => headers[name.toLowerCase()] ?? null;
  assert.deepEqual(plain(answerOf(200, header, '{"display_name":"Paris"}', 12.4)),
    { data: { display_name: 'Paris' }, build: AGAIN, dataset: 'full.simplified.all', ms: 12.4 });
  const refused = answerOf(400, () => null, 'dataset admin-minimal.exact.none is not offered: x', 2);
  assert.equal(refused.error, 'HTTP 400: dataset admin-minimal.exact.none is not offered: x');
  assert.equal(refused.data, undefined);
  assert.equal(answerOf(200, () => null, 'not json', 1).error, 'not JSON: not json');
});

// A column without an answer (null) lacks no field.
test('the field table marks the fields the answers disagree on or lack', () => {
  const { fieldRows } = core();
  const rows = fieldRows([{ city: 'Paris', road: 'Rue A', zzz: '1', landmark: 'Tower' }, { city: 'Paris', road: 'Rue B' }, null]);
  assert.deepEqual(plain(rows), [
    { field: 'landmark', values: ['Tower', '', ''], differs: false, missing: true },
    { field: 'road', values: ['Rue A', 'Rue B', ''], differs: true, missing: false },
    { field: 'city', values: ['Paris', 'Paris', ''], differs: false, missing: false },
    { field: 'zzz', values: ['1', '', ''], differs: false, missing: true },
  ]);
  assert.deepEqual(plain(fieldRows([{ city: 'Paris' }, { city: 'Paris' }])), [{ field: 'city', values: ['Paris', 'Paris'], differs: false, missing: false }]);
});

test('answers differ by display name, a field, or the places and their distances', () => {
  const { answersDiffer } = core();
  const paris = { display_name: 'Paris', address: { city: 'Paris' }, places: [{ name: 'Louvre', category: 'museum', distance_m: 12 }] };
  assert.equal(answersDiffer([paris, structuredClone(paris)]), false);
  assert.equal(answersDiffer([paris, null]), false);
  assert.equal(answersDiffer([paris, { ...paris, display_name: 'Paris 1er' }]), true);
  assert.equal(answersDiffer([paris, { ...paris, address: { city: 'Paris', postcode: '75001' } }]), true);
  assert.equal(answersDiffer([paris, { ...paris, places: [] }]), true);
  assert.equal(answersDiffer([paris, { ...paris, places: [{ name: 'Louvre', category: 'museum', distance_m: 40 }] }]), true);
});

test('the detail shows per answer its dataset, build, sizes and time, and flags another build', () => {
  const { detailHtml } = core();
  const c = catalog();
  const dataset = c.datasets.find((d) => d.id === 'full.simplified.all');
  const answer = (city, build) => ({ data: { display_name: city, address: { city } }, build, ms: 12.4 });
  const views = [
    { label: dataset.label, color: '#111', dataset, answer: answer('Paris', '2026-11-20') },
    { label: 'other', color: '#222', dataset, answer: answer('Lyon', '2026-11-19') },
    { label: 'loading', color: '#333', dataset, answer: { loading: true } },
  ];
  const out = detailHtml(views, '2026-11-20');
  assert.ok(out.includes('full.simplified.all · build 2026-11-20 · 12 ms'), out);
  assert.ok(out.includes(`${(dataset.size_disk / 2 ** 30).toFixed(2)} GiB on disk · ${(dataset.size_download / 2 ** 30).toFixed(2)} GiB download`));
  // The sizes are those of the build served: an answer of another build goes
  // without them.
  assert.equal(out.match(/ GiB on disk · /g).length, 2, out);
  assert.ok(!detailHtml([views[1]], '2026-11-20').includes('on disk'));
  assert.match(out, /class="tag stale"[^>]*>build 2026-11-19/);
  assert.equal(out.match(/class="tag stale"/g).length, 1);
  assert.match(out, /<tr class="differs">\s*<td[^>]*>city<\/td>/);
  assert.ok(out.includes('loading-spinner'));

  // Of the builds of one date, the one published before is another; a date
  // alone (a link's b= of a date) is any.
  const republished = detailHtml([
    { label: 'before', color: '#111', dataset, answer: answer('Paris', ID) },
    { label: 'now', color: '#222', dataset, answer: answer('Paris', AGAIN) },
    { label: 'saved', color: '#333', dataset, answer: answer('Paris', '2026-11-20') },
  ], AGAIN);
  assert.match(republished, new RegExp(`class="tag stale"[^>]*>build ${ID}, not ${AGAIN}`));
  assert.equal(republished.match(/class="tag stale"/g).length, 1, republished);
});

// The detail keeps a result's open JSON panels open as its answers come in:
// each is named by the place of its view, whatever views before it have no
// answer yet.
test('each JSON panel of the detail is named by the place of its view', () => {
  const { detailHtml } = core();
  const view = (label, answer) => ({ label, color: '#111', dataset: null, answer });
  const out = detailHtml([view('a', { loading: true }), view('b', { data: { display_name: 'B' } }), view('c', null), view('d', { data: { display_name: 'D' } })], null);
  assert.deepEqual([...out.matchAll(/<details data-view="(\d+)"/g)].map((m) => m[1]), ['1', '3']);
  assert.equal(out.match(/<details/g).length, 2);
});

test('a card names the first answer and says when the columns differ', () => {
  const { cardHtml } = core();
  const view = (city) => ({ label: city, color: '#111', dataset: null, answer: { data: { display_name: city, address: { city } } } });
  const same = cardHtml({ id: 7, lat: 48.85, lng: 2.29, source: 'map click' }, [view('Paris'), view('Paris')], true);
  assert.match(same, /^<div class="result-card selected" data-id="7">/);
  assert.ok(same.includes('Paris') && !same.includes('differs'));
  assert.ok(cardHtml({ id: 7, lat: 48.85, lng: 2.29 }, [view('Paris'), view('Lyon')], false).includes('differs'));
  const failed = cardHtml({ id: 8, lat: 1, lng: 2 }, [view('Paris'), { label: 'Smallest', color: '#222', dataset: null, answer: { error: 'HTTP 503: no build' } }], false);
  assert.ok(failed.includes('Smallest: HTTP 503: no build'), failed);
});

// The columns' answers of a result may come from builds other than the one
// served now (saved, or asked before the build changed): its link names the
// oldest of those, so that a link to answers the server no longer gives is
// flagged. Where the portal went back to an older build, an answer of the
// newer one is such an answer. Of two publications of one date, which came
// first their IDs don't tell: they go by ID.
test('a link to a result names the oldest build other than the one served that answered it in the columns', () => {
  const { linkBuild } = core();
  const answers = {
    'full.simplified.all': { data: {}, build: '2026-11-20' },
    'admin-minimal.simplified.none': { data: {}, build: '2026-11-19' },
    'admin.simplified.major': { loading: true },
    'full.exact.all': { data: {}, build: '2026-11-18' },
  };
  assert.equal(linkBuild(answers, ['full.simplified.all', 'admin-minimal.simplified.none', 'admin.simplified.major'], '2026-11-21'), '2026-11-19');
  assert.equal(linkBuild(answers, ['full.simplified.all'], '2026-11-21'), '2026-11-20');
  assert.equal(linkBuild(answers, ['admin.simplified.major', 'no-addresses.exact.none'], '2026-11-21'), '2026-11-21');
  assert.equal(linkBuild({ '': { data: {} } }, [''], null), null);
  const NEXT = '2026-11-21-0d1e2f3a4b5c';
  const republished = {
    a: { data: {}, build: ID },
    b: { data: {}, build: AGAIN },
    c: { data: {}, build: NEXT },
  };
  assert.equal(linkBuild(republished, ['a', 'b', 'c'], NEXT), AGAIN);
  assert.equal(linkBuild(republished, ['a', 'c'], NEXT), ID);
  // Served again after NEXT: that build's answer saved beside a fresh one.
  assert.equal(linkBuild(republished, ['a', 'c'], ID), NEXT);
  assert.equal(linkBuild(republished, ['b', 'a', 'c'], ID), AGAIN);
  assert.equal(linkBuild(republished, ['a'], ID), ID);
  assert.equal(linkBuild({ a: { data: {}, build: '2026-11-20' } }, ['a'], AGAIN), '2026-11-20');
});

test('a link of another build than the one served is flagged', () => {
  const { mismatchHtml } = core();
  const flagged = mismatchHtml('2026-11-19', '2026-11-20');
  assert.ok(flagged.includes('2026-11-19') && flagged.includes('2026-11-20'), flagged);
  assert.equal(mismatchHtml('2026-11-20', '2026-11-20'), '');
  assert.equal(mismatchHtml(null, '2026-11-20'), '');
  assert.equal(mismatchHtml('2026-11-19', null), '');
  assertEscaped(mismatchHtml('<img src=x>', '2026-11-20'));
  // A link of one publication of a date, opened where the other is served
  // (both in the dir 2026-11-20-1 of their volumes), is flagged; one of the
  // date alone opens as of either.
  const republished = mismatchHtml(ID, AGAIN);
  assert.ok(republished.includes(`build ${ID}`) && republished.includes(`build ${AGAIN}`), republished);
  assert.equal(mismatchHtml(AGAIN, AGAIN), '');
  assert.equal(mismatchHtml('2026-11-20', AGAIN), '');
  assert.ok(mismatchHtml('2026-11-19', AGAIN).includes('2026-11-19'));
});

// /health names the build served by its publication ID: once that is
// another than the one the columns are of (a build published again under its
// date among them, in a dir of the name of the one before), or one is served
// while the page waits for a build, the page loads /datasets anew.
test('the page loads the datasets anew once /health names another build', () => {
  const { catalogStale } = core();
  const c = catalog();
  const health = (id, dir) => ({ status: 'ok', build: { date: id.slice(0, 10), version: 24, id, dir }, datasets: 28, sync: {} });
  assert.equal(catalogStale(health(ID, '2026-11-20-1'), 'catalog', c), false);
  assert.equal(catalogStale(health(AGAIN, '2026-11-20-2'), 'catalog', c), true);
  assert.equal(catalogStale(health(AGAIN, '2026-11-20-1'), 'catalog', c), true);
  assert.equal(catalogStale(health('2026-11-21-0d1e2f3a4b5c', '2026-11-21-1'), 'catalog', c), true);
  assert.equal(catalogStale(health(ID, '2026-11-20-1'), 'waiting', null), true);
  assert.equal(catalogStale({ status: 'unavailable', reason: 'no build to serve', sync: {} }, 'waiting', null), false);
  assert.equal(catalogStale(null, 'catalog', c), false);
  assert.equal(catalogStale(health(AGAIN, '2026-11-20-1'), 'single', null), false);
});

test('the status line tells the build served and how the sync goes', () => {
  const { statusHtml } = core();
  const sync = {
    running: null, progress: null, last_started: '2026-11-20T07:10:00Z', last_finished: '2026-11-20T07:12:03Z', next_round: '2026-11-20T08:12:03Z',
    last_result: 'ok: serving 2026-11-20-1', latest: { date: '2026-11-21', build_version: 25 }, served: '2026-11-20-1',
    refused: [{ date: '2026-11-21', version: 25, reason: 'build version 25, not 24', at: '2026-11-21T07:00:00Z' }],
  };
  const ok = statusHtml({ status: 'ok', build: { date: '2026-11-20', version: 24, id: ID, dir: '2026-11-20-1' }, datasets: 28, sync });
  for (const text of [`build ${ID} (v24, dir 2026-11-20-1) · 28 datasets`, 'last sync 2026-11-20 07:12:03 UTC: ok: serving 2026-11-20-1', 'next 2026-11-20 08:12:03 UTC',
    'latest 2026-11-21', 'refused 2026-11-21 (v25): build version 25, not 24']) {
    assert.ok(ok.includes(text), `${text}: ${ok}`);
  }
  const running = statusHtml({ status: 'unavailable', reason: 'no build to serve: waiting for a build of version 24', sync: {
    ...sync, refused: [], running: 'fetching 2026-11-21', progress: { objects_done: 3, objects_total: 88, bytes_done: 2 ** 30, bytes_total: 9 * 2 ** 30 } } });
  assert.ok(running.includes('no build to serve: waiting for a build of version 24'), running);
  assert.ok(running.includes('sync: fetching 2026-11-21 (3/88 objects, 1.00 GiB of 9.00 GiB)'), running);
  assert.equal(statusHtml(null), '');
  assert.equal(statusHtml({ status: 'ok' }), '');
});

// A request holds its place until its body is in: the headers of 8 hold
// every place while their bodies come, and each body in lets one more go.
test('the requests in flight are at most 8, over every column, each until its body is in', async () => {
  const { Pacer } = core();
  const started = [];
  const pacer = new Pacer({
    fetch: async (url) => {
      let deliver;
      const body = new Promise((resolve) => { deliver = resolve; });
      started.push({ url, deliver });
      return { status: 200, headers: { get: () => null }, text: () => body };
    },
  });
  const requests = Array.from({ length: 40 }, (_, i) => pacer.request(`/reverse?n=${i}`));
  await settle();
  assert.equal(started.length, 8);
  for (let delivered = 0; delivered < 40; delivered++) {
    started[delivered].deliver('{}');
    await settle();
    assert.equal(started.length, Math.min(40, delivered + 9), `${delivered + 1} bodies in`);
  }
  const got = await Promise.all(requests);
  assert.ok(got.every((g) => g.status === 200 && g.body === '{}' && g.ms >= 0));
  assert.deepEqual(started.map((s) => s.url), Array.from({ length: 40 }, (_, i) => `/reverse?n=${i}`));
  assert.deepEqual([pacer.active, pacer.waiting.length], [0, 0]);
});

test('a 429 is asked again after a pause, which every request waits out', async () => {
  const { Pacer } = core();
  const clock = handClock();
  const { pacer, sent } = handPacer(Pacer, clock);
  const a = pacer.request('a');
  await settle();
  sent[0].answer(429);
  await settle();
  // b, asked within the pause, waits it out with a's second ask.
  const b = pacer.request('b');
  await settle();
  assert.deepEqual(went(sent), [['a', 0]]);
  assert.deepEqual(clock.lengths(), [100, 100]);
  clock.wake();
  await settle();
  assert.deepEqual(went(sent.slice(1)).sort(), [['a', 100], ['b', 100]]);
  for (const s of sent.slice(1)) s.answer(200);
  assert.deepEqual([(await a).status, (await b).status], [200, 200]);
});

// Each pause is twice the one before, up to the longest; after the last of
// its tries a 429 is the answer, with no pause after it.
test('the pause doubles with each 429 in a row, and the last of its tries is the answer', async () => {
  const { Pacer } = core();
  for (const [maxPause, pauses] of [[8000, [20, 40, 80]], [50, [20, 40, 50]]]) {
    const clock = handClock();
    const { pacer, sent } = handPacer(Pacer, clock, { firstPause: 20, tries: 4, maxPause });
    const got = pacer.request('x');
    for (const pause of pauses) {
      await settle();
      sent.at(-1).answer(429);
      await settle();
      assert.deepEqual(clock.lengths(), [pause], `the pause after ${sent.length} 429s, of at most ${maxPause} ms`);
      clock.wake();
    }
    await settle();
    assert.equal(sent.length, 4);
    sent[3].answer(429);
    const last = await got;
    assert.deepEqual([last.status, last.body], [429, 'x']);
    assert.deepEqual(clock.lengths(), []);
    assert.equal(pacer.active, 0);
  }
});

test('the 429s of requests in flight together make one pause', async () => {
  const { Pacer } = core();
  let calls = 0;
  let open;
  const gate = new Promise((resolve) => { open = resolve; });
  const pauses = [];
  const pacer = new Pacer({
    firstPause: 30,
    fetch: async () => {
      calls++;
      if (calls <= 8) {
        if (calls === 8) open();
        await gate;
        return reply(429, 'Rate limit exceeded');
      }
      pauses.push(pacer.scope('reverse').pause);
      return reply(200, '{}');
    },
  });
  const got = await Promise.all(Array.from({ length: 8 }, (_, i) => pacer.request(`/reverse?n=${i}`)));
  assert.ok(got.every((g) => g.status === 200));
  assert.equal(pauses.length, 8);
  assert.equal(Math.max(...pauses), 30);
});

test('a /polygons request goes while the reverse lookups wait out a pause, which holds no place', async () => {
  const { Pacer } = core();
  const clock = handClock();
  const { pacer, sent } = handPacer(Pacer, clock);
  const lookups = Array.from({ length: 12 }, (_, i) => pacer.request(`/reverse?n=${i}`, 'reverse'));
  await settle();
  assert.equal(sent.length, 8);
  for (const s of sent) s.answer(429);
  await settle();

  const polygons = pacer.request('/polygons?n=1', 'polygons');
  await settle();
  assert.deepEqual(went(sent.slice(8)), [['/polygons?n=1', 0]]);
  sent[8].answer(200);
  assert.equal((await polygons).status, 200);
  // Once the pause is over every lookup goes again, 8 at a time.
  clock.wake();
  await settle();
  assert.equal(sent.length, 17);
  for (const s of sent.slice(9)) s.answer(200);
  await settle();
  assert.equal(sent.length, 21);
  for (const s of sent.slice(17)) s.answer(200);
  assert.ok((await Promise.all(lookups)).every((g) => g.status === 200));
  assert.deepEqual(sent.slice(9).map((s) => s.url).sort(), Array.from({ length: 12 }, (_, i) => `/reverse?n=${i}`).sort());
  assert.ok(sent.slice(9).every((s) => s.at === 100));
});

test('a /polygons answer leaves the pause of the reverse lookups as it was', async () => {
  const { Pacer } = core();
  const clock = handClock();
  const { pacer, sent } = handPacer(Pacer, clock);
  const lookup = pacer.request('/reverse?n=1', 'reverse');
  await settle();
  sent[0].answer(429);
  await settle();
  assert.deepEqual(clock.lengths(), [100]);
  clock.wake();
  await settle();
  const polygons = pacer.request('/polygons?n=1', 'polygons');
  await settle();
  assert.deepEqual(went(sent), [['/reverse?n=1', 0], ['/reverse?n=1', 100], ['/polygons?n=1', 100]]);
  sent[2].answer(200);
  assert.equal((await polygons).status, 200);

  // The lookup asked again after the pause began: its 429 doubles it.
  sent[1].answer(429);
  await settle();
  assert.deepEqual(clock.lengths(), [200]);
  clock.wake();
  await settle();
  sent[3].answer(200);
  assert.equal((await lookup).status, 200);
});

test('the pause of the reverse lookups ends only with a success of one started after it began', async () => {
  const { Pacer } = core();
  const clock = handClock();
  const { pacer, sent } = handPacer(Pacer, clock);
  const a = pacer.request('/reverse?a', 'reverse');
  const b = pacer.request('/reverse?b', 'reverse');
  await settle();
  clock.t = 10;
  sent[0].answer(429);
  await settle();
  assert.deepEqual(clock.lengths(), [100]);
  // b went before the pause began: its success ends nothing.
  sent[1].answer(200);
  assert.equal((await b).status, 200);
  clock.wake();
  await settle();
  sent[2].answer(429);
  await settle();
  assert.deepEqual(clock.lengths(), [200]);

  // c goes after the pause began, but its answer is no success.
  const c = pacer.request('/reverse?c', 'reverse');
  await settle();
  clock.wake();
  await settle();
  assert.deepEqual(went(sent.slice(3)).sort(), [['/reverse?a', 310], ['/reverse?c', 310]]);
  const [askedA, askedC] = ['/reverse?a', '/reverse?c'].map((url) => sent.slice(3).find((s) => s.url === url));
  askedC.answer(503);
  assert.equal((await c).status, 503);
  askedA.answer(429);
  await settle();
  assert.deepEqual(clock.lengths(), [400]);

  // a's success, of a request that went after the pause began, ends it:
  // the next 429 pauses anew.
  clock.wake();
  await settle();
  sent[5].answer(200);
  assert.equal((await a).status, 200);
  const d = pacer.request('/reverse?d', 'reverse');
  await settle();
  sent[6].answer(429);
  await settle();
  assert.deepEqual(clock.lengths(), [100]);
  clock.wake();
  await settle();
  sent[7].answer(200);
  assert.equal((await d).status, 200);
});

test('a request that fails frees its place', async () => {
  const { Pacer } = core();
  let calls = 0;
  const pacer = new Pacer({ limit: 1, fetch: async () => { calls++; if (calls === 1) throw new Error('offline'); return reply(200, 'ok'); } });
  await assert.rejects(pacer.request('a'), /offline/);
  assert.equal((await pacer.request('b')).body, 'ok');
});

test("Nominatim's column shows its answer, its request in flight, or why it failed", () => {
  const { nominatimView, detailHtml } = core();
  assert.deepEqual(plain(nominatimView({ nominatim: { display_name: 'Paris' } }).answer), { data: { display_name: 'Paris' } });
  assert.deepEqual(plain(nominatimView({ nominatim: null, nominatimLoading: true }).answer), { loading: true });
  const failed = nominatimView({ nominatim: null, nominatimError: 'HTTP 429: Too Many Requests' });
  assert.deepEqual(plain(failed.answer), { error: 'HTTP 429: Too Many Requests' });
  const out = detailHtml([failed], '2026-11-20');
  assert.ok(out.includes('<div class="result-error">HTTP 429: Too Many Requests</div>') && !out.includes('not asked yet'), out);
});

// The page before the dataset columns saved a result's answer of its server
// as data (or error) and that of the minimal one as minimal, of datasets it
// did not record: they go, and the columns are asked again. The point and
// Nominatim's answer stay.
test('a saved result keeps its answers; one the page saved before the dataset columns keeps its point', () => {
  const { storedResult } = core();
  const now = { id: 'a', lat: 1, lng: 2, source: 'map click', nominatim: null,
    answers: { 'admin.simplified.major': { data: { display_name: 'Paris' }, build: '2026-11-20', dataset: 'admin.simplified.major', ms: 3 } } };
  assert.deepEqual(plain(storedResult(now)), now);
  const before = { id: 1763600000000, lat: 48.85, lng: 2.29, data: { display_name: 'Paris' }, nominatim: { display_name: 'Paris, France' },
    minimal: { display_name: 'France' }, source: 'IMG_1.jpg' };
  assert.deepEqual(plain(storedResult(before)), {
    id: 1763600000000, lat: 48.85, lng: 2.29, source: 'IMG_1.jpg', nominatim: { display_name: 'Paris, France' }, answers: {},
  });
  assert.deepEqual(plain(storedResult({ id: 2, lat: 1, lng: 2, data: null, minimal: null, error: 'HTTP 503' }).answers), {});
  assert.equal(storedResult({ id: 3, lat: 91, lng: 0, answers: {} }), null);
  assert.equal(storedResult(null), null);
});

// The answers of a column no longer shown are saved too: each keeps the
// build that gave it, which a link to the result names and the detail
// flags. One being asked is not.
test('a result is saved with every answer it has but those being asked, and read back as it was', () => {
  const { savedResult, storedResult } = core();
  const entry = { id: 'a', lat: 1, lng: 2, source: 'map click', nominatim: { display_name: 'Paris, France' }, nominatimLoading: false, nominatimError: null,
    answers: {
      'full.simplified.all': { data: { display_name: 'Paris' }, build: '2026-11-20-1', dataset: 'full.simplified.all', ms: 3 },
      'admin.simplified.major': { data: { display_name: 'Paris' }, build: '2026-11-19-1', dataset: 'admin.simplified.major', ms: 4 },
      'admin-minimal.simplified.none': { error: 'HTTP 502: Bad Gateway', build: null, dataset: null, ms: 1 },
      'no-addresses.exact.none': { loading: true, pending: Promise.resolve() },
    } };
  const saved = JSON.parse(JSON.stringify(savedResult(entry)));
  assert.deepEqual(saved, {
    id: 'a', lat: 1, lng: 2, source: 'map click', nominatim: { display_name: 'Paris, France' },
    answers: Object.fromEntries(Object.entries(entry.answers).filter(([key]) => key !== 'no-addresses.exact.none')),
  });
  assert.deepEqual(plain(storedResult(saved)), saved);
});

// A save the browser's storage refuses (full, or off) says why, which the
// page shows beside the results' count until a save goes in: the results
// saved before stay as they were.
test('a save the storage refuses says why', () => {
  const { keep } = core();
  const kept = new Map();
  const storage = { setItem: (key, value) => kept.set(key, value) };
  assert.equal(keep(storage, 'results', '[1]'), '');
  assert.equal(kept.get('results'), '[1]');
  const full = { setItem: () => { throw new Error("Failed to execute 'setItem' on 'Storage': exceeded the quota."); } };
  assert.equal(keep(full, 'results', '[1,2]'), "not saved: Failed to execute 'setItem' on 'Storage': exceeded the quota.");
  assert.ok(html.includes('<span id="saveError" class="warn"></span>'));
});

test('the polygon overlay cycles off, each column, off', () => {
  const { nextPolygons } = core();
  assert.deepEqual([nextPolygons('off', 2), nextPolygons(0, 2), nextPolygons(1, 2)], [0, 1, 'off']);
  assert.equal(nextPolygons('off', 0), 'off');
  assert.equal(nextPolygons(3, 2), 'off');
});

test('polygons are drawn only while no clear came after their request and its result and column are still shown', async () => {
  const { PolygonOverlay } = core();
  const asked = [];
  const drawn = [];
  const KEY = 'full.simplified.all';
  let shown = { id: 'a', key: KEY };
  const overlay = new PolygonOverlay({
    // What the pacer gives.
    request: () => new Promise((resolve) => asked.push((n) => resolve({ status: 200, header: () => null, body: JSON.stringify({ type: 'FeatureCollection', features: [], n }), ms: 1 }))),
    draw: (fc) => drawn.push(fc.n),
    erase: () => drawn.push('erased'),
    shown: () => shown,
  });
  // A clear (the overlay's column removed, the results cleared) leaves
  // nothing drawn, whatever comes in after it.
  const cleared = overlay.show('a', KEY, '/polygons?n=1');
  overlay.clear();
  asked[0](1);
  assert.equal(await cleared, false);
  assert.deepEqual(drawn, ['erased']);
  // Nor while another column or result is shown.
  const otherColumn = overlay.show('a', KEY, '/polygons?n=2');
  shown = { id: 'a', key: 'admin.simplified.none' };
  asked[1](2);
  assert.equal(await otherColumn, false);
  shown = { id: 'a', key: KEY };
  const otherResult = overlay.show('a', KEY, '/polygons?n=3');
  shown = { id: 'b', key: KEY };
  asked[2](3);
  assert.equal(await otherResult, false);
  // Of two in flight, the last asked.
  const earlier = overlay.show('b', KEY, '/polygons?n=4');
  const later = overlay.show('b', KEY, '/polygons?n=5');
  asked[4](5);
  asked[3](4);
  assert.deepEqual([await later, await earlier], [true, false]);
  assert.deepEqual(drawn, ['erased', 5]);
  await settle();
  assert.deepEqual(drawn, ['erased', 5]);
});

// A reload of /datasets or a picker may give the overlay's column another
// dataset, the columns another build, or leave the overlay no column: what
// it showed is then of none of them, and goes. A point selected while the
// portal waited for its build was asked of the one column of the server as
// it was.
test('a reload of the datasets or a picker moves the polygon overlay where its dataset or build is another, or it is off', () => {
  const { overlayTarget, overlayMoved, resolveColumn } = core();
  const c = catalog();
  const columns = (tokens) => tokens.map((token) => resolveColumn(c, token));
  const shown = overlayTarget(0, columns(['with-addresses', 'smallest']), '2026-11-20-1');
  assert.deepEqual(plain(shown), { key: 'full.simplified.all', build: '2026-11-20-1' });
  assert.equal(overlayMoved(shown, overlayTarget(0, columns(['with-addresses', 'smallest']), '2026-11-20-1')), false);
  assert.equal(overlayMoved(shown, overlayTarget(0, columns(['with-addresses', 'smallest']), '2026-11-20-2')), true);
  assert.equal(overlayMoved(shown, overlayTarget(0, columns(['smallest', 'with-addresses']), '2026-11-20-1')), true);
  assert.equal(overlayTarget('off', columns(['with-addresses']), '2026-11-20-1'), null);
  assert.equal(overlayTarget(1, columns(['with-addresses']), '2026-11-20-1'), null);
  assert.equal(overlayTarget(0, columns(['everything']), '2026-11-20-1'), null);
  assert.equal(overlayMoved(shown, null), true);
  assert.equal(overlayMoved(null, null), false);
  const waiting = overlayTarget(0, [{ single: true }], null);
  assert.deepEqual(plain(waiting), { key: '', build: null });
  assert.equal(overlayMoved(waiting, shown), true);
  // The picker of the overlay's column moves it to another dataset; that of
  // another column, or a column removed before it (its place shifts, its
  // dataset stays), leaves it where it was.
  assert.equal(overlayMoved(shown, overlayTarget(0, columns(['max-quality', 'smallest']), '2026-11-20-1')), true);
  assert.equal(overlayMoved(shown, overlayTarget(0, columns(['with-addresses', 'max-quality']), '2026-11-20-1')), false);
  assert.equal(overlayMoved(overlayTarget(1, columns(['smallest', 'with-addresses']), '2026-11-20-1'),
    overlayTarget(0, columns(['with-addresses']), '2026-11-20-1')), false);
});

test('a polygon request is cancelled once another show or a clear comes after it', { timeout: 5000 }, async () => {
  const { PolygonOverlay } = core();
  const signals = [];
  const overlay = new PolygonOverlay({
    request: (url, signal) => {
      signals.push(signal);
      return new Promise((resolve, reject) => { if (signal) signal.addEventListener('abort', () => reject(signal.reason)); });
    },
    draw: () => assert.fail('nothing is drawn'),
    erase: () => {},
    shown: () => ({ id: 'b', key: 'full.simplified.all' }),
  });
  const first = overlay.show('a', 'full.simplified.all', '/polygons?n=1');
  const second = overlay.show('b', 'full.simplified.all', '/polygons?n=2');
  assert.deepEqual(signals.map((s) => Boolean(s && s.aborted)), [true, false]);
  assert.equal(await first, false);
  overlay.clear();
  assert.equal(signals[1].aborted, true);
  assert.equal(await second, false);
});

test('a request cancelled while it waits for a place is never sent, and the next in line goes', { timeout: 5000 }, async () => {
  const { Pacer } = core();
  const clock = handClock();
  const { pacer, sent } = handPacer(Pacer, clock, { limit: 1 });
  const cancel = new AbortController();
  const first = pacer.request('/polygons?n=1', 'polygons');
  const cancelled = assert.rejects(pacer.request('/polygons?n=2', 'polygons', cancel.signal), { name: 'AbortError' });
  const next = pacer.request('/polygons?n=3', 'polygons');
  await settle();
  cancel.abort();
  await settle();
  sent[0].answer(200);
  await settle();
  assert.deepEqual(sent.map((s) => s.url), ['/polygons?n=1', '/polygons?n=3']);
  sent[1].answer(200);
  assert.deepEqual([(await first).status, (await next).status], [200, 200]);
  await cancelled;
  assert.deepEqual([pacer.active, pacer.waiting.length], [0, 0]);
});

// The request gives up at once, while the clock stands within its pause.
test('a request cancelled during its pause is not asked again', { timeout: 5000 }, async () => {
  const { Pacer } = core();
  const clock = handClock();
  const { pacer, sent } = handPacer(Pacer, clock);
  const cancel = new AbortController();
  const cancelled = assert.rejects(pacer.request('/polygons?n=1', 'polygons', cancel.signal), { name: 'AbortError' });
  await settle();
  sent[0].answer(429);
  await settle();
  assert.deepEqual(clock.lengths(), [100]);
  cancel.abort();
  await cancelled;
  assert.equal(clock.t, 0);
  clock.wake();
  await settle();
  assert.deepEqual(went(sent), [['/polygons?n=1', 0]]);
  assert.equal(pacer.active, 0);
});

test('a request cancelled while its body comes stops reading it and frees its place', { timeout: 5000 }, async () => {
  const { Pacer } = core();
  const given = [];
  const pacer = new Pacer({
    limit: 1,
    fetch: async (url, signal) => {
      given.push(signal);
      return url === 'slow' ? { status: 200, headers: { get: () => null }, text: () => new Promise(() => {}) } : reply(200, 'ok');
    },
  });
  const cancel = new AbortController();
  const cancelled = assert.rejects(pacer.request('slow', 'polygons', cancel.signal), { name: 'AbortError' });
  await settle();
  assert.equal(given[0], cancel.signal, 'the fetch has the signal, which ends its download');
  cancel.abort();
  await cancelled;
  assert.equal(pacer.active, 0);
  assert.equal((await pacer.request('next', 'polygons')).body, 'ok');
});

test('the JSON of an answer is shown highlighted and escaped', () => {
  const { syntaxHighlight } = core();
  const out = syntaxHighlight({ name: '<img src=x>', n: 1, ok: true, none: null });
  assert.ok(!out.includes('<img'));
  assert.ok(out.includes('<span class="json-string">"&lt;img src=x&gt;"</span>'), out);
  assert.ok(out.includes('<span class="json-key">"name":</span>'));
  assert.ok(out.includes('<span class="json-number">1</span>'));
});

// -- Place search --

// A result of /search?debug=1: the binding's fields, then how it was found.
// A place of Paris unless `extra` says otherwise.
function hit(name, latitude, longitude, extra = {}) {
  return {
    admin1name: 'Île-de-France', admin2name: 'Paris', country: 'France', country_code: 'FR', latitude, longitude, name, type: 'city',
    class: 0, context: false, entity: 1, kind: 'P', score: 0.5, text: 1, ...extra,
  };
}

// A /search?debug=1 body of `results`.
const debugBody = (results, us = 1104.5) => JSON.stringify({ context_checks: 1, keys: 2, postings: 3, q: 'paris', results, reverse_lookups: 4, us });

// Each column's view of the search: its label, colour, dataset and answer.
function searchView(label, color, answer, dataset = null, error = null) {
  return { label, color, dataset, error, answer };
}

// The rows of a matrix's HTML: each one's classes, its rank cells (class
// and text) and the rest.
function matrixRows(out) {
  return [...out.matchAll(/<tr class="([^"]*)" data-row="(\d+)">([\s\S]*?)<\/tr>/g)].map((m) => ({
    classes: m[1], n: Number(m[2]),
    ranks: [...m[3].matchAll(/<td class="(rank[^"]*)"[^>]*>([^<]*)<\/td>/g)].map((r) => [r[1], r[2]]),
    rest: m[3],
  }));
}

// Timers that fire only when the test moves the clock: set and clear as
// setTimeout and clearTimeout, advance(ms) firing those due by then in
// order, and the ms each one pending is still to wait.
function handTimers() {
  const timers = {
    t: 0,
    ids: 0,
    due: new Map(),
    set: (fn, ms) => {
      timers.due.set(++timers.ids, { fn, at: timers.t + ms });
      return timers.ids;
    },
    clear: (id) => { timers.due.delete(id); },
    advance(ms) {
      const end = timers.t + ms;
      for (;;) {
        const next = [...timers.due].filter(([, d]) => d.at <= end).sort((a, b) => a[1].at - b[1].at || a[0] - b[0])[0];
        if (!next) break;
        timers.due.delete(next[0]);
        timers.t = next[1].at;
        next[1].fn();
      }
      timers.t = end;
    },
    pending: () => [...timers.due.values()].map((d) => d.at - timers.t),
  };
  return timers;
}

// A PlaceSearch of the page's on `timers`, of the datasets `datasets()`,
// whose requests the test answers: each in `sent` once it goes, with its URL,
// q and dataset, its signal and answer(status, body, headers).
function handSearch(PlaceSearch, timers, datasets) {
  const sent = [];
  let changes = 0;
  const search = new PlaceSearch({
    setTimer: timers.set,
    clearTimer: timers.clear,
    datasets,
    changed: () => { changes++; },
    request: (url, signal) => new Promise((resolve, reject) => {
      const params = new URL(url, 'http://portal').searchParams;
      sent.push({
        url, q: params.get('q'), dataset: params.get('dataset'), signal,
        answer: (status, body, headers = {}) => resolve({ status, header: (name) => headers[name.toLowerCase()] ?? null, body, ms: 7.5 }),
      });
      signal.addEventListener('abort', () => reject(signal.reason), { once: true });
    }),
  });
  return { search, sent, changes: () => changes };
}

test('the search is offered where /datasets names each dataset\'s index', () => {
  const { searchOffered } = core();
  const searching = catalog();
  for (const d of searching.datasets) d.search = { index: d.id, shipped: false };
  assert.equal(searchOffered('catalog', searching), true);
  assert.equal(searchOffered('catalog', catalog()), false, 'a portal without /search');
  assert.equal(searchOffered('single', null), false, 'a production server');
  assert.equal(searchOffered('waiting', null), false);
});

test('a search asks /search for 10 results of a dataset, with how each was found', () => {
  const { searchUrl } = core();
  const url = searchUrl('paris, texas & #5', 'full.simplified.all');
  assert.equal(url, '/search?q=paris%2C%20texas%20%26%20%235&dataset=full.simplified.all&limit=10&key=test&debug=1');
  assert.equal(new URL(url, 'http://portal').searchParams.get('q'), 'paris, texas & #5');
  assert.equal(searchUrl('x', 'a"b'), '/search?q=x&dataset=a%22b&limit=10&key=test&debug=1');
});

test('a search answer keeps its results and timings, or the state of the index, or why it has none', () => {
  const { searchAnswerOf } = core();
  const headers = { 'x-geocoder-build-id': ID, 'x-geocoder-dataset': 'full.simplified.all', 'x-geocoder-search-index': 'full.simplified.all', 'retry-after': '5' };
  const header = (name) => headers[name.toLowerCase()] ?? null;
  const paris = hit('Paris', 48.8566, 2.3522);
  assert.deepEqual(plain(searchAnswerOf(200, header, debugBody([paris]), 12.4)),
    { results: [paris], us: 1104.5, index: 'full.simplified.all', build: ID, dataset: 'full.simplified.all', ms: 12.4 });
  const building = searchAnswerOf(503, header, '{"dataset":"smallest","index":"admin-minimal.simplified.none","since":"2026-11-21T18:57:07Z","status":"building"}', 1);
  assert.deepEqual(plain(building), { building: { index: 'admin-minimal.simplified.none', since: '2026-11-21T18:57:07Z' }, retry: 5, dataset: 'smallest', ms: 1 });
  assert.equal(searchAnswerOf(503, () => null, '{"dataset":"x","index":"x","since":"t","status":"building"}', 1).retry, 5, 'Retry-After or 5 s');
  const failed = searchAnswerOf(503, () => null, '{"dataset":"x","index":"full.exact.all","reason":"dataset x: no room","since":"2026-11-21T19:00:00Z","status":"failed"}', 1);
  assert.deepEqual(plain(failed), { failed: { index: 'full.exact.all', since: '2026-11-21T19:00:00Z', reason: 'dataset x: no room' }, dataset: 'x', ms: 1 });
  assert.equal(searchAnswerOf(503, () => null, 'no build to serve: waiting for a build of version 24', 1).error,
    'HTTP 503: no build to serve: waiting for a build of version 24');
  assert.equal(searchAnswerOf(400, () => null, 'q must be at most 256 bytes', 1).error, 'HTTP 400: q must be at most 256 bytes');
  assert.equal(searchAnswerOf(429, () => null, 'Rate limit exceeded', 1).error, 'HTTP 429: Rate limit exceeded');
  assert.equal(searchAnswerOf(200, () => null, 'not json', 1).error, 'not a search answer: not json');
  assert.equal(searchAnswerOf(200, () => null, '[]', 1).error, 'not a search answer: []');
});

// One degree of latitude is 111,195 m: 0.004 degrees 445 m, 0.01 degrees
// 1,112 m.
test('results of the columns are one row where their names are equal and their points within a kilometre', () => {
  const { searchRows } = core();
  const paris = hit('Paris', 48.8566, 2.3522);
  const texas = hit('Paris', 33.6609, -95.5555, { admin1name: 'Texas', admin2name: 'Lamar County', country: 'United States', country_code: 'US' });
  const rows = searchRows([
    [paris, texas],
    [texas, hit('Paris', 48.8606, 2.3522), hit('Parys', 48.8566, 2.3522)],
    [hit('Paris', 48.8666, 2.3522)],
    null,
  ]);
  const shape = (row) => [row.name, row.lat, row.lng, row.cells.map((c) => (c ? c.rank : null)), row.best, row.missing, row.ranksDiffer, Math.round(row.moved)];
  assert.deepEqual(plain(rows.map(shape)), [
    ['Paris', 48.8566, 2.3522, [1, 2, null, null], 1, true, true, 445],
    ['Paris', 33.6609, -95.5555, [2, 1, null, null], 1, true, true, 0],
    ['Paris', 48.8666, 2.3522, [null, null, 1, null], 1, true, false, 0],
    ['Parys', 48.8566, 2.3522, [null, 3, null, null], 3, true, false, 0],
  ]);
  assert.equal(Math.round(rows[0].cells[1].metres), 445);
  // A column with no answer (null) lacks nothing.
  const one = searchRows([[paris], null, [paris]]);
  assert.deepEqual(plain(one.map(shape)), [['Paris', 48.8566, 2.3522, [1, null, 1], 1, false, false, 0]]);
  assert.deepEqual(plain(searchRows([null, null])), []);
  assert.deepEqual(plain(searchRows([[], []])), []);
});

// A chain's shops of one name, 334 m apart: each pairs with the nearest
// of the other column's, whatever their ranks, and a column's own two
// results never share a row.
test('results of one name near each other pair with the nearest, each once', () => {
  const { searchRows } = core();
  const shop = (lat) => hit('Starbucks', lat, -73.99, { kind: 'O', type: 'cafe' });
  const rows = searchRows([[shop(40.75), shop(40.753)], [shop(40.7531), shop(40.75)]]);
  assert.deepEqual(plain(rows.map((r) => [r.lat, r.cells.map((c) => c.rank), Math.round(r.moved)])), [[40.75, [1, 2], 0], [40.753, [2, 1], 11]]);
  const twice = searchRows([[shop(40.75), shop(40.75)], [shop(40.75)]]);
  assert.deepEqual(plain(twice.map((r) => r.cells.map((c) => (c ? c.rank : null)))), [[1, 1], [2, null]]);
});

// A boundary without a label node is put at a point on its ring's surface,
// which moves with the boundary's precision: it matches within 20 km.
test('a boundary without a label matches within 20 km, a place only within 1 km', () => {
  const { searchRows } = core();
  const area = (lat) => hit('Bayern', lat, 11.4, { kind: 'A', type: 'administrative' });
  const rows = searchRows([[area(48.9)], [area(49.0)]]);
  assert.equal(rows.length, 1);
  assert.equal(Math.round(rows[0].moved), 11120);
  assert.equal(searchRows([[area(48.9)], [area(49.07)]]).length, 1);
  assert.equal(searchRows([[area(48.9)], [area(49.1)]]).length, 2);
  assert.equal(searchRows([[area(48.9)], [area(49.2)]]).length, 2);
  const place = (lat) => hit('Bayern', lat, 11.4);
  assert.equal(searchRows([[place(48.9)], [place(49.0)]]).length, 2);
  // A boundary labelled in one dataset and not in another.
  assert.equal(searchRows([[place(48.9)], [area(49.0)]]).length, 1);
});

// A dataset without POIs gives the town of a name where another gives a
// memorial of that name 311 m from it: they are two rows.
test('a POI is one row with a POI only, however near a place of its name', () => {
  const { searchRows } = core();
  const memorial = hit('Paris', 33.6609, -95.5555, { kind: 'O', type: 'memorial' });
  const town = hit('Paris', 33.6637, -95.5555, { type: 'town' });
  const ranks = (rows) => plain(rows.map((r) => [r.kind, r.cells.map((c) => (c ? c.rank : null))]));
  assert.deepEqual(ranks(searchRows([[memorial], [town]])), [['O', [1, null]], ['P', [null, 1]]]);
  assert.deepEqual(ranks(searchRows([[town], [memorial]])), [['P', [1, null]], ['O', [null, 1]]]);
  assert.deepEqual(ranks(searchRows([[memorial, town], [town]])), [['O', [1, null]], ['P', [2, 1]]]);
  assert.deepEqual(ranks(searchRows([[memorial], [hit('Paris', 33.6637, -95.5555, { kind: 'O', type: 'museum' })]])), [['O', [1, 1]]]);
});

test('a row tells the context its columns give, and the parts they disagree on', () => {
  const { searchRows } = core();
  const [row] = searchRows([
    [hit('Paris', 48.8566, 2.3522)],
    [hit('Paris', 48.8566, 2.3522, { admin1name: 'Ile-de-France', type: 'town' })],
    null,
    [hit('Paris', 48.8566, 2.3522, { admin2name: '' })],
  ]);
  assert.deepEqual(plain(row.context), [
    { part: 'admin2name', value: 'Paris', values: ['Paris', 'Paris', null, ''], differs: true },
    { part: 'admin1name', value: 'Île-de-France', values: ['Île-de-France', 'Ile-de-France', null, 'Île-de-France'], differs: true },
    { part: 'country', value: 'France', values: ['France', 'France', null, 'France'], differs: false },
  ]);
  assert.equal(row.typeDiffers, true);
  const [same] = searchRows([[hit('Paris', 48.8566, 2.3522)], [hit('Paris', 48.8566, 2.3522)]]);
  assert.ok(same.context.every((p) => !p.differs));
  assert.equal(same.typeDiffers, false);
});

// Rows go by their best rank in any column; of rows of one best rank,
// those of the columns before first, each column's in its order.
test('the rows go by their best rank', () => {
  const { searchRows } = core();
  const at = (name, lat) => hit(name, lat, 0);
  const [a, b, c, d] = [at('A', 1), at('B', 2), at('C', 3), at('D', 4)];
  const rows = searchRows([[a, b, c], [d, a, b]]);
  assert.deepEqual(plain(rows.map((r) => [r.name, r.best, r.cells.map((x) => (x ? x.rank : null))])),
    [['A', 1, [1, 2]], ['D', 1, [null, 1]], ['B', 2, [2, 3]], ['C', 3, [3, null]]]);
});

test('the matrix gives each row its rank in each column, marking what a column lacks, what moved and the context they disagree on', () => {
  const { searchRows, searchMatrixHtml } = core();
  const paris = hit('Paris', 48.8566, 2.3522, { score: 0.21, text: 1, context: true });
  const texas = hit('Paris', 33.6609, -95.5555, { admin1name: 'Texas', admin2name: 'Lamar County', country: 'United States', type: 'town' });
  const lists = [[paris, texas], [texas, hit('Paris', 48.8606, 2.3522, { admin1name: 'Ile-de-France' })], null];
  const views = [
    searchView('With addresses', '#6c8cff', { results: lists[0] }),
    searchView('Smallest', '#d9944a', { results: lists[1] }),
    searchView('Max quality', '#c77dff', { building: { since: '2026-11-21T18:57:07Z', index: 'full.exact.all' } }),
  ];
  const out = searchMatrixHtml(searchRows(lists), views, false);
  assert.ok(out.includes('<th class="rank" style="color:#6c8cff" title="With addresses">1</th>'), out);
  assert.ok(out.includes('<th class="rank" style="color:#c77dff" title="Max quality">3</th>'), out);
  const rows = matrixRows(out);
  assert.deepEqual(plain(rows.map((r) => [r.n, r.classes, r.ranks])), [
    [0, 'search-hit', [['rank', '1'], ['rank shifted', '2'], ['rank unanswered', '·']]],
    [1, 'search-hit', [['rank shifted', '2'], ['rank', '1'], ['rank unanswered', '·']]],
  ]);
  assert.match(rows[0].rest, /<td class="rank" style="color:#6c8cff" title="With addresses: #1 · score 0\.210 · text 1\.000 · its context matched">1<\/td>/);
  assert.ok(rows[0].rest.includes('<span class="tag" title="The farthest of its points from the first column\'s">moved 445 m</span>'), rows[0].rest);
  assert.ok(!rows[1].rest.includes('moved'), rows[1].rest);
  assert.ok(rows[0].rest.includes('<span class="differs" title="With addresses: Île-de-France · Smallest: Ile-de-France">Île-de-France</span>'), rows[0].rest);
  assert.ok(rows[0].rest.includes('<div class="hit-context">Paris · <span class="differs"'), rows[0].rest);
  assert.ok(rows[0].rest.includes('· France</div>'), rows[0].rest);
  assert.ok(rows[1].rest.includes('<div class="hit-context">Lamar County · Texas · United States</div>'), rows[1].rest);
  assert.ok(rows[1].rest.includes('<span class="hit-kind" title="a place node of a settlement">place</span><span class="hit-type">town</span>'), rows[1].rest);

  // A row a column lacks among its answers.
  const lacking = matrixRows(searchMatrixHtml(searchRows([[paris], []]), views.slice(0, 2), false));
  assert.deepEqual(plain(lacking.map((r) => [r.classes, r.ranks])), [['search-hit missing', [['rank', '1'], ['rank absent', '–']]]]);
  assert.match(lacking[0].rest, /<td class="rank absent" title="Smallest: not among its results">–<\/td>/);
});

test('the matrix shows 5 rows, and every row once asked', () => {
  const { searchRows, searchMatrixHtml } = core();
  const list = Array.from({ length: 7 }, (_, i) => hit(`P${i}`, i, 0));
  const views = [searchView('With addresses', '#6c8cff', { results: list })];
  const rows = searchRows([list]);
  const few = searchMatrixHtml(rows, views, false);
  assert.deepEqual(matrixRows(few).map((r) => r.n), [0, 1, 2, 3, 4]);
  assert.ok(few.includes('<button class="tool search-more" data-action="more">Show 2 more</button>'), few);
  const all = searchMatrixHtml(rows, views, true);
  assert.deepEqual(matrixRows(all).map((r) => r.n), [0, 1, 2, 3, 4, 5, 6]);
  assert.ok(all.includes('<button class="tool search-more" data-action="more">Show the first 5</button>'), all);
  assert.ok(!searchMatrixHtml(rows.slice(0, 5), views, false).includes('search-more'));
  assert.equal(searchMatrixHtml([], [searchView('With addresses', '#6c8cff', { results: [] })], false), '<div class="search-none">No results</div>');
  assert.equal(searchMatrixHtml([], [searchView('With addresses', '#6c8cff', { loading: true })], false), '');
});

test('each column\'s head tells its search: its time and index, or its index building or failed, or why it has no answer', () => {
  const { searchHeadsHtml } = core();
  const health = { search: { ready: 6, datasets: 28, groups: [
    { index: 'full.simplified.all', state: 'ready' }, { index: 'full.exact.all', state: 'building', since: '2026-11-21T18:57:07Z' },
    { index: 'admin-minimal.simplified.none', state: 'queued', since: '2026-11-21T18:57:00Z' }, { index: 'full.exact.none', state: 'failed' },
  ] } };
  const results = { results: [hit('Paris', 1, 2), hit('Paris', 3, 4)], us: 1104.5, ms: 12.4, index: 'full.simplified.all' };
  const out = searchHeadsHtml([
    searchView('With addresses', '#6c8cff', results),
    searchView('No addresses', '#d9944a', { ...results, results: [hit('Paris', 1, 2)] }),
    searchView('Max quality', '#c77dff', { building: { since: '2026-11-21T18:57:07Z', index: 'full.exact.all' } }),
    searchView('Smallest', '#4ac9d9', { building: { since: '2026-11-21T18:57:00Z', index: 'admin-minimal.simplified.none' } }),
  ], health);
  const heads = [...out.matchAll(/<div class="search-head" style="border-left-color:([^"]+)">([\s\S]*?)<\/div>/g)].map((m) => [m[1], m[2]]);
  assert.equal(heads.length, 4, out);
  assert.equal(heads[0][1], '<span style="color:#6c8cff">1</span> With addresses: 2 results · 12 ms, 1.10 ms in the search · index full.simplified.all');
  assert.equal(heads[1][1], '<span style="color:#d9944a">2</span> No addresses: 1 result · 12 ms, 1.10 ms in the search · index full.simplified.all, as 1\'s');
  assert.equal(heads[2][1], '<span style="color:#c77dff">3</span> Max quality: <span class="loading-spinner"></span>index full.exact.all building since 2026-11-21 18:57:07 UTC · 1 of 4 indexes ready');
  assert.equal(heads[3][1], '<span style="color:#4ac9d9">4</span> Smallest: <span class="loading-spinner"></span>index admin-minimal.simplified.none queued since 2026-11-21 18:57:00 UTC · 1 of 4 indexes ready');

  const others = searchHeadsHtml([
    searchView('Max quality', '#6c8cff', { failed: { since: '2026-11-21T19:00:00Z', index: 'full.exact.all', reason: 'dataset full.exact.all: no room' } }),
    searchView('Smallest', '#d9944a', { error: 'HTTP 429: Rate limit exceeded' }),
    searchView('Mine', '#c77dff', { loading: true }),
    searchView('everything', '#4ac9d9', null, null, 'unknown dataset "everything": pick one'),
  ], null);
  assert.ok(others.includes('Max quality: <span class="warn">search unavailable: dataset full.exact.all: no room</span> · index full.exact.all, failed 2026-11-21 19:00:00 UTC; the next sync round tries it again'), others);
  assert.ok(others.includes('Smallest: <span class="warn">HTTP 429: Rate limit exceeded</span>'), others);
  assert.ok(others.includes('Mine: <span class="loading-spinner"></span>searching…'), others);
  assert.ok(others.includes('everything: <span class="warn">unknown dataset &quot;everything&quot;: pick one</span>'), others);
  // Without /health's groups, a building index says so without its progress.
  assert.ok(searchHeadsHtml([searchView('Max quality', '#6c8cff', { building: { since: '2026-11-21T18:57:07Z', index: 'full.exact.all' } })], null)
    .endsWith('index full.exact.all building since 2026-11-21 18:57:07 UTC</div></div>'));
});

test('every string of a search answer and of its index is escaped where the page shows it', () => {
  const { searchRows, searchMatrixHtml, searchHeadsHtml, searchHtml } = core();
  const evil = hit('<img src=x onerror=alert(1)>', 1, 2, { type: '<b>t</b>', kind: '<i>k</i>', admin1name: '<em>a1</em>', admin2name: '" onmouseover="alert(3)', country: '<script>c</script>' });
  const other = { ...evil, admin1name: '<u>other</u>', type: '<b>u</b>' };
  const views = [
    searchView('<b>label</b>', '#111', { results: [evil], us: 1, ms: 1, index: '<script>i</script>' }),
    searchView('<i>two</i>', '#222', { results: [other], us: 1, ms: 1, index: '<script>i</script>' }),
    searchView('<u>three</u>', '#333', { failed: { since: '<b>', index: '<i>', reason: '<img src=r>' } }),
    searchView('<em>four</em>', '#444', { building: { since: '<b>', index: '<script>' } }),
  ];
  const health = { search: { groups: [{ index: '<script>', state: '<b>queued</b>' }] } };
  const rows = searchRows(views.map((v) => (v.answer.results || null)));
  for (const out of [searchMatrixHtml(rows, views, true), searchHeadsHtml(views, health), searchHtml(views, health, rows, true)]) {
    assertEscaped(out);
    assert.ok(!out.includes('<script'), out);
  }
  assert.ok(searchMatrixHtml(rows, views, true).includes('&lt;img src=x onerror=alert(1)&gt;'));
});

test('the search shows the heads, then the matrix', () => {
  const { searchRows, searchHtml } = core();
  const views = [searchView('With addresses', '#6c8cff', { results: [hit('Paris', 1, 2)], us: 1, ms: 1, index: 'full.simplified.all' })];
  const rows = searchRows([views[0].answer.results]);
  const out = searchHtml(views, null, rows, false);
  assert.match(out, /^<div class="search-heads">[\s\S]*<\/div><table class="search-matrix">/);
});

// One dot a row, of a slice a column: the column's colour where it has the
// result, grey where it lacks it or has no answer.
test('a row\'s dot on the map is coloured by the columns that have it', () => {
  const { searchRows, dotBackground, dotTitle } = core();
  const colors = ['#6c8cff', '#d9944a', '#c77dff'];
  const paris = hit('Paris', 48.8566, 2.3522);
  const [row] = searchRows([[paris], [], [paris]]);
  assert.equal(dotBackground(row, colors), 'conic-gradient(#6c8cff 0% 33.33%, #555566 33.33% 66.67%, #c77dff 66.67% 100%)');
  assert.equal(dotBackground(searchRows([[paris]])[0], colors), '#6c8cff');
  assert.equal(dotBackground(searchRows([[paris], [paris]])[0], colors), 'conic-gradient(#6c8cff 0% 50%, #d9944a 50% 100%)');
  assert.equal(dotTitle(row, ['With addresses', 'Smallest', 'Max quality']), 'Paris (city): #1 in With addresses, #1 in Max quality');
});

test('a result picked from the matrix is looked up as a search of its best column', () => {
  const { searchRows, searchSource } = core();
  const rows = searchRows([[hit('Louvre', 1, 1), hit('Eiffel Tower', 48.8584, 2.2945)], [hit('Eiffel Tower', 48.8584, 2.2945)]]);
  const eiffel = rows.find((r) => r.name === 'Eiffel Tower');
  assert.equal(searchSource('eiffel', eiffel, ['With addresses', 'Smallest']), 'search "eiffel": Eiffel Tower, #1 in Smallest');
});

test('a search waits for 250 ms of quiet in the box, and Enter searches at once', () => {
  const { PlaceSearch } = core();
  const timers = handTimers();
  const { search, sent } = handSearch(PlaceSearch, timers, () => ['full.simplified.all', 'admin-minimal.simplified.none']);
  search.typed('p');
  timers.advance(100);
  search.typed('pa');
  timers.advance(200);
  search.typed('par');
  timers.advance(249);
  assert.deepEqual(sent, []);
  assert.equal(search.q, '');
  timers.advance(1);
  assert.deepEqual(sent.map((s) => [s.q, s.dataset]), [['par', 'full.simplified.all'], ['par', 'admin-minimal.simplified.none']]);
  assert.equal(search.q, 'par');
  assert.equal(sent[0].url, '/search?q=par&dataset=full.simplified.all&limit=10&key=test&debug=1');

  // Text typed back to the search shown asks nothing again; Enter asks it
  // again at once, and the pause typed before it is dropped.
  search.typed('pari');
  search.typed('par');
  timers.advance(250);
  assert.equal(sent.length, 2);
  search.typed('paris');
  search.submit('paris');
  assert.deepEqual(sent.slice(2).map((s) => s.q), ['paris', 'paris']);
  timers.advance(1000);
  assert.equal(sent.length, 4);
  assert.deepEqual(timers.pending(), []);
  search.submit('paris');
  assert.equal(sent.length, 6);
});

test('a newer search cancels the requests of the one before, whose answers are dropped', async () => {
  const { PlaceSearch } = core();
  const timers = handTimers();
  const { search, sent } = handSearch(PlaceSearch, timers, () => ['a', 'b']);
  search.submit('pa');
  search.submit('par');
  assert.deepEqual(sent.map((s) => [s.q, s.signal.aborted]), [['pa', true], ['pa', true], ['par', false], ['par', false]]);
  sent[0].answer(200, debugBody([hit('Pa', 1, 1)]));
  sent[2].answer(200, debugBody([hit('Paris', 1, 1)]));
  await settle();
  assert.deepEqual(Object.keys(search.answers).sort(), ['a', 'b']);
  assert.equal(search.answers.a.results[0].name, 'Paris');
  assert.deepEqual(plain(search.answers.b), { loading: true });
  sent[3].answer(200, debugBody([]));
  await settle();
  assert.deepEqual(plain(search.answers.b.results), []);
});

test('an empty box ends the search at once', async () => {
  const { PlaceSearch } = core();
  const timers = handTimers();
  const { search, sent } = handSearch(PlaceSearch, timers, () => ['a']);
  search.submit('paris');
  search.typed('pari');
  search.typed('   ');
  assert.equal(sent[0].signal.aborted, true);
  assert.equal(search.q, '');
  assert.deepEqual(plain(search.answers), {});
  assert.deepEqual(timers.pending(), []);
  sent[0].answer(200, debugBody([hit('Paris', 1, 1)]));
  await settle();
  assert.deepEqual(plain(search.answers), {});
  search.typed('');
  search.submit('');
  assert.equal(sent.length, 1);
});

// Columns of one dataset ask it once; a column added (or another dataset
// picked) asks its dataset, and a reload of the datasets asks them all
// again.
test('a search asks each dataset of the columns once, a dataset picked since, and all again for a new build', async () => {
  const { PlaceSearch } = core();
  const timers = handTimers();
  let datasets = ['a', 'a', 'b'];
  const { search, sent } = handSearch(PlaceSearch, timers, () => datasets);
  search.ask();
  assert.equal(sent.length, 0, 'no search shown');
  search.submit('paris');
  assert.deepEqual(sent.map((s) => s.dataset), ['a', 'b']);
  for (const s of sent) s.answer(200, debugBody([]));
  await settle();
  datasets = ['a', 'c'];
  search.ask();
  assert.deepEqual(sent.slice(2).map((s) => [s.q, s.dataset]), [['paris', 'c']]);
  search.again();
  assert.deepEqual(sent.slice(3).map((s) => [s.q, s.dataset]), [['paris', 'a'], ['paris', 'c']]);
});

test('a column whose index is building is asked again after Retry-After, showing it building meanwhile; a failed one is not', async () => {
  const { PlaceSearch } = core();
  const timers = handTimers();
  const { search, sent } = handSearch(PlaceSearch, timers, () => ['a', 'b']);
  search.submit('paris');
  sent[0].answer(503, '{"dataset":"a","index":"full.exact.all","since":"2026-11-21T18:57:07Z","status":"building"}', { 'retry-after': '5' });
  sent[1].answer(503, '{"dataset":"b","index":"full.exact.none","reason":"no room","since":"2026-11-21T19:00:00Z","status":"failed"}');
  await settle();
  assert.deepEqual(plain(search.answers.a.building), { index: 'full.exact.all', since: '2026-11-21T18:57:07Z' });
  assert.equal(search.answers.b.failed.reason, 'no room');
  assert.deepEqual(timers.pending(), [5000]);
  timers.advance(5000);
  assert.deepEqual(sent.slice(2).map((s) => [s.q, s.dataset]), [['paris', 'a']]);
  assert.ok(search.answers.a.building, 'still shown building while it is asked again');
  sent[2].answer(200, debugBody([hit('Paris', 1, 1)]));
  await settle();
  assert.equal(search.answers.a.results.length, 1);
  assert.deepEqual(timers.pending(), []);

  // A newer search drops the retry of the one before.
  search.submit('lyon');
  sent[3].answer(503, '{"dataset":"a","index":"full.exact.all","since":"2026-11-21T18:57:07Z","status":"building"}', { 'retry-after': '5' });
  await settle();
  assert.deepEqual(timers.pending(), [5000]);
  search.submit('nice');
  assert.deepEqual(timers.pending(), []);
});

test('a search request that fails says why, in its column', async () => {
  const { PlaceSearch } = core();
  const timers = handTimers();
  const search = new PlaceSearch({ setTimer: timers.set, clearTimer: timers.clear, datasets: () => ['a'], request: async () => { throw new Error('offline'); } });
  search.submit('paris');
  await settle();
  assert.deepEqual(plain(search.answers.a), { error: 'offline' });
});

test('a search\'s 429 pause leaves the reverse lookups going', async () => {
  const { Pacer } = core();
  const clock = handClock();
  const { pacer, sent } = handPacer(Pacer, clock);
  const searching = pacer.request('/search?q=a', 'search');
  await settle();
  sent[0].answer(429);
  await settle();
  assert.deepEqual(clock.lengths(), [100]);
  const lookup = pacer.request('/reverse?n=1', 'reverse');
  await settle();
  assert.deepEqual(went(sent), [['/search?q=a', 0], ['/reverse?n=1', 0]]);
  sent[1].answer(200);
  assert.equal((await lookup).status, 200);
  assert.equal(pacer.scope('reverse').pause, 0);
  clock.wake();
  await settle();
  sent[2].answer(200);
  assert.equal((await searching).status, 200);
});

// The side panel stacks the datasets, the search, the coordinates, the photo
// upload, the results and, once one is selected, its detail. Four columns of
// datasets, every row of a search and the detail of a result fill a window
// of 1000 px without the results, whose toolbar and cards would go: they
// keep a height of their own, the datasets, the search and the detail scroll
// within what is left, down to a least height of theirs, and a window too
// short for those scrolls the panel rather than cutting off its foot. The
// results' section hides what overflows it: its list of cards scrolls.
test('the results keep room for their toolbar and cards however tall the datasets, the search and the detail grow', () => {
  const height = /\b[1-9]\d*(px|%|vh)/;
  assert.match(style('.results-section')['min-height'] ?? 'none', height);
  for (const giving of ['.datasets-section', '.search-section', '.detail-panel']) {
    assert.equal(style(giving)['overflow-y'], 'auto', giving);
    assert.match(style(giving)['min-height'] ?? 'none', height, giving);
  }
  assert.equal(style('.side-panel')['overflow-y'], 'auto');
  assert.equal(style('.results-section').overflow, 'hidden');
  assert.equal(style('.results-list')['overflow-y'], 'auto');
});

// Four columns and Nominatim share the detail's width: a column's places
// table breaks a long word (place_of_worship) within its own column rather
// than running into the next one.
test("each column's places table wraps its words within the column", () => {
  for (const cell of ['.detail-places th', '.detail-places td']) {
    assert.equal(style(cell)['overflow-wrap'], 'anywhere', cell);
  }
});

// However narrow the detail (a phone's width, or a page zoomed in), each
// column's places table keeps a width it reads at, under its column's
// label: the places scroll sideways in a box of their own rather than one
// table running into the next, and the rest of the detail stays put.
test("the places tables keep a readable width, scrolling within a box of their own", () => {
  const { detailHtml } = core();
  const places = [{ name: 'Basilique du Sacré-Cœur', category: 'place_of_worship', distance_m: 120 }];
  const views = [1, 2, 3, 4, 5].map((n) => ({ label: `column ${n}`, color: '#111', dataset: null, answer: { data: { display_name: 'Paris', places } } }));
  const out = detailHtml(views, null);
  const box = out.match(/<div class="places-scroll"><div class="places-grid" style="grid-template-columns:repeat\(5,minmax\((\d+)px,1fr\)\)">([\s\S]*?)<\/div><\/div><\/div>/);
  assert.ok(box, out);
  assert.ok(Number(box[1]) >= 160, box[1]);
  assert.deepEqual([...box[2].matchAll(/<div class="answer-label"[^>]*>([^<]*)<\/div><table>/g)].map((m) => m[1]), views.map((v) => v.label));
  assert.equal(style('.places-scroll')['overflow-x'], 'auto');
  assert.equal(style('.places-grid').display, 'grid');
});

test("the page's other scripts parse", () => {
  const others = scripts.filter((s) => s.id !== 'core');
  assert.ok(others.length > 0);
  for (const s of others) {
    assert.doesNotThrow(() => new vm.Script(s.body, { filename: 'index.html' }));
  }
});

test('the page asks its own server only: no :3001 and no minimal_url', () => {
  assert.ok(!html.includes(':3001'));
  assert.ok(!html.includes('minimal_url'));
});

// -- Feedback --

// The link's state as the page has it: its columns, its selected point and
// the search box's text.
const PAGE = { columns: ['with-addresses', 'smallest'], point: { lat: 48.8584, lng: 2.2945 }, q: 'eiffel' };

// What a report of nothing expected says: no field of it.
const NOTHING = { text: null, name: null, road: null, house_number: null, city: null, postcode: null, country: null, empty: false };

test("a report of a column's reverse answer names its dataset, build, point and answer, what was expected, and the link", () => {
  const { reverseTarget, feedbackReport, feedbackSays } = core();
  const dataset = catalog().datasets.find((d) => d.id === 'full.simplified.all');
  const data = { display_name: 'Avenue Anatole France, Paris', address: { road: 'Avenue Anatole France', city: 'Paris' } };
  const view = { label: dataset.label, color: '#111', dataset, answer: { data, build: ID, dataset: 'full.simplified.all', ms: 3 } };
  const form = { text: ' the tower itself ', name: 'Tour Eiffel', road: '', house_number: ' 5 ', city: '', postcode: '', country: '', comment: 'right there' };

  const report = plain(feedbackReport(reverseTarget(view, { id: 'r1', lat: 48.8584, lng: 2.2945 }), form, PAGE));

  assert.deepEqual(report, {
    kind: 'reverse', lat: 48.8584, lon: 2.2945, q: null, result_index: null, dataset: 'full.simplified.all', build: ID,
    answer: data, answer_trimmed: false,
    expected: { ...NOTHING, text: 'the tower itself', name: 'Tour Eiffel', house_number: '5' },
    comment: 'right there',
    page: { columns: ['with-addresses', 'smallest'], point: { lat: 48.8584, lon: 2.2945 }, q: 'eiffel' },
  });
  assert.equal(feedbackSays(report), true);
});

// A name or contact given goes with the report, trimmed; none is left out.
// The form shows the one this browser remembers, escaped.
test("a report carries the contact its sender gave, and the form shows the remembered one", () => {
  const { generalTarget, feedbackReport, feedbackFormHtml } = core();
  const bare = { columns: [], point: null, q: '' };
  const target = generalTarget(null, ID);

  assert.equal(plain(feedbackReport(target, { comment: 'hi', contact: '  ana@example.org ' }, bare)).contact, 'ana@example.org');
  assert.equal('contact' in plain(feedbackReport(target, { comment: 'hi', contact: '  ' }, bare)), false);
  const html = feedbackFormHtml(target, 'Ana <a@b>');
  assert.match(html, /name="contact"[^>]*value="Ana &lt;a@b&gt;"/);
});

// A search's report carries the column's results; the form names one of
// them (by its index, its point the report's) or none, for the results as a
// whole.
test("a report of a column's search names the query, and the result picked with its point", () => {
  const { searchTarget, feedbackReport } = core();
  const results = [hit('Paris', 48.8566, 2.3522), hit('Paris', 33.6609, -95.5555, { admin1name: 'Texas' })];
  const answer = { results, build: ID, dataset: 'full.simplified.all', index: 'full.simplified.all', ms: 7 };
  const target = searchTarget(searchView('With addresses', '#111', answer, { id: 'full.simplified.all' }), 'paris');
  const page = { columns: ['with-addresses'], point: null, q: 'paris' };

  const picked = plain(feedbackReport(target, { result: '1', text: 'Paris, Texas first' }, page));
  const whole = plain(feedbackReport(target, { result: '', empty: 'on' }, page));

  assert.deepEqual([picked.kind, picked.q, picked.result_index, picked.lat, picked.lon, picked.dataset, picked.build],
    ['search', 'paris', 1, 33.6609, -95.5555, 'full.simplified.all', ID]);
  assert.deepEqual(picked.answer, plain(results));
  assert.deepEqual(picked.page, { columns: ['with-addresses'], point: null, q: 'paris' });
  assert.deepEqual([whole.result_index, whole.lat, whole.lon, whole.expected], [null, null, null, { ...NOTHING, empty: true }]);
});

// A general report is of no answer: the build served, and the selected
// result's point where there is one. A report says what was expected (in
// words, a field, or that the answer should be empty) or comments, or the
// form doesn't send it.
test('a general report is of the page, at the selected point where there is one, and says something', () => {
  const { generalTarget, feedbackReport, feedbackSays } = core();
  const bare = { columns: [], point: null, q: '' };

  const none = plain(feedbackReport(generalTarget(null, ID), { comment: '  ' }, bare));
  const at = plain(feedbackReport(generalTarget({ lat: 1.5, lng: 2.5 }, ID), { comment: 'slow at night' }, bare));

  assert.deepEqual(none, {
    kind: 'general', lat: null, lon: null, q: null, result_index: null, dataset: null, build: ID, answer: null, answer_trimmed: false,
    expected: NOTHING, comment: null, page: { columns: [], point: null, q: null },
  });
  assert.equal(feedbackSays(none), false);
  assert.deepEqual([at.lat, at.lon, at.comment], [1.5, 2.5, 'slow at night']);
  for (const form of [{ comment: 'x' }, { text: 'x' }, { city: 'Paris' }, { empty: 'on' }]) {
    assert.equal(feedbackSays(feedbackReport(generalTarget(null, ID), form, bare)), true, JSON.stringify(form));
  }
});

// The portal takes a report of 16 KiB at most: the answer's JSON goes as it
// is up to 8 KiB, else the start of it as a string, of at most 8 KiB in the
// report's JSON, cut between characters.
test('an answer too long for a report goes as the start of its JSON, of at most 8 KiB, flagged', () => {
  const { reverseTarget, feedbackReport, FEEDBACK_ANSWER_MAX } = core();
  const places = Array.from({ length: 400 }, (_, i) => ({ name: `Café "☕" 東京 ${i}`, category: 'cafe', distance_m: i }));
  const data = { display_name: 'Tōkyō', places };
  const view = (d) => ({ label: 'x', color: '#111', dataset: { id: 'full.exact.all' }, answer: { data: d, build: ID, dataset: 'full.exact.all' } });
  const entry = { lat: 35.68, lng: 139.76 };

  const long = feedbackReport(reverseTarget(view(data), entry), { comment: 'x' }, PAGE);
  const short = feedbackReport(reverseTarget(view({ display_name: 'Tōkyō' }), entry), { comment: 'x' }, PAGE);

  assert.equal(FEEDBACK_ANSWER_MAX, 8192);
  assert.equal(long.answer_trimmed, true);
  assert.equal(typeof long.answer, 'string');
  assert.ok(JSON.stringify(data).startsWith(long.answer));
  const bytes = Buffer.byteLength(JSON.stringify(long.answer)) - 2;
  assert.ok(bytes <= 8192 && bytes > 8192 - 6, bytes);
  assert.ok(Buffer.byteLength(JSON.stringify(long)) < 16384);
  assert.deepEqual([plain(short.answer), short.answer_trimmed], [{ display_name: 'Tōkyō' }, false]);
});

// The form shows what the report is of, the answer it sends, a search's
// results to pick from, the fields of what was expected and what is kept,
// each string escaped. A column's answer of the portal (of a dataset, a
// reverse lookup's or a search's) has a "Disagree?" button; Nominatim's,
// and one not in, have none.
test('the form and its buttons escape every string they show, and only answers of a dataset have one', () => {
  const { feedbackFormHtml, reverseTarget, searchTarget, generalTarget, detailHtml, searchHeadsHtml } = core();
  const dataset = { id: '"><img src=x onerror=alert(1)>', label: '<b>label</b>', size_disk: 1, size_download: 1 };
  const view = { label: '<i>col</i>', color: '#111', dataset, answer: { data: { display_name: '<img src=y>', address: { road: '<script>r</script>' } }, build: '<u>build</u>' } };
  const results = [hit('<em>Paris</em>', 1, 2, { type: '<b>t</b>' })];

  const reverse = feedbackFormHtml(reverseTarget(view, { lat: 1, lng: 2 }));
  const search = feedbackFormHtml(searchTarget({ ...view, answer: { results, build: '<u>b</u>' } }, '<script>q</script>'));
  const general = feedbackFormHtml(generalTarget(null, '<u>b</u>'));

  for (const out of [reverse, search, general]) {
    assertEscaped(out);
    for (const name of ['text', 'name', 'road', 'house_number', 'city', 'postcode', 'country', 'empty', 'comment', 'contact']) assert.ok(out.includes(`name="${name}"`), name);
    assert.ok(out.includes('no IP address is kept'), out);
  }
  assert.ok(reverse.includes('&quot;&gt;&lt;img src=x onerror=alert(1)&gt; · build &lt;u&gt;build&lt;/u&gt; · 1.00000, 2.00000'), reverse);
  assert.ok(search.includes('<option value="0">#1 &lt;em&gt;Paris&lt;/em&gt; (&lt;b&gt;t&lt;/b&gt;)</option>'), search);
  assert.ok(!reverse.includes('name="result"') && !general.includes('<details'));

  const nominatim = { label: 'Nominatim', color: '#222', dataset: null, answer: { data: { display_name: 'N' } } };
  const detail = detailHtml([view, nominatim, { ...view, answer: { loading: true } }, { ...view, answer: { error: 'HTTP 503' } }], null);
  const heads = searchHeadsHtml([searchView('a', '#111', { results }, dataset), searchView('b', '#222', { results: [] }, dataset),
    searchView('c', '#333', { loading: true }, dataset), searchView('d', '#444', { results }, null)], null);
  assert.deepEqual([...detail.matchAll(/data-feedback="(\d+)"/g)].map((m) => m[1]), ['0']);
  assert.deepEqual([...heads.matchAll(/data-feedback-search="(\d+)"/g)].map((m) => m[1]), ['0', '1']);
  assertEscaped(detail);
  assertEscaped(heads);
});

test('the form thanks with the counts of the reports, or says why a send failed', () => {
  const { feedbackThanks, feedbackError } = core();
  assert.equal(feedbackThanks({ today: 3, total: 12 }), 'Thank you! That makes 3 reports today, 12 in all.');
  assert.equal(feedbackThanks({ today: 1, total: 1 }), 'Thank you! That makes 1 report today, 1 in all.');
  assert.equal(feedbackThanks(null), 'Thank you! We have your report.');
  assert.equal(feedbackError(413, 'a report is at most 16384 bytes'), 'Too long to send: shorten what you wrote.');
  assert.equal(feedbackError(429, 'at most 20 reports a minute'), 'Too many reports from here just now: try again in a minute.');
  assert.equal(feedbackError(400, 'a report says what was expected or comments'), 'Not sent: HTTP 400: a report says what was expected or comments');
});

// -- The Immich column --
// The column of what Immich stores today, as /datasets lists it among its
// others (server/src/portal/immich.rs), and its answers.
const IMMICH = {
  id: 'immich-cities500',
  kind: 'immich',
  label: 'Immich today (GeoNames cities500)',
  description: 'What Immich v3.4.2 stores for a photo here: the nearest GeoNames cities500 place in a box of 25 km about the point, else the Natural Earth country, else nothing.',
  immich: { release: 'v3.4.2', commit: 'b6b4b80df113656bfa4872cab31c7319e89438a9', geodata_date: '2026-10-29T14:18:17+00:00', max_distance_m: 25000 },
  // countryInfo.txt's names, a few of them: Immich's, not i18n-iso-countries'
  // ('United States', not 'United States of America').
  country_names: { FR: 'France', US: 'United States', PR: 'Puerto Rico', DE: 'Germany', TW: 'Taiwan', GB: 'United Kingdom' },
  search: null,
};

// /datasets of the template with the Immich column among its others.
function withImmich(c = catalog()) {
  return { ...c, others: [IMMICH] };
}

// An answer of the Immich column: what it stores and how it found it.
function immichData(how, stored, extra = {}) {
  const shown = [stored.city, stored.state, stored.country].filter(Boolean);
  const address = Object.fromEntries(Object.entries(stored).filter(([, v]) => v));
  return {
    lat: 48.8566, lon: 2.3522, display_name: shown.length ? shown.join(', ') : null, address,
    immich: { ...IMMICH.immich, stored, how, place: null, natural_earth: null, ...extra },
  };
}

const PARIS = {
  geonameid: 2988507, name: 'Paris', latitude: 48.85341, longitude: 2.3488, feature_code: 'PPLC', country_code: 'FR',
  admin1_code: '11', admin1: 'Île-de-France', admin2_code: '75', admin2: 'Paris', distance_m: 1234.5, beyond_max_distance: false,
};

test('the Immich column is one of the others: a link or the picker names it, and it is no custom pick', () => {
  const { resolveColumn, pickerHtml, parseLink, linkSearch, columnKey } = core();
  const c = withImmich();
  const col = resolveColumn(c, 'immich-cities500');
  assert.equal(col.dataset.id, 'immich-cities500');
  assert.equal(col.dataset.kind, 'immich');
  assert.equal(col.custom, false);
  assert.equal(col.error, null);
  assert.equal(columnKey(col), 'immich-cities500');
  // Made a custom pick, it starts from the default dataset's axes.
  assert.deepEqual(plain(col.selection), { region: 'planet', mode: 'full', quality: 'simplified', poi_tier: 'all' });
  assert.match(resolveColumn(catalog(), 'immich-cities500').error, /^unknown dataset "immich-cities500"/);

  const link = parseLink('?d=with-addresses&d=immich-cities500&at=48.8566,2.3522');
  assert.deepEqual(plain(link.datasets), ['with-addresses', 'immich-cities500']);
  assert.equal(linkSearch({ datasets: link.datasets, point: link.points[0] }), '?d=with-addresses&d=immich-cities500&at=48.8566,2.3522');

  const picker = pickerHtml(c, col, 1, 2);
  assert.match(picker, /<option value="immich-cities500" selected title="What Immich v3\.4\.2 stores[^"]*">Immich today \(GeoNames cities500\)<\/option>/);
  assert.ok(picker.includes('<div class="picker-name">Immich today (GeoNames cities500)</div>'), picker);
  assert.ok(picker.includes('Immich v3.4.2 · geodata of 2026-10-29 · immich-cities500'), picker);
  assert.ok(!picker.includes('on disk'), picker);
  assert.ok(!picker.includes('data-axis='), picker);
  assert.ok(!picker.includes('<option value="" selected>'), picker);
  const preset = pickerHtml(c, resolveColumn(c, 'with-addresses'), 0, 2);
  assert.match(preset, /<option value="immich-cities500" title="[^"]*">Immich today/);
  assert.match(preset, /<option value="with-addresses" selected/);
});

test('the Immich column shows what Immich stores and how it found it, beside the other columns', () => {
  const { detailHtml, resolveColumn, answersDiffer } = core();
  const c = withImmich();
  const geocoder = resolveColumn(c, 'with-addresses').dataset;
  const views = [
    { label: geocoder.label, color: '#111', dataset: geocoder,
      answer: { data: { display_name: 'Rue de Rivoli, Paris', address: { road: 'Rue de Rivoli', city: 'Paris', state: 'Île-de-France', country: 'France' } }, build: ID, ms: 3 } },
    { label: IMMICH.label, color: '#222', dataset: IMMICH,
      answer: { data: immichData('cities500', { city: 'Paris', state: 'Île-de-France', country: 'France' }, { place: PARIS, in_box: 57 }), build: null, dataset: IMMICH.id, ms: 2 } },
  ];
  const out = detailHtml(views, ID);
  assert.ok(out.includes('<div class="answer-name">Paris, Île-de-France, France</div>'), out);
  assert.ok(out.includes('cities500: <strong>Paris</strong> (PPLC, geonameid 2988507) at 48.85341, 2.34880, 1.2 km away, the nearest of 57 in the 25 km box'), out);
  assert.ok(out.includes('state: admin1 FR.11 → Île-de-France · country: FR → France · admin2 (not stored): Paris'), out);
  assert.ok(out.includes('Immich v3.4.2 · geodata of 2026-10-29T14:18:17+00:00'), out);
  assert.ok(!out.includes('beyond 25 km'), out);
  // Its answer is of no build: no sizes, no flag of another build.
  assert.equal(out.match(/ GiB on disk · /g).length, 1, out);
  assert.ok(!out.includes('tag stale'), out);
  // The field table sets its city, state and country beside the others'.
  assert.match(out, /<tr class="missing">\s*<td[^>]*>road<\/td>/);
  for (const field of ['city', 'state', 'country']) assert.match(out, new RegExp(`<tr class="">\\s*<td[^>]*>${field}</td>`), field);
  assert.deepEqual([...out.matchAll(/data-feedback="(\d+)"/g)].map((m) => m[1]), ['0', '1']);
  assert.equal(answersDiffer(views.map((v) => v.answer.data)), true);

  const corner = detailHtml([{ ...views[1], answer: { data: immichData('cities500', { city: 'Far', state: null, country: 'France' },
    { place: { ...PARIS, name: 'Far', admin1: null, distance_m: 33_900, beyond_max_distance: true }, in_box: 1 }) } }], ID);
  assert.ok(corner.includes('33.9 km away, the nearest of 1 in the 25 km box'), corner);
  assert.ok(corner.includes('>beyond 25 km</span> in a corner of the box'), corner);
  assert.ok(corner.includes('state: admin1 FR.11 → none'), corner);

  const kosovo = detailHtml([{ ...views[1], answer: { data: immichData('natural_earth', { city: null, state: null, country: null },
    { natural_earth: { admin: 'Kosovo', adm0_a3: 'KOS', type: 'Disputed', feature: 120, polygon: 0 } }) } }], ID);
  assert.ok(kosovo.includes("No cities500 place in the 25 km box: Natural Earth's <strong>Kosovo</strong> (KOS, Disputed) holds the point"), kosovo);
  assert.ok(kosovo.includes('countryInfo.txt has no KOS: Immich stores no country'), kosovo);
  assert.ok(kosovo.includes('<div class="answer-name">—</div>'), kosovo);
  const france = detailHtml([{ ...views[1], answer: { data: immichData('natural_earth', { city: null, state: null, country: 'France' },
    { natural_earth: { admin: 'France', adm0_a3: 'FRA', type: 'Country', feature: 1, polygon: 3 } }) } }], ID);
  assert.ok(!france.includes('countryInfo.txt has no'), france);

  const sea = detailHtml([{ ...views[1], answer: { data: immichData('nothing', { city: null, state: null, country: null }) } }], ID);
  assert.ok(sea.includes('No cities500 place in the 25 km box and no Natural Earth country holds the point: Immich stores no place'), sea);
  const null_island = detailHtml([{ ...views[1], answer: { data: immichData('no_gps', { city: null, state: null, country: null }) } }], ID);
  assert.ok(null_island.includes('0, 0: Immich takes it for no GPS and stores no place'), null_island);
});

test('the Immich column has no search and no polygons, and says so in its search head', () => {
  const { resolveColumn, searchDatasets, searchHeadsHtml, searchMatrixHtml, searchRows, overlayTarget, polygonKey } = core();
  const c = withImmich();
  const columns = ['with-addresses', 'immich-cities500', 'smallest'].map((token) => resolveColumn(c, token));
  assert.deepEqual(plain(searchDatasets(columns)), ['full.simplified.all', 'admin-minimal.simplified.none']);

  const results = { results: [hit('Paris', 48.85, 2.35)], us: 900, ms: 5, index: 'full.simplified.all' };
  const views = [searchView('With addresses', '#111', results, columns[0].dataset), searchView(IMMICH.label, '#222', null, IMMICH)];
  const heads = searchHeadsHtml(views, null);
  assert.ok(heads.includes(`${IMMICH.label}: <span class="warn">no search</span>: this column looks up points alone, as Immich does`), heads);
  assert.equal([...heads.matchAll(/data-feedback-search="(\d+)"/g)].length, 1, heads);
  const matrix = searchMatrixHtml(searchRows([results.results, null]), views, false);
  assert.ok(matrix.includes(`title="${IMMICH.label}: no search">·</td>`), matrix);

  assert.equal(polygonKey(columns[1]), null);
  assert.equal(overlayTarget(1, columns, ID), null);
  assert.deepEqual(plain(overlayTarget(2, columns, ID)), { key: 'admin-minimal.simplified.none', build: ID });
});

test("a report of the Immich column's answer names its dataset and no build", () => {
  const { reverseTarget, feedbackReport } = core();
  const data = immichData('cities500', { city: 'Paris', state: 'Île-de-France', country: 'France' }, { place: PARIS, in_box: 57 });
  const view = { label: IMMICH.label, color: '#222', dataset: IMMICH, answer: { data, build: null, dataset: IMMICH.id, ms: 2 } };
  const report = plain(feedbackReport(reverseTarget(view, { id: 'r1', lat: 48.8566, lng: 2.3522 }), { city: 'Paris 1er' },
    { columns: ['with-addresses', 'immich-cities500'], point: { lat: 48.8566, lng: 2.3522 }, q: '' }));
  assert.equal(report.dataset, 'immich-cities500');
  assert.equal(report.build, null);
  assert.deepEqual(report.answer, data);
  assert.equal(report.expected.city, 'Paris 1er');
  assert.deepEqual(report.page.columns, ['with-addresses', 'immich-cities500']);
});

test('every string of the Immich column is escaped where the page shows it', () => {
  const { pickerHtml, detailHtml, resolveColumn } = core();
  const hostileImmich = { ...IMMICH, label: '<img src=x onerror=alert(1)>', description: '"><script>alert(2)</script>',
    immich: { ...IMMICH.immich, release: '<b>v3</b>', geodata_date: '<i>today</i>' } };
  const c = { ...catalog(), others: [hostileImmich] };
  const picker = pickerHtml(c, resolveColumn(c, 'immich-cities500'), 0, 1);
  assertEscaped(picker);
  const place = { ...PARIS, name: '<img src=x onerror=alert(1)>', feature_code: '<b>P</b>', admin1: '<em>x</em>', admin2: '<u>y</u>' };
  const data = immichData('cities500', { city: place.name, state: '<em>x</em>', country: '<i>c</i>' }, { place, in_box: '<b>1</b>', release: '<b>v3</b>' });
  const ne = immichData('natural_earth', { city: null, state: null, country: null },
    { natural_earth: { admin: '<b>K</b>', adm0_a3: '<i>K</i>', type: '<u>t</u>', feature: 0, polygon: 0 } });
  for (const d of [data, ne]) {
    assertEscaped(detailHtml([{ label: hostileImmich.label, color: '#222', dataset: hostileImmich, answer: { data: d } }], ID));
  }
});

// -- The address shown whole --
// The field table's first two rows: the full address (display_name; the
// Immich column's stored values joined as Immich joins them) and what a
// photo there would show in Immich (immichShown of what Immich stores, or
// of what our integration would store from the answer: mapGeocoderAddress).
const NAMES = IMMICH.country_names;

// The rows of `datas`' answers shown whole, by field.
function shownRows(datas, names = NAMES) {
  const { displayRows } = core();
  return Object.fromEntries(plain(displayRows(datas, names)).map((r) => [r.field, r]));
}

// Answers of our datasets at the Eiffel Tower and Paris, as with-addresses
// and smallest give them.
const EIFFEL = {
  display_name: 'Eiffel Tower, Avenue Gustave Eiffel 5, Quartier du Gros-Caillou, 7th Arrondissement, Paris, Ile-de-France, Metropolitan France, 75007, France',
  address: { landmark: 'Eiffel Tower', house_number: '5', road: 'Avenue Gustave Eiffel', city: 'Paris', state: 'Ile-de-France', region: 'Metropolitan France',
    suburb: '7th Arrondissement', quarter: 'Quartier du Gros-Caillou', city_district: 'Paris', postcode: '75007', country: 'France', country_code: 'FR' },
};
const PARIS_SMALLEST = {
  display_name: 'Paris, Ile-de-France, Metropolitan France, France',
  address: { city: 'Paris', state: 'Ile-de-France', region: 'Metropolitan France', city_district: 'Paris', country: 'France', country_code: 'FR' },
};

test("the field table shows each answer whole: its full address and what Immich would show, a landmark first", () => {
  const passy = immichData('cities500', { city: 'Paris 16 Passy', state: 'Île-de-France', country: 'France' }, { place: PARIS, in_box: 459 });
  const rows = shownRows([EIFFEL, PARIS_SMALLEST, passy]);
  assert.deepEqual(rows['Full address'], {
    field: 'Full address',
    values: [EIFFEL.display_name, PARIS_SMALLEST.display_name, 'Paris 16 Passy, Île-de-France, France'],
    differs: true, missing: false,
  });
  assert.deepEqual(rows['In Immich'], {
    field: 'In Immich',
    values: ['Eiffel Tower, Paris, Ile-de-France, France', 'Paris, Ile-de-France, France', 'Paris 16 Passy, Île-de-France, France'],
    differs: true, missing: false, fallback: [false, false, false],
  });
  // Two answers Immich would show alike are no difference there, though
  // their full addresses differ.
  const alike = shownRows([PARIS_SMALLEST, { display_name: 'Paris, France', address: { city: 'Paris', state: 'Ile-de-France', country_code: 'fr' } }]);
  assert.equal(alike['Full address'].differs, true);
  assert.deepEqual([alike['In Immich'].values[1], alike['In Immich'].differs], ['Paris, Ile-de-France, France', false]);

  const { detailHtml, resolveColumn } = core();
  const c = withImmich();
  const geocoder = resolveColumn(c, 'with-addresses').dataset;
  const smallest = resolveColumn(c, 'smallest').dataset;
  const out = detailHtml([
    { label: geocoder.label, color: '#111', dataset: geocoder, answer: { data: EIFFEL, build: ID } },
    { label: smallest.label, color: '#222', dataset: smallest, answer: { data: PARIS_SMALLEST, build: ID } },
    { label: IMMICH.label, color: '#333', dataset: IMMICH, answer: { data: passy, dataset: IMMICH.id } },
  ], ID, NAMES);
  // The heading line stays the display name.
  assert.ok(out.includes(`<div class="answer-name">${EIFFEL.display_name}</div>`), out);
  // The two rows come first, before the address's fields, marked where
  // the columns differ.
  const names = [...out.matchAll(/<tr[^>]*>\s*<td class="field"[^>]*>([^<]+)<\/td>/g)].map((m) => m[1]);
  assert.deepEqual(names.slice(0, 3), ['Full address', 'In Immich', 'landmark']);
  assert.match(out, /<tr class="differs shown"><td class="field" title="[^"]*display_name[^"]*">Full address<\/td>/);
  assert.ok(out.includes('<td class="value">Eiffel Tower, Paris, Ile-de-France, France</td><td class="value">Paris, Ile-de-France, France</td>'
    + '<td class="value">Paris 16 Passy, Île-de-France, France</td>'), out);
  assert.ok(!out.includes('GeoNames fallback'), out);
});

test("what Immich would show is what our integration's mapping stores, as the branch writes it", () => {
  const shown = (address, names = NAMES) => shownRows([{ display_name: 'x', address }], names)['In Immich'].values[0];
  // A village is the city; its county the state where it has no state.
  assert.equal(shown({ village: 'Giverny', county: 'Eure', country: 'France', country_code: 'fr' }), 'Giverny, Eure, France');
  assert.equal(shown({ hamlet: 'Le Hameau', municipality: 'Commune', state: 'Normandie', country_code: 'FR' }), 'Le Hameau, Normandie, France');
  // The city stands in for the state of a city-state.
  assert.equal(shown({ city: 'Taipei', country: 'Taiwan', country_code: 'tw' }), 'Taipei, Taipei, Taiwan');
  // The country is Immich's name of the code, not the answer's: not
  // i18n-iso-countries' 'United States of America' of the branch, which
  // differs from Immich's only by its library.
  assert.equal(shown({ city: 'Austin', state: 'Texas', country: 'United States of America', country_code: 'us' }), 'Austin, Texas, United States');
  assert.equal(shown({ city: 'Berlin', state: 'Berlin', country: 'Deutschland', country_code: 'de' }), 'Berlin, Berlin, Germany');
  // A dependent territory mapped as a state of its sovereign is its own
  // country, folded as the branch folds names.
  assert.equal(shown({ city: 'San Juan', state: 'Puerto Rico', country: 'United States', country_code: 'us' }), 'San Juan, Puerto Rico, Puerto Rico');
  assert.equal(shown({ city: 'Gustavia', state: 'Saint-Barthélemy', country: 'France', country_code: 'fr' }, { ...NAMES, BL: 'Saint Barthelemy' }),
    'Gustavia, Saint-Barthélemy, Saint Barthelemy');
  // A code Immich names no country by: the answer's country.
  assert.equal(shown({ town: 'Ushuaia', state: 'Tierra del Fuego', country: 'Argentina', country_code: 'ar' }), 'Ushuaia, Tierra del Fuego, Argentina');
  assert.equal(shown({ city: 'Paris', country: 'France', country_code: 'FR' }, {}), 'Paris, Paris, France');
  // No country at all: none stored (the integration falls back).
  assert.equal(shown({ city: 'X', country_code: 'zz' }, {}), '');
});

test("Immich shows nothing of a place without a country, and the full address joins what it stores", () => {
  // A cities500 place whose code countryInfo.txt doesn't name: Immich
  // stores its city and state, and its detail panel shows nothing.
  const nameless = immichData('cities500', { city: 'Pristina', state: 'Pristina', country: null }, { place: PARIS, in_box: 3 });
  const rows = shownRows([nameless]);
  assert.deepEqual([rows['Full address'].values, rows['In Immich'].values], [['Pristina, Pristina'], ['']]);
  // A Natural Earth country alone.
  const ne = immichData('natural_earth', { city: null, state: null, country: 'France' });
  assert.deepEqual(plain(shownRows([ne])['In Immich'].values), ['France']);
});

test('an empty answer at sea shows no address, and falls back to what Immich stores as the integration does', () => {
  const { detailHtml, resolveColumn } = core();
  const c = withImmich();
  const geocoder = resolveColumn(c, 'with-addresses').dataset;
  const sea = immichData('nothing', { city: null, state: null, country: null });
  const views = [
    { label: geocoder.label, color: '#111', dataset: geocoder, answer: { data: { address: {} }, build: ID } },
    { label: IMMICH.label, color: '#222', dataset: IMMICH, answer: { data: sea, dataset: IMMICH.id } },
  ];
  const rows = shownRows(views.map((v) => v.answer.data));
  assert.deepEqual(rows['Full address'], { field: 'Full address', values: ['', ''], differs: false, missing: false });
  assert.deepEqual(rows['In Immich'], { field: 'In Immich', values: ['', ''], differs: false, missing: false, fallback: [true, false] });
  const out = detailHtml(views, ID, NAMES);
  // The two rows show though no field does, each answer's '—'.
  assert.match(out, /<tr class="shown"><td class="field"[^>]*>Full address<\/td><td class="empty">—<\/td><td class="empty">—<\/td><\/tr>/);
  assert.match(out, /<tr class="shown"><td class="field"[^>]*>In Immich<\/td><td class="empty">— <span class="tag" title="No country in this answer: [^"]*the Immich column's">GeoNames fallback<\/span><\/td><td class="empty">—<\/td><\/tr>/);

  // Off a coast our datasets name nothing at, Immich's lookup may find a
  // place: the integration stores that, as the Immich column shows it.
  const shore = immichData('cities500', { city: 'Saint-Malo', state: 'Brittany', country: 'France' }, { place: PARIS, in_box: 2 });
  const near = shownRows([{ address: {} }, shore]);
  assert.deepEqual([near['In Immich'].values, near['In Immich'].differs], [['Saint-Malo, Brittany, France', 'Saint-Malo, Brittany, France'], false]);
  assert.deepEqual(near['Full address'].values, ['', 'Saint-Malo, Brittany, France']);
  assert.equal(near['Full address'].missing, true);
  // Without the Immich column it is unknown here, and says so.
  const alone = detailHtml([views[0]], ID, NAMES);
  assert.match(alone, /<td class="empty">— <span class="tag" title="[^"]*as the Immich column shows it">GeoNames fallback<\/span><\/td>/);
  // A column without an answer shows none.
  const unanswered = detailHtml([views[0], { ...views[1], answer: { loading: true } }], ID, NAMES);
  assert.match(unanswered, /<td class="field"[^>]*>Full address<\/td><td class="empty">—<\/td><td class="empty"><\/td>/);
  assert.ok(!detailHtml([{ ...views[0], answer: { loading: true } }], ID, NAMES).includes('Full address'));
});

test("the page names our answers' countries by the Immich column's names, none without it", () => {
  const { immichCountryNames } = core();
  assert.deepEqual(plain(immichCountryNames(withImmich())), NAMES);
  assert.deepEqual(plain(immichCountryNames(catalog())), {});
  assert.deepEqual(plain(immichCountryNames(null)), {});
});

test('the address shown whole is escaped', () => {
  const { detailHtml, resolveColumn } = core();
  const c = withImmich();
  const geocoder = resolveColumn(c, 'with-addresses').dataset;
  const data = { display_name: '<img src=x onerror=alert(1)>', address: { landmark: '<b>L</b>', city: '<i>c</i>', country_code: 'xx' } };
  const out = detailHtml([{ label: 'x', color: '#111', dataset: geocoder, answer: { data } }], ID, { XX: '<em>X</em>' });
  assertEscaped(out);
  assert.ok(out.includes('&lt;b&gt;L&lt;/b&gt;, &lt;i&gt;c&lt;/i&gt;, &lt;i&gt;c&lt;/i&gt;, &lt;em&gt;X&lt;/em&gt;'), out);
});
