// The test page's logic: index.html's <script id="core">, which reads no DOM
// (the link, the columns and their datasets, the pacing of requests, the
// comparison of answers and the HTML of each part), run in a context of its
// own as the page runs it. The page's other scripts, which wire it to the DOM
// and the map, are checked to parse, and its style for the layout of the side
// panel. /datasets is that of a build of the template, as the portal lists
// it (server/src/portal/catalog.rs).
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
  const context = vm.createContext({ URLSearchParams, setTimeout, clearTimeout });
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
        const preset = t.presets.find((x) => has(x.selections));
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
  return { build: { date: '2026-11-20', version: 24, dir: '2026-11-20-1' }, default: 'full.simplified.all', axes, presets, datasets, excluded };
}

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

// A Pacer of the page's on `clock`, its first pause 100 ms, whose requests
// are answered when the test answers them: each in `sent` once it goes,
// with its URL, the time it went and answer(status).
function handPacer(Pacer, clock) {
  const sent = [];
  const pacer = new Pacer({
    firstPause: 100,
    now: clock.now,
    sleep: clock.sleep,
    fetch: (url) => new Promise((resolve) => sent.push({ url, at: clock.t, answer: (status) => resolve(reply(status, url)) })),
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
  });
  assert.equal(
    linkSearch({ datasets: ['with-addresses', 'europe/full.simplified.all'], point: { lat: 48.8584, lng: 2.2945 }, build: '2026-11-20' }),
    '?d=with-addresses&d=europe/full.simplified.all&at=48.8584,2.2945&b=2026-11-20',
  );
  assert.deepEqual(plain(parseLink(linkSearch({ datasets: ['smallest'], point: { lat: 1.000000123, lng: -2.5 }, build: null }))),
    { datasets: ['smallest'], points: [{ lat: 1.000000123, lng: -2.5 }], build: null });
  assert.equal(linkSearch({ datasets: [], point: null, build: null }), '');
  assert.equal(parseLink('?d=smallest&b=2026-11-20-2').build, '2026-11-20-2');
  assert.equal(linkSearch({ datasets: ['smallest'], point: null, build: '2026-11-20-2' }), '?d=smallest&b=2026-11-20-2');
});

test('a link keeps 4 columns and the points on the globe', () => {
  const { parseLink } = core();
  const link = parseLink('?d=a&d=b&d=c&d=d&d=e&d=&at=91,0&at=0,181&at=x,1&at=1&at=,&at=0,0');
  assert.deepEqual(plain(link.datasets), ['a', 'b', 'c', 'd']);
  assert.deepEqual(plain(link.points), [{ lat: 0, lng: 0 }]);
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

// The build is the portal's dir of it (X-Geocoder-Build-Dir), which tells a
// build published again under its date from the one before.
test('an answer keeps the dataset and build that answered and the time it took', () => {
  const { answerOf } = core();
  const headers = { 'x-geocoder-build': '2026-11-20', 'x-geocoder-build-dir': '2026-11-20-2', 'x-geocoder-dataset': 'full.simplified.all' };
  const header = (name) => headers[name.toLowerCase()] ?? null;
  assert.deepEqual(plain(answerOf(200, header, '{"display_name":"Paris"}', 12.4)),
    { data: { display_name: 'Paris' }, build: '2026-11-20-2', dataset: 'full.simplified.all', ms: 12.4 });
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

test('answers differ by display name, a field or the places', () => {
  const { answersDiffer } = core();
  const paris = { display_name: 'Paris', address: { city: 'Paris' }, places: [{ name: 'Louvre', category: 'museum' }] };
  assert.equal(answersDiffer([paris, structuredClone(paris)]), false);
  assert.equal(answersDiffer([paris, null]), false);
  assert.equal(answersDiffer([paris, { ...paris, display_name: 'Paris 1er' }]), true);
  assert.equal(answersDiffer([paris, { ...paris, address: { city: 'Paris', postcode: '75001' } }]), true);
  assert.equal(answersDiffer([paris, { ...paris, places: [] }]), true);
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
  assert.match(out, /class="tag stale"[^>]*>build 2026-11-19/);
  assert.equal(out.match(/class="tag stale"/g).length, 1);
  assert.match(out, /<tr class="differs">\s*<td[^>]*>city<\/td>/);
  assert.ok(out.includes('loading-spinner'));

  // Of the builds of one date, the one published before is another; a date
  // alone (an answer saved before builds were named by their dir) is any.
  const republished = detailHtml([
    { label: 'before', color: '#111', dataset, answer: answer('Paris', '2026-11-20-1') },
    { label: 'now', color: '#222', dataset, answer: answer('Paris', '2026-11-20-2') },
    { label: 'saved', color: '#333', dataset, answer: answer('Paris', '2026-11-20') },
  ], '2026-11-20-2');
  assert.match(republished, /class="tag stale"[^>]*>build 2026-11-20-1, not 2026-11-20-2/);
  assert.equal(republished.match(/class="tag stale"/g).length, 1, republished);
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

// The columns' answers of a result may come from builds older than the one
// served now (saved, or asked before the build changed): its link names the
// oldest, so that a link to answers the server no longer gives is flagged.
test('a link to a result names the oldest build that answered it in the columns', () => {
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
  const republished = {
    a: { data: {}, build: '2026-11-20-10' },
    b: { data: {}, build: '2026-11-20-9' },
    c: { data: {}, build: '2026-11-21-1' },
  };
  assert.equal(linkBuild(republished, ['a', 'b', 'c'], '2026-11-21-1'), '2026-11-20-9');
  assert.equal(linkBuild(republished, ['a', 'c'], '2026-11-21-1'), '2026-11-20-10');
});

test('a link of another build than the one served is flagged', () => {
  const { mismatchHtml } = core();
  const flagged = mismatchHtml('2026-11-19', '2026-11-20');
  assert.ok(flagged.includes('2026-11-19') && flagged.includes('2026-11-20'), flagged);
  assert.equal(mismatchHtml('2026-11-20', '2026-11-20'), '');
  assert.equal(mismatchHtml(null, '2026-11-20'), '');
  assert.equal(mismatchHtml('2026-11-19', null), '');
  assertEscaped(mismatchHtml('<img src=x>', '2026-11-20'));
  const republished = mismatchHtml('2026-11-20-1', '2026-11-20-2');
  assert.ok(republished.includes('build 2026-11-20-1') && republished.includes('build 2026-11-20-2'), republished);
  assert.equal(mismatchHtml('2026-11-20-2', '2026-11-20-2'), '');
  assert.equal(mismatchHtml('2026-11-20', '2026-11-20-2'), '');
  assert.ok(mismatchHtml('2026-11-19', '2026-11-20-2').includes('2026-11-19'));
});

// /health names the build served by its dir: once that is another than the
// one the columns are of (a build published again under its date among
// them), or one is served while the page waits for a build, the page loads
// /datasets anew.
test('the page loads the datasets anew once /health names another build', () => {
  const { catalogStale } = core();
  const c = catalog();
  const health = (dir) => ({ status: 'ok', build: { date: dir.slice(0, 10), version: 24, dir }, datasets: 28, sync: {} });
  assert.equal(catalogStale(health('2026-11-20-1'), 'catalog', c), false);
  assert.equal(catalogStale(health('2026-11-20-2'), 'catalog', c), true);
  assert.equal(catalogStale(health('2026-11-21-1'), 'catalog', c), true);
  assert.equal(catalogStale(health('2026-11-20-1'), 'waiting', null), true);
  assert.equal(catalogStale({ status: 'unavailable', reason: 'no build to serve', sync: {} }, 'waiting', null), false);
  assert.equal(catalogStale(null, 'catalog', c), false);
  assert.equal(catalogStale(health('2026-11-20-2'), 'single', null), false);
});

test('the status line tells the build served and how the sync goes', () => {
  const { statusHtml } = core();
  const sync = {
    running: null, progress: null, last_started: '2026-11-20T07:10:00Z', last_finished: '2026-11-20T07:12:03Z', next_round: '2026-11-20T08:12:03Z',
    last_result: 'ok: serving 2026-11-20-1', latest: { date: '2026-11-21', build_version: 25 }, served: '2026-11-20-1',
    refused: [{ date: '2026-11-21', version: 25, reason: 'build version 25, not 24', at: '2026-11-21T07:00:00Z' }],
  };
  const ok = statusHtml({ status: 'ok', build: { date: '2026-11-20', version: 24, dir: '2026-11-20-1' }, datasets: 28, sync });
  for (const text of ['build 2026-11-20-1 (v24) · 28 datasets', 'last sync 2026-11-20 07:12:03 UTC: ok: serving 2026-11-20-1', 'next 2026-11-20 08:12:03 UTC',
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

test('the requests in flight are at most 8, over every column', async () => {
  const { Pacer } = core();
  let now = 0;
  let most = 0;
  const pacer = new Pacer({
    fetch: async () => {
      now++;
      most = Math.max(most, now);
      await new Promise((r) => setTimeout(r, 2));
      now--;
      return reply(200, '{}');
    },
  });
  const batch = () => Promise.all(Array.from({ length: 40 }, (_, i) => pacer.request(`/reverse?n=${i}`)));
  const got = await batch();
  assert.equal(most, 8);
  assert.ok(got.every((g) => g.status === 200 && g.body === '{}' && g.ms >= 0));
  // Every place is free once they are answered, and the next ones are held
  // to 8 as well.
  assert.deepEqual([pacer.active, pacer.waiting.length], [0, 0]);
  most = 0;
  await batch();
  assert.equal(most, 8);
});

test('a 429 is asked again after a pause, which every request waits out', async () => {
  const { Pacer } = core();
  const started = [];
  let first = true;
  const t0 = Date.now();
  const pacer = new Pacer({
    firstPause: 60,
    fetch: async (url) => {
      started.push([url, Date.now() - t0]);
      if (first) {
        first = false;
        return reply(429, 'Rate limit exceeded');
      }
      return reply(200, url);
    },
  });
  const a = pacer.request('a');
  await new Promise((r) => setTimeout(r, 5));
  const b = await pacer.request('b');
  assert.equal((await a).status, 200);
  assert.equal(b.status, 200);
  assert.deepEqual(started.map(([url]) => url).sort(), ['a', 'a', 'b']);
  const [, bAt] = started.find(([url]) => url === 'b');
  const [, againAt] = started.filter(([url]) => url === 'a')[1];
  assert.ok(bAt >= 55, `b started ${bAt} ms in, within the pause`);
  assert.ok(againAt >= 55, `a was asked again ${againAt} ms in, within the pause`);
});

test('the pause doubles with each 429 in a row, and the last of its tries is the answer', async () => {
  const { Pacer } = core();
  const at = [];
  const t0 = Date.now();
  const pacer = new Pacer({ firstPause: 20, tries: 4, fetch: async () => { at.push(Date.now() - t0); return reply(429, 'Rate limit exceeded'); } });
  const got = await pacer.request('x');
  assert.equal(got.status, 429);
  assert.equal(got.body, 'Rate limit exceeded');
  assert.equal(at.length, 4);
  const gaps = at.slice(1).map((t, i) => t - at[i]);
  assert.ok(gaps[0] >= 18 && gaps[1] >= 38 && gaps[2] >= 78, `gaps ${gaps}`);
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
// as data (or error) and that of the minimal one as minimal: they are those
// of with-addresses and smallest. A failed one is asked again.
test('a saved result keeps its answers, those the page saved before the dataset columns too', () => {
  const { storedResult } = core();
  const now = { id: 'a', lat: 1, lng: 2, source: 'map click', nominatim: null,
    answers: { 'admin.simplified.major': { data: { display_name: 'Paris' }, build: '2026-11-20', dataset: 'admin.simplified.major', ms: 3 } } };
  assert.deepEqual(plain(storedResult(now)), now);
  const before = { id: 1763600000000, lat: 48.85, lng: 2.29, data: { display_name: 'Paris' }, nominatim: { display_name: 'Paris, France' },
    minimal: { display_name: 'France' }, source: 'IMG_1.jpg' };
  assert.deepEqual(plain(storedResult(before)), {
    id: 1763600000000, lat: 48.85, lng: 2.29, source: 'IMG_1.jpg', nominatim: { display_name: 'Paris, France' },
    answers: { 'full.simplified.all': { data: { display_name: 'Paris' } }, 'admin-minimal.simplified.none': { data: { display_name: 'France' } } },
  });
  assert.deepEqual(plain(storedResult({ id: 2, lat: 1, lng: 2, data: null, minimal: null, error: 'HTTP 503' }).answers), {});
  assert.equal(storedResult({ id: 3, lat: 91, lng: 0, answers: {} }), null);
  assert.equal(storedResult(null), null);
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

// A reload of /datasets may give the overlay's column another dataset, the
// columns another build, or leave the overlay no column: what it showed is
// then of none of them, and goes. A point selected while the portal waited
// for its build was asked of the one column of the server as it was.
test('a reload of the datasets moves the polygon overlay where its dataset or build is another, or it is off', () => {
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
});

test('the JSON of an answer is shown highlighted and escaped', () => {
  const { syntaxHighlight } = core();
  const out = syntaxHighlight({ name: '<img src=x>', n: 1, ok: true, none: null });
  assert.ok(!out.includes('<img'));
  assert.ok(out.includes('<span class="json-string">"&lt;img src=x&gt;"</span>'), out);
  assert.ok(out.includes('<span class="json-key">"name":</span>'));
  assert.ok(out.includes('<span class="json-number">1</span>'));
});

// The side panel stacks the datasets, the coordinates, the photo upload, the
// results and, once one is selected, its detail. Four columns of datasets
// and the detail of a result fill a window of 1000 px without the results,
// whose toolbar and cards would go: they keep a height of their own, the
// datasets and the detail scroll within what is left, down to a least
// height of theirs, and a window too short for those scrolls the panel
// rather than cutting off its foot.
test('the results keep room for their toolbar and cards however tall the datasets and the detail grow', () => {
  const height = /\b[1-9]\d*(px|%|vh)/;
  assert.match(style('.results-section')['min-height'] ?? 'none', height);
  for (const giving of ['.datasets-section', '.detail-panel']) {
    assert.equal(style(giving)['overflow-y'], 'auto', giving);
    assert.match(style(giving)['min-height'] ?? 'none', height, giving);
  }
  assert.equal(style('.side-panel')['overflow-y'], 'auto');
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
