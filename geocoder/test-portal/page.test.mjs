// The test page's logic: index.html's <script id="core">, which reads no DOM
// (the link, the columns and their datasets, the pacing of requests, the
// comparison of answers and the HTML of each part), run in a context of its
// own as the page runs it. The page's other scripts, which wire it to the DOM
// and the map, are checked to parse. /datasets is that of a build of the
// template, as the portal lists it (server/src/portal/catalog.rs).
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

// A template whose every string the page shows is markup.
function hostile(t) {
  t.presets[0].label = '<img src=x onerror=alert(1)>';
  t.presets[0].description = '"><script>alert(2)</script>';
  t.axes.mode.label = '<u>mode</u>';
  t.axes.mode.description = '" onmouseover="alert(3)';
  t.axes.mode.values.full.label = '<b>Full</b>';
  t.axes.quality.values.exact.description = '<i>every point</i>';
  t.constraints[0].message = '<em>no</em> & "quotes"';
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
    assert.ok(preset.includes(`<option value="${p.id}"`), p.id);
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

test('every string of the manifest is escaped where the page shows it', () => {
  const { pickerHtml, resolveColumn, detailHtml, cardHtml, statusHtml } = core();
  const c = catalog(hostile);
  const picker = pickerHtml(c, resolveColumn(c, 'smallest'), 0, 2) + pickerHtml(c, resolveColumn(c, 'admin-minimal.exact.none'), 1, 2)
    + pickerHtml(c, resolveColumn(c, 'full.exact.all'), 2, 3);
  assertEscaped(picker);
  assert.ok(picker.includes('&lt;img src=x onerror=alert(1)&gt;'));
  assert.ok(picker.includes('&lt;em&gt;no&lt;/em&gt; &amp; &quot;quotes&quot;'));

  const dataset = c.datasets.find((d) => d.id === 'admin-minimal.simplified.none');
  const answer = { data: { display_name: '<img src=y>', address: { road: '<b>x</b>' }, places: [{ name: '<i>p</i>', category: '<u>c</u>', distance_m: 3 }] }, build: '<s>', ms: 3 };
  const views = [{ label: dataset.label, color: '#fff', dataset, answer }, { label: 'x', color: '#000', dataset: null, answer: { error: '<script>e</script>' } }];
  const detail = detailHtml(views, '2026-11-20');
  const card = cardHtml({ id: 1, lat: 1, lng: 2, source: '<img src=s>' }, views, false);
  for (const out of [detail, card]) {
    assertEscaped(out);
    assert.ok(!out.includes('<s>'));
  }
  assertEscaped(statusHtml({ status: 'unavailable', reason: '<img src=r>', sync: { running: '<b>r</b>', refused: [{ date: '<i>', version: 1, reason: '<script>', at: '' }], damaged: '<u>d</u>', last_result: null } }));
});

test('an answer keeps the dataset and build that answered and the time it took', () => {
  const { answerOf } = core();
  const headers = { 'x-geocoder-build': '2026-11-20', 'x-geocoder-dataset': 'full.simplified.all' };
  const header = (name) => headers[name.toLowerCase()] ?? null;
  assert.deepEqual(plain(answerOf(200, header, '{"display_name":"Paris"}', 12.4)),
    { data: { display_name: 'Paris' }, build: '2026-11-20', dataset: 'full.simplified.all', ms: 12.4 });
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
});

test('a link of another build than the one served is flagged', () => {
  const { mismatchHtml } = core();
  const flagged = mismatchHtml('2026-11-19', '2026-11-20');
  assert.ok(flagged.includes('2026-11-19') && flagged.includes('2026-11-20'), flagged);
  assert.equal(mismatchHtml('2026-11-20', '2026-11-20'), '');
  assert.equal(mismatchHtml(null, '2026-11-20'), '');
  assert.equal(mismatchHtml('2026-11-19', null), '');
  assertEscaped(mismatchHtml('<img src=x>', '2026-11-20'));
});

test('the status line tells the build served and how the sync goes', () => {
  const { statusHtml } = core();
  const sync = {
    running: null, progress: null, last_started: '2026-11-20T07:10:00Z', last_finished: '2026-11-20T07:12:03Z', next_round: '2026-11-20T08:12:03Z',
    last_result: 'ok: serving 2026-11-20-1', latest: { date: '2026-11-21', build_version: 25 }, served: '2026-11-20-1',
    refused: [{ date: '2026-11-21', version: 25, reason: 'build version 25, not 24', at: '2026-11-21T07:00:00Z' }], damaged: null,
  };
  const ok = statusHtml({ status: 'ok', build: { date: '2026-11-20', version: 24, dir: '2026-11-20-1' }, datasets: 28, sync });
  for (const text of ['build 2026-11-20 (v24) · 28 datasets', 'last sync 2026-11-20 07:12:03 UTC: ok: serving 2026-11-20-1', 'next 2026-11-20 08:12:03 UTC',
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
  const got = await Promise.all(Array.from({ length: 40 }, (_, i) => pacer.request(`/reverse?n=${i}`)));
  assert.equal(most, 8);
  assert.ok(got.every((g) => g.status === 200 && g.body === '{}' && g.ms >= 0));
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
      pauses.push(pacer.pause);
      return reply(200, '{}');
    },
  });
  const got = await Promise.all(Array.from({ length: 8 }, (_, i) => pacer.request(`/reverse?n=${i}`)));
  assert.ok(got.every((g) => g.status === 200));
  assert.equal(pauses.length, 8);
  assert.equal(Math.max(...pauses), 30);
});

test('a request that fails frees its place', async () => {
  const { Pacer } = core();
  let calls = 0;
  const pacer = new Pacer({ limit: 1, fetch: async () => { calls++; if (calls === 1) throw new Error('offline'); return reply(200, 'ok'); } });
  await assert.rejects(pacer.request('a'), /offline/);
  assert.equal((await pacer.request('b')).body, 'ok');
});

test('the polygon overlay cycles off, each column, off', () => {
  const { nextPolygons } = core();
  assert.deepEqual([nextPolygons('off', 2), nextPolygons(0, 2), nextPolygons(1, 2)], [0, 1, 'off']);
  assert.equal(nextPolygons('off', 0), 'off');
  assert.equal(nextPolygons(3, 2), 'off');
});

test('the JSON of an answer is shown highlighted and escaped', () => {
  const { syntaxHighlight } = core();
  const out = syntaxHighlight({ name: '<img src=x>', n: 1, ok: true, none: null });
  assert.ok(!out.includes('<img'));
  assert.ok(out.includes('<span class="json-string">"&lt;img src=x&gt;"</span>'), out);
  assert.ok(out.includes('<span class="json-key">"name":</span>'));
  assert.ok(out.includes('<span class="json-number">1</span>'));
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
