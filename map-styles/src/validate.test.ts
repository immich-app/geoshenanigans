import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { CLIENT_SPECS, validateStyle } from './validate';

const styleWith = (layer: object) => ({
  version: 8,
  sources: { vector: { type: 'vector', url: 'https://tiles.immich.cloud/v1.json' } },
  layers: [{ id: 'test', type: 'fill', source: 'vector', 'source-layer': 'landuse', ...layer }],
});

const latestOnly = CLIENT_SPECS.filter(({ client }) => client === 'latest spec');

describe('validateStyle', () => {
  it('accepts a style both specs accept', (t) => {
    const warn = t.mock.method(console, 'warn', () => {});
    validateStyle('test', styleWith({ paint: { 'fill-opacity': 0.5 } }));
    assert.equal(warn.mock.callCount(), 0);
  });

  it('rejects a property only the latest spec knows', () => {
    // fill-layer-opacity needs maplibre-gl 6: the latest spec accepts it, but 5.6 would drop the whole style.
    const style = styleWith({ paint: { 'fill-layer-opacity': 0.5 } });
    assert.doesNotThrow(() => validateStyle('test', style, latestOnly));
    assert.throws(() => validateStyle('test', style), /unknown property "fill-layer-opacity" \(oldest client spec\)$/);
  });

  it('logs warnings without failing', (t) => {
    const warn = t.mock.method(console, 'warn', () => {});
    // The latest spec warns about a legacy filter inside an expression; the oldest reads it as comparing two strings.
    validateStyle('test', styleWith({ filter: ['!', ['==', 'kind', 'park']] }));
    assert.equal(warn.mock.callCount(), 1);
    assert.match(String(warn.mock.calls[0].arguments[0]), /^test: .*Mixing deprecated filter syntax.*\(latest spec\)$/);
  });

  it('fails on errors that come with warnings', (t) => {
    const warn = t.mock.method(console, 'warn', () => {});
    const style = styleWith({ filter: ['!', ['in', 'kind', 'park', 'forest']] });
    assert.throws(() => validateStyle('test', style, latestOnly), /Expected 2 arguments, but found 3 instead\. \(latest spec\)$/);
    assert.equal(warn.mock.callCount(), 1);
  });
});
