import { describe, expect, it } from 'vitest';
import { tileIdToZxy, tileIdZoom, tilePath, zoomEnd, zoomRange, zoomStart } from '../../src/bunny/tile-id';
import { zxyToTileId } from '../../src/pmtiles/utils';
import { random } from './upload-fixture';

describe('tile ids', () => {
  it('knows where each zoom starts', () => {
    expect([0, 1, 2, 3, 8, 15].map(zoomStart)).toEqual([0, 1, 5, 21, 21845, 357913941]);
    expect(zoomEnd(15)).toBe(1_431_655_765);
    expect(zoomRange(0, 8)).toEqual({ from: 0, to: 87_381 });
    expect(zoomRange(15, 15)).toEqual({ from: 357_913_941, to: 1_431_655_765 });
    expect(() => zoomRange(3, 2)).toThrow();
  });

  it('round-trips every tile of zooms 0-7 both ways', () => {
    for (let z = 0; z <= 7; z++) {
      for (let x = 0; x < 2 ** z; x++) {
        for (let y = 0; y < 2 ** z; y++) {
          const id = zxyToTileId(z, x, y);
          expect(tileIdToZxy(id)).toEqual([z, x, y]);
        }
      }
    }
    for (let id = 0; id < zoomEnd(7); id++) {
      expect(zxyToTileId(...tileIdToZxy(id))).toBe(id);
    }
  });

  it('round-trips random tiles up to zoom 26', () => {
    const next = random(42);
    for (let i = 0; i < 5_000; i++) {
      const z = Math.floor(next() * 27);
      const x = Math.floor(next() * 2 ** z);
      const y = Math.floor(next() * 2 ** z);
      const id = zxyToTileId(z, x, y);
      expect(tileIdToZxy(id)).toEqual([z, x, y]);
      expect(tileIdZoom(id)).toBe(z);
    }
    for (const z of [15, 16, 20, 26]) {
      expect(tileIdToZxy(zoomStart(z))).toEqual([z, 0, 0]);
      expect(zxyToTileId(...tileIdToZxy(zoomEnd(z) - 1))).toBe(zoomEnd(z) - 1);
    }
  });

  it('rejects ids that are not tile ids', () => {
    expect(() => tileIdToZxy(-1)).toThrow();
    expect(() => tileIdToZxy(1.5)).toThrow();
    expect(() => tileIdToZxy(zoomEnd(26))).toThrow();
  });

  it('builds zone paths', () => {
    expect(tilePath('v1', 0)).toBe('v1/0/0/0.mvt');
    expect(tilePath('v1', zxyToTileId(15, 17000, 11000))).toBe('v1/15/17000/11000.mvt');
    expect(tilePath('/itest-a-1/v1/', 1)).toBe('itest-a-1/v1/1/0/0.mvt');
    expect(tilePath('', 1)).toBe('1/0/0.mvt');
  });
});
