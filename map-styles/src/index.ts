import themes, { ImmichTheme } from './themes';
import fs from 'fs';
import path from 'path';
import { layers } from '@protomaps/basemaps';
import { validateStyle } from './validate';

// POI kinds the protomaps sprites have no icon for, and the icon to show instead.
const ICON_FALLBACKS: Record<string, string> = { townhall: 'building' };

// The pois layer picks its icon with ["match", ["get", "kind"], ...branches, <the kind itself>]; give kinds without
// an icon a branch to their fallback, unless upstream already handles them.
const withIconFallbacks = (layer: ReturnType<typeof layers>[number]) => {
  const icon = layer.type === 'symbol' ? layer.layout?.['icon-image'] : undefined;
  if (layer.id !== 'pois' || !Array.isArray(icon) || icon[0] !== 'match') {
    return layer;
  }
  const branches = icon.slice(2, -1);
  const labels = branches.filter((_, i) => i % 2 === 0);
  const extra = Object.entries(ICON_FALLBACKS).filter(([kind]) => !labels.includes(kind)).flat();
  const iconImage = ['match', icon[1], ...branches, ...extra, icon[icon.length - 1]];
  return { ...layer, layout: { ...layer.layout, 'icon-image': iconImage } } as typeof layer;
};

type Layer = ReturnType<typeof layers>[number];

// Buildings turn 3D at z15, the tiles' maxzoom (z14 tiles merge buildings and round heights to 5 m), growing from
// flat at z15 to full height at z16.
const BUILDINGS_3D_ZOOM = 15;
const BUILDINGS_3D_FULL_ZOOM = 16;
// For buildings without height or building:levels (the tiles already turn levels into levels * 3 + 2 m): one level
// above their base.
const DEFAULT_BUILDING_HEIGHT = 5;

const growWithZoom = (value: unknown) => [
  'interpolate', ['linear'], ['zoom'], BUILDINGS_3D_ZOOM, 0, BUILDINGS_3D_FULL_ZOOM, value,
];
// The base a building part stands on, and its roof: its height, or one level above its base without one (a part
// can carry min_height alone). The base never goes above the roof, which would turn the walls inside out.
const BASE = ['coalesce', ['get', 'min_height'], 0];
const ROOF = ['coalesce', ['get', 'height'], ['+', BASE, DEFAULT_BUILDING_HEIGHT]];
// Shown from z15 on (1) or not (0). The extrusion and the flat fill both stay in the z14 tiles too and switch over by
// opacity at z15: a z14 tile drawn in place of a z15 one still loading keeps its buildings either way.
const from3dZoom = (below: number, above: number) => ['step', ['zoom'], below, BUILDINGS_3D_ZOOM, above];

// Replaces the flat buildings fill with an extrusion from z15. The extrusion goes after every road, bridge and
// boundary layer and before the first label: MapLibre draws layers above a 3D layer over it without depth testing,
// so a road above it would cross building walls; the labels stay on top.
const with3dBuildings = (all: Layer[], source: string, theme: ImmichTheme): Layer[] => {
  const flat = all.findIndex((layer) => layer.id === 'buildings');
  const firstLabel = all.findIndex((layer) => layer.type === 'symbol');
  if (flat === -1 || firstLabel === -1 || firstLabel < flat) {
    throw new Error('basemaps layer order changed: expected the buildings fill before the first symbol layer');
  }
  const extrusion = {
    id: 'buildings_3d',
    type: 'fill-extrusion',
    source,
    'source-layer': 'buildings',
    minzoom: BUILDINGS_3D_ZOOM - 1,
    filter: ['in', 'kind', 'building', 'building_part'],
    paint: {
      'fill-extrusion-color': theme.buildings3d,
      // Opaque from z15: translucent extrusions take an extra depth-only pass over every building.
      'fill-extrusion-opacity': from3dZoom(0, 1),
      'fill-extrusion-height': growWithZoom(ROOF),
      'fill-extrusion-base': growWithZoom(['min', BASE, ROOF]),
    },
  } as Layer;
  const flatFill = all[flat] as Layer & { paint?: Record<string, unknown> };
  const flatOpacity = flatFill.paint?.['fill-opacity'] ?? 1;
  const out = all.map((layer, i) =>
    i === flat ? ({ ...flatFill, paint: { ...flatFill.paint, 'fill-opacity': from3dZoom(flatOpacity as number, 0) } } as Layer) : layer,
  );
  out.splice(firstLabel, 0, extrusion);
  return out;
};

const generateLayers = (themeName: string) => {
  const sourceName = 'vector'
  const theme = themes[themeName];
  const style = {
    version: 8,
    "name": "Immich Map",
    "id": `immich-map-${themeName}`,
    sources: {
      [sourceName]: {
        type: "vector",
        url: 'https://tiles.immich.cloud/v1.json',
      },
    },
    layers: with3dBuildings(layers(sourceName, theme, { lang:"en" }).map(withIconFallbacks), sourceName, theme),
    sprite: `https://static.immich.cloud/tiles/sprites/v4/${theme.iconStyle}`,
    glyphs: "https://static.immich.cloud/tiles/fonts/v2/{fontstack}/{range}.pbf",
  };

  return style;
}

const dist = path.resolve(__dirname, '../dist');
if (!fs.existsSync(dist)) {
  fs.mkdirSync(dist);
}

for (const theme of Object.keys(themes)) {
  const style = generateLayers(theme);
  validateStyle(theme, style);
  fs.writeFileSync(path.resolve(dist, `${theme}.json`), JSON.stringify(style, null, 2));
}
