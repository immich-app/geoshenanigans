import themes from './themes';
import fs from 'fs';
import path from 'path';
import { layers } from '@protomaps/basemaps';

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
    layers: layers(sourceName, theme, { lang:"en" }).map(withIconFallbacks),
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
  fs.writeFileSync(path.resolve(dist, `${theme}.json`), JSON.stringify(generateLayers(theme), null, 2));
}
