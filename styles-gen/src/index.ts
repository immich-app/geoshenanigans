import themes, { ImmichTheme } from './themes';
import fs from 'fs';
import path from 'path';
import { layersWithCustomTheme } from '../../submodules/basemaps/styles/src';

const generateLayers = (themeName: string) => {
  const sourceName = 'protomaps'
  const theme = themes[themeName];
  const layers = layersWithCustomTheme(sourceName, theme);
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
    layers: layers,
    sprite: `https://static.immich.cloud/tiles/sprites/v1/${theme.iconStyle}`,
    glyphs: "https://static.immich.cloud/tiles/fonts/{fontstack}/{range}.pbf",
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
