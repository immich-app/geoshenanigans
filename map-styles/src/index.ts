import themes from './themes';
import fs from 'fs';
import path from 'path';
import { layers } from '@protomaps/basemaps';

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
    layers: layers(sourceName, theme, { lang:"en" }),
    sprite: `https://static.immich.cloud/tiles/sprites/v2/${theme.iconStyle}`,
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
