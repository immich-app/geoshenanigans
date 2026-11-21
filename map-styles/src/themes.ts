import { BLACK, DARK, Flavor, LIGHT } from '@protomaps/basemaps';

export interface ImmichTheme extends Flavor {
  iconStyle: 'dark' | 'light' | 'black';
  // fill-extrusion-color of 3D buildings: lit by the style's default light, a roof comes out as today's flat
  // buildings fill (buildings at 0.5 opacity over earth).
  buildings3d: string;
}

export const IMMICH_LIGHT: ImmichTheme = {
  iconStyle: 'light',
  ...LIGHT,
  water: 'rgba(148, 209, 236, 0.66)',
  buildings3d: '#d0cecc',
};

export const IMMICH_DARK: ImmichTheme = {
  iconStyle: 'dark',
  ...DARK,
  background: BLACK.background,
  earth: BLACK.earth,
  landcover: {
    grassland: "#1F231F",
    barren: "#231F1F",
    urban_area: "#1C1C20",
    farmland: "#21231D",
    glacier: "#1D2123",
    scrub: "#1F1F1F",
    forest: "#1B211B",
  },
  water: "#02050F",
  buildings3d: '#050505',
};

export const IMMICH_BLACK: ImmichTheme = {
  iconStyle: 'dark',
  ...BLACK,
  buildings3d: '#020202',
};

const themes: Record<string, ImmichTheme> = {
  light: IMMICH_LIGHT,
  dark: IMMICH_DARK,
  black: IMMICH_BLACK,
};

export default themes;
