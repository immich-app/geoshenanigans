import { BLACK, DARK, Flavor, LIGHT } from '@protomaps/basemaps';

export interface ImmichTheme extends Flavor {
  iconStyle: 'dark' | 'light' | 'black';
}

export const IMMICH_LIGHT: ImmichTheme = {
  iconStyle: 'light',
  ...LIGHT,
  water: 'rgba(148, 209, 236, 0.66)'
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
};

export const IMMICH_BLACK: ImmichTheme = {
  iconStyle: 'dark',
  ...BLACK,
};

const themes: Record<string, ImmichTheme> = {
  light: IMMICH_LIGHT,
  dark: IMMICH_DARK,
  black: IMMICH_BLACK,
};

export default themes;
