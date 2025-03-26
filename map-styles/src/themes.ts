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
};

export const IMMICH_BLACK: ImmichTheme = {
  iconStyle: 'black',
  ...BLACK,
};

const themes: Record<string, ImmichTheme> = {
  light: IMMICH_LIGHT,
  dark: IMMICH_DARK,
  black: IMMICH_BLACK,
};

export default themes;
