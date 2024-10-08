import { BLACK, LIGHT, Theme } from '../../submodules/basemaps/styles/src/themes';

export interface ImmichTheme extends Theme {
  iconStyle: 'dark' | 'light';
}

export const IMMICH_LIGHT: ImmichTheme = {
  iconStyle: 'light',
  ...LIGHT,
  water: 'rgba(148, 209, 236, 0.66)'
};

export const IMMICH_DARK: ImmichTheme = {
  iconStyle: 'dark',
  ...BLACK,
};

const themes: Record<string, ImmichTheme> = {
  light: IMMICH_LIGHT,
  dark: IMMICH_DARK,
};

export default themes;
