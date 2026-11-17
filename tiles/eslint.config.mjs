import js from '@eslint/js';
import typescriptEslint from '@typescript-eslint/eslint-plugin';
import tsParser from '@typescript-eslint/parser';
import { importX } from 'eslint-plugin-import-x';
import { defineConfig } from 'eslint/config';

export default defineConfig([
  {
    extends: [
      js.configs.recommended,
      typescriptEslint.configs['flat/recommended'],
      importX.flatConfigs.recommended,
      importX.flatConfigs.typescript,
    ],

    languageOptions: {
      parser: tsParser,
      sourceType: 'module',

      parserOptions: {
        project: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },

    rules: {
      curly: 2,
      '@typescript-eslint/no-floating-promises': 'error',
      '@typescript-eslint/no-explicit-any': 'off',
      'preserve-caught-error': 'off',
      'import-x/no-unresolved': ['error', { ignore: ['^cloudflare:'] }],
    },
  },
]);
