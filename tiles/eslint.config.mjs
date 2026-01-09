import { fixupPluginRules } from '@eslint/compat';
import { FlatCompat } from '@eslint/eslintrc';
import js from '@eslint/js';
import typescriptEslint from '@typescript-eslint/eslint-plugin';
import tsParser from '@typescript-eslint/parser';
import _import from 'eslint-plugin-import';
import { defineConfig } from 'eslint/config';

const compat = new FlatCompat({
  baseDirectory: import.meta.dirname,
  recommendedConfig: js.configs.recommended,
  allConfig: js.configs.all,
});

export default defineConfig([
  {
    extends: compat.extends('eslint:recommended', 'plugin:@typescript-eslint/recommended'),

    languageOptions: {
      parser: tsParser,
      sourceType: 'module',

      parserOptions: {
        project: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },

    plugins: {
      '@typescript-eslint': typescriptEslint,
      import: fixupPluginRules(_import),
    },

    rules: {
      curly: 2,
      '@typescript-eslint/no-floating-promises': 'error',
      '@typescript-eslint/no-explicit-any': 'off',
    },
  },
]);
