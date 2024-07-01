// @ts-check

import eslint from '@eslint/js';
import tseslint from 'typescript-eslint';
import mochaPlugin from 'eslint-plugin-mocha';
import globals from 'globals';

export default tseslint.config(
  eslint.configs.recommended,
  mochaPlugin.configs.flat.recommended,
  ...tseslint.configs.strict,
  ...tseslint.configs.stylistic,
  {
    rules: {
      // TODO: Fix/ignore in tests and remove
      '@typescript-eslint/no-loss-of-precision': 'warn',
      // TODO: Fix and remove
      '@typescript-eslint/prefer-for-of': 'warn',
      // Change back to an error (by removing) once we can
      '@typescript-eslint/no-explicit-any': 'warn',
      // Change back to an error (by removing) once we can
      '@typescript-eslint/no-non-null-assertion': 'warn',
      // Enable if we remove all cjs files
      '@typescript-eslint/no-var-requires': 'off',
      'mocha/max-top-level-suites': 'off',
      '@typescript-eslint/no-unused-vars': [
        'error',
        {
          argsIgnorePattern: '^_',
          varsIgnorePattern: '^_',
          caughtErrorsIgnorePattern: '^_',
        },
      ],
    },
  },
  {
    languageOptions: {
      globals: {
        ...globals.node,
      },
    },
  },
  {
    ignores: ['gen-nodejs/*', 'dist/**/*'],
  }
);
