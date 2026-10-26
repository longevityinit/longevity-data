export default [
  {
    files: [
      'src/charts/headline*.js',
      'tests/js/**/*.js',
      'tests/browser/**/*.js',
      'playwright.config.js',
    ],
    languageOptions: {
      ecmaVersion: 'latest',
      sourceType: 'module',
      globals: {
        window: 'readonly',
        document: 'readonly',
        navigator: 'readonly',
        location: 'readonly',
        localStorage: 'readonly',
        history: 'readonly',
        fetch: 'readonly',
        URL: 'readonly',
        URLSearchParams: 'readonly',
        Intl: 'readonly',
        ResizeObserver: 'readonly',
        requestAnimationFrame: 'readonly',
        process: 'readonly',
        console: 'readonly',
      },
    },
    rules: {
      'no-undef': 'error',
      'no-unused-vars': ['error', { argsIgnorePattern: '^_', caughtErrorsIgnorePattern: '^_' }],
      'no-unreachable': 'error',
      'no-constant-condition': 'error',
      eqeqeq: ['error', 'always', { null: 'ignore' }],
    },
  },
];
