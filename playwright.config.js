import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests/browser',
  fullyParallel: true,
  use: { baseURL: 'http://127.0.0.1:8765', headless: true, viewport: { width: 1280, height: 900 } },
  webServer: {
    command: `${process.env.PYTHON || 'python'} tests/browser/serve.py`,
    url: 'http://127.0.0.1:8765',
    reuseExistingServer: false,
  },
});
