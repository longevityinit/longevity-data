import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests/browser',
  fullyParallel: true,
  workers: process.env.CI ? 1 : undefined,
  globalTimeout: 5 * 60 * 1000,
  use: { baseURL: 'http://127.0.0.1:8765', headless: true, viewport: { width: 1280, height: 900 } },
  webServer: {
    command: `${process.env.PYTHON || 'python'} tests/browser/serve.py`,
    url: 'http://127.0.0.1:8765',
    reuseExistingServer: false,
  },
});
