import { defineConfig, devices } from '@playwright/test'

export default defineConfig({
  testDir: 'test/e2e/runsInBrowsers',
  testMatch: '**/*.spec.ts',
  timeout: 30_000,
  fullyParallel: true,
  forbidOnly: Boolean(process.env.CI),
  retries: process.env.CI ? 2 : 0,
  reporter: [
    ['list'],
    ['json', { outputFile: 'test/.results/browsers.json' }],
    ['html', { open: 'never' }],
  ],
  use: {
    baseURL: 'http://127.0.0.1:4173',
    trace: 'retain-on-failure',
  },
  webServer: {
    command: 'node test/e2e/runsInBrowsers/server.ts',
    url: 'http://127.0.0.1:4173',
    reuseExistingServer: !process.env.CI,
  },
  projects: [
    {
      name: 'chromium',
      use: { browserName: 'chromium' },
    },
    {
      name: 'firefox',
      use: { browserName: 'firefox' },
    },
    {
      name: 'webkit',
      use: { browserName: 'webkit' },
    },
    {
      name: 'mobile-chromium',
      use: { ...devices['Pixel 7'] },
    },
    {
      name: 'mobile-firefox',
      use: {
        browserName: 'firefox',
        viewport: { width: 390, height: 844 },
        hasTouch: true,
      },
    },
    {
      name: 'mobile-webkit',
      use: { ...devices['iPhone 15'] },
    },
  ],
})
