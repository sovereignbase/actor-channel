import { expect, test } from 'vitest'
import type { JSONReport } from '@playwright/test/reporter'
import { renderTests, replaceTests } from '../../scripts/test-report.ts'

test('missing results never become compatibility or coverage claims', () => {
  const body = renderTests({})
  expect(body).toContain('Vitest: not run.')
  expect(body).toContain('Coverage was not collected')
  expect(body).toContain('| Chromium | Not run | 0 | 0 | 0 | 0 |')
  expect(body).toContain('| Node.js | — | Not run |')
})

test('a coverage failure overrides passing assertions and runner failures remain visible', () => {
  const body = renderTests(
    {
      coverage: { code: 1 },
      browsers: { code: 1 },
      node: { code: 1, version: 'v24' },
    },
    {
      success: true,
      numPassedTests: 3,
      numFailedTests: 0,
      numPendingTests: 0,
      numTodoTests: 0,
    }
  )
  expect(body).toContain(
    '3 passed, 0 failed, 0 skipped, 0 todo. Result: failed.'
  )
  expect(body).toContain('Browser suite: failed.')
  expect(body).toContain('| Node.js | v24 | Failed |')
})

test('nested browser results distinguish failures, retries, skipped and filtered projects', () => {
  const report = {
    errors: [{ message: 'Runner error' }],
    suites: [
      {
        specs: [],
        suites: [
          {
            specs: [
              {
                tests: [
                  { projectName: 'chromium', status: 'expected' },
                  { projectName: 'chromium', status: 'flaky' },
                  { projectName: 'firefox', status: 'unexpected' },
                  { projectName: 'webkit', status: 'skipped' },
                ],
              },
            ],
          },
        ],
      },
    ],
  } as unknown as Pick<JSONReport, 'suites' | 'errors'>
  const body = renderTests({}, undefined, undefined, report)
  expect(body).toContain('| Chromium | Flaky | 1 | 0 | 1 | 0 |')
  expect(body).toContain('| Firefox | Failed | 0 | 1 | 0 | 0 |')
  expect(body).toContain('| WebKit | Incomplete | 0 | 0 | 0 | 1 |')
  expect(body).toContain('| Mobile Chromium (Pixel 7 emulation) | Not run |')
  expect(body).toContain('Browser runner errors: 1.')
})

test('README updates preserve surrounding sections and line endings', () => {
  const original =
    '# Package\r\n\r\n## Tests\r\n\r\nOld\r\n\r\n## License\r\n\r\nApache-2.0\r\n'
  expect(replaceTests(original, 'New\n\n| Test | Result |')).toBe(
    '# Package\r\n\r\n## Tests\r\n\r\nNew\r\n\r\n| Test | Result |\r\n\r\n## License\r\n\r\nApache-2.0\r\n'
  )
  expect(() => replaceTests('# Package\n', 'Results')).toThrow(
    'Missing README section'
  )
})
