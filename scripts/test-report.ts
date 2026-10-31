import { existsSync, readFileSync, writeFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { format, resolveConfig } from 'prettier'
import type {
  JSONReport,
  JSONReportSuite,
  JSONReportTest,
} from '@playwright/test/reporter'

export const paths = {
  readme: fileURLToPath(new URL('../README.md', import.meta.url)),
  coverage: fileURLToPath(
    new URL('../coverage/coverage-summary.json', import.meta.url)
  ),
  vitest: fileURLToPath(
    new URL('../test/.results/vitest.json', import.meta.url)
  ),
  browsers: fileURLToPath(
    new URL('../test/.results/browsers.json', import.meta.url)
  ),
  status: fileURLToPath(
    new URL('../test/.results/status.json', import.meta.url)
  ),
}

export type Suite =
  'coverage' | 'browsers' | 'node' | 'bun' | 'deno' | 'edge' | 'cloudflare'
export type Status = Partial<Record<Suite, { code: number; version?: string }>>
type VitestReport = {
  numPassedTests: number
  numFailedTests: number
  numPendingTests: number
  numTodoTests: number
  success: boolean
}
const metrics = ['statements', 'branches', 'functions', 'lines'] as const
type Coverage = { total: Record<(typeof metrics)[number], { pct: number }> }
const browsers = [
  ['chromium', 'Chromium'],
  ['firefox', 'Firefox'],
  ['webkit', 'WebKit'],
  ['mobile-chromium', 'Mobile Chromium (Pixel 7 emulation)'],
  ['mobile-firefox', 'Mobile Firefox (touch viewport)'],
  ['mobile-webkit', 'Mobile WebKit (iPhone 15 emulation)'],
] as const
const runtimes = [
  ['node', 'Node.js'],
  ['bun', 'Bun'],
  ['deno', 'Deno'],
  ['edge', 'Vercel Edge Runtime'],
  ['cloudflare', 'Cloudflare Workers (workerd)'],
] as const

export function readReport<T>(path: string): T | undefined {
  return existsSync(path)
    ? (JSON.parse(readFileSync(path, 'utf8')) as T)
    : undefined
}

function table(rows: string[][]): string {
  return rows
    .flatMap((row, index) => [
      `| ${row.join(' | ')} |`,
      ...(index === 0 ? [`| ${row.map(() => '---').join(' | ')} |`] : []),
    ])
    .join('\n')
}

function browserTests(suites: JSONReportSuite[]): JSONReportTest[] {
  return suites.flatMap((suite) => [
    ...suite.specs.flatMap((spec) => spec.tests),
    ...browserTests(suite.suites ?? []),
  ])
}

/** Only tool-generated results are rendered; missing suites never imply support. */
export function renderTests(
  status: Status,
  vitest?: VitestReport,
  coverage?: Coverage,
  report?: Pick<JSONReport, 'suites' | 'errors'>
): string {
  const tests = browserTests(report?.suites ?? [])
  return [
    vitest
      ? `Vitest: ${vitest.numPassedTests} passed, ${vitest.numFailedTests} failed, ${vitest.numPendingTests} skipped, ${vitest.numTodoTests} todo. Result: ${vitest.success && status.coverage?.code === 0 ? 'passed' : 'failed'}.`
      : `Vitest: ${status.coverage ? 'runner failed before results were available' : 'not run'}.`,
    '',
    '### Coverage',
    '',
    coverage
      ? table([
          ['Statements', 'Branches', 'Functions', 'Lines'],
          metrics.map((metric) => `${coverage.total[metric].pct}%`),
        ])
      : 'Coverage was not collected in the latest Vitest run.',
    '',
    '### Browsers',
    '',
    table([
      ['Browser', 'Result', 'Passed', 'Failed', 'Flaky', 'Skipped'],
      ...browsers.map(([name, label]) => {
        const results = tests.filter((test) => test.projectName === name)
        const counts = ['expected', 'unexpected', 'flaky', 'skipped'].map(
          (state) => results.filter((test) => test.status === state).length
        )
        const result = !results.length
          ? 'Not run'
          : counts[1]
            ? 'Failed'
            : counts[2]
              ? 'Flaky'
              : counts[3]
                ? 'Incomplete'
                : 'Passed'
        return [label, result, ...counts.map(String)]
      }),
    ]),
    ...(report?.errors.length
      ? ['', `Browser runner errors: ${report.errors.length}.`]
      : []),
    ...(status.browsers?.code ? ['', 'Browser suite: failed.'] : []),
    '',
    '### Broker runtimes',
    '',
    table([
      ['Runtime', 'Version', 'Result'],
      ...runtimes.map(([name, label]) => {
        const result = status[name]
        return [
          label,
          result?.version ?? '—',
          result ? (result.code === 0 ? 'Passed' : 'Failed') : 'Not run',
        ]
      }),
    ]),
    '',
    'Results are updated automatically after each test run. Browser tests use native Broadcast Channel, Web Locks and WebSocket APIs with a local broker. Mobile projects use emulation; runtime tests cover `ChannelBroker`. “Not run” describes missing test results, not lack of platform support.',
  ].join('\n')
}

export function replaceTests(source: string, body: string): string {
  const heading = /^## Tests[ \t]*\r?\n/m.exec(source)
  if (!heading) throw new Error('Missing README section: Tests')
  const start = heading.index + heading[0].length
  const next = /^## /m.exec(source.slice(start))
  const end = next ? start + next.index : source.length
  const newline = source.includes('\r\n') ? '\r\n' : '\n'
  return (
    source.slice(0, start) +
    newline +
    body.trim().replace(/\r?\n/g, newline) +
    newline +
    newline +
    source.slice(end)
  )
}

export async function updateReadme(status: Status): Promise<void> {
  const body = renderTests(
    status,
    readReport(paths.vitest),
    readReport(paths.coverage),
    readReport(paths.browsers)
  )
  const formatted = await format(body, {
    ...(await resolveConfig(paths.readme)),
    parser: 'markdown',
  })
  const original = readFileSync(paths.readme, 'utf8')
  const updated = replaceTests(original, formatted)
  if (original !== updated) writeFileSync(paths.readme, updated)
}
