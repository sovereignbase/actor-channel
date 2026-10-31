import { spawnSync } from 'node:child_process'
import { createRequire } from 'node:module'
import { mkdirSync, rmSync, writeFileSync } from 'node:fs'
import { dirname } from 'node:path'
import { paths, readReport, updateReadme } from './test-report.ts'
import type { Status, Suite } from './test-report.ts'

const root = new URL('../', import.meta.url)
const require = createRequire(import.meta.url)
const node = process.execPath
const runtimeFile = 'test/e2e/runsInRuntimes/channel-broker-runtime.ts'
const commands: Record<Suite, string[]> = {
  coverage: [node, 'node_modules/vitest/vitest.mjs', 'run', '--coverage'],
  browsers: [node, 'node_modules/@playwright/test/cli.js', 'test'],
  node: [node, runtimeFile],
  bun: ['bun', runtimeFile],
  deno: ['deno', 'run', '--allow-read', runtimeFile],
  edge: [node, 'test/e2e/runsInRuntimes/edge-runtime.ts'],
  cloudflare: [
    node,
    'node_modules/vitest/vitest.mjs',
    'run',
    '--config',
    'test/e2e/runsInRuntimes/vitest.cloudflare.config.ts',
  ],
}
const [selection = 'all', ...args] = process.argv.slice(2)
const runtimes: Suite[] = ['node', 'bun', 'deno', 'edge', 'cloudflare']
const suites: Suite[] =
  selection === 'all'
    ? ['coverage', 'browsers', ...runtimes]
    : selection === 'e2e'
      ? ['browsers', ...runtimes]
      : selection === 'runtimes'
        ? runtimes
        : selection in commands
          ? [selection as Suite]
          : []
if (!suites.length) throw new Error(`Unknown test suite: ${selection}`)

mkdirSync(dirname(paths.status), { recursive: true })
const status: Status = readReport(paths.status) ?? {}
// Clear all selected reports before starting so interrupted runs cannot reuse old successes.
for (const suite of suites) {
  status[suite] = { code: 1 }
  if (suite === 'coverage') {
    rmSync(paths.vitest, { force: true })
    rmSync(paths.coverage, { force: true })
  }
  if (suite === 'browsers') rmSync(paths.browsers, { force: true })
}
writeFileSync(paths.status, JSON.stringify(status, null, 2) + '\n')
let exitCode = 0
try {
  for (const suite of suites) {
    const [command, ...commandArgs] = commands[suite]
    const result = spawnSync(
      command,
      [...commandArgs, ...(suites.length === 1 ? args : [])],
      {
        cwd: root,
        stdio: 'inherit',
        windowsHide: true,
      }
    )
    if (result.error) console.error(result.error.message)
    const code = result.status ?? 1
    let version: string | undefined
    if (['node', 'bun', 'deno'].includes(suite)) {
      const output = spawnSync(command, ['--version'], {
        encoding: 'utf8',
        windowsHide: true,
      })
      version = output.stdout?.trim().split(/\r?\n/)[0]
    } else if (suite === 'edge' || suite === 'cloudflare') {
      version = (
        require(
          `${suite === 'edge' ? 'edge-runtime' : 'workerd'}/package.json`
        ) as { version: string }
      ).version
    }
    status[suite] = { code, ...(version ? { version } : {}) }
    exitCode ||= code
    writeFileSync(paths.status, JSON.stringify(status, null, 2) + '\n')
  }
} finally {
  await updateReadme(status)
}
process.exitCode = exitCode
