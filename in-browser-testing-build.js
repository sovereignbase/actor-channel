import { build } from 'esbuild'

await Promise.all([
  build({
    entryPoints: ['./in-browser-testing-libs.ts'],
    outfile: './index.js',
    bundle: true,
    external: [],
    platform: 'browser',
    format: 'esm',
  }),
])
