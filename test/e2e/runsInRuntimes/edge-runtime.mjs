import { build } from 'esbuild'
import { EdgeRuntime } from 'edge-runtime'

const bundle = await build({
  entryPoints: ['test/e2e/runsInRuntimes/channel-broker-runtime.mjs'],
  bundle: true,
  format: 'iife',
  platform: 'browser',
  target: 'es2022',
  write: false,
})
const runtime = new EdgeRuntime({ initialCode: bundle.outputFiles[0].text })

if (runtime.context.__actorChannelRuntimePassed !== true)
  throw new Error('ChannelBroker did not complete in Edge Runtime.')
