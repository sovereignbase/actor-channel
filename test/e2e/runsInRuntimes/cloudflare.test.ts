import './channel-broker-runtime.ts'
import { expect, test } from 'vitest'

test('runs ChannelBroker in Cloudflare Workers', () => {
  expect(
    (
      globalThis as typeof globalThis & {
        __actorChannelRuntimePassed?: boolean
      }
    ).__actorChannelRuntimePassed
  ).toBe(true)
})
