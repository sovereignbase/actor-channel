import { decode, encode } from '@msgpack/msgpack'
import { ActorChannelError, ChannelBroker } from '../../../dist/index.js'
import type { ActorChannelPair, Context } from '../../../dist/index.js'

type RuntimeChannel = ActorChannelPair & { frames: ArrayBuffer[] }
type RuntimeContext = Context<string, string, string, string>
const frame = (context: unknown) => encode(context).slice().buffer
const received = (channel: RuntimeChannel) =>
  channel.frames.map((data) => decode(data) as RuntimeContext)
const channel = (): RuntimeChannel => ({
  readyState: 1,
  frames: [],
  send(data: ArrayBuffer) {
    this.frames.push(data)
  },
})

const broker = new ChannelBroker<string, string, string, string>()
const publisher = channel()
const subscriber = channel()
const outsider = channel()

broker.addChannel(publisher, { topics: new Set(['topic']) })
broker.addChannel(subscriber, { topics: new Set(['topic']) })
broker.addChannel(outsider)
broker.handleMessage(
  publisher,
  frame({ kind: 'publish', topic: 'topic', from: 'client', detail: 'hello' })
)

const publisherMessages = received(publisher)
const subscriberMessages = received(subscriber)

if ((publisherMessages.at(-1) as { detail?: string })?.detail !== 'hello')
  throw new Error('Publisher did not receive the publication.')
if ((subscriberMessages.at(-1) as { detail?: string })?.detail !== 'hello')
  throw new Error('Subscriber did not receive the publication.')
if (outsider.frames.length !== 0)
  throw new Error('Publication leaked outside the topic.')

let violation: string | undefined
broker.addEventListener('violation', (event) => {
  violation = event.detail.description
})
broker.handleMessage(outsider, new Uint8Array([0xc1]).buffer)
if (violation !== 'Wrong message encoding.')
  throw new Error('Malformed frame was not reported.')
broker.handleMessage(
  outsider,
  frame({ kind: 'request', id: 'denied', detail: 'request' })
)
if (String(violation) !== 'Unauthorized.')
  throw new Error('Unauthorized request was not rejected.')
const caller = channel()
broker.addChannel(caller, { rpcEnabled: true })
broker.addEventListener('request', (event) => {
  event.detail[1](`response:${event.detail[0]}`)
})
broker.handleMessage(
  caller,
  frame({ kind: 'request', id: 'rpc', detail: 'request' })
)
const reply = received(caller).at(-1)
if (
  reply?.kind !== 'response' ||
  reply.id !== 'rpc' ||
  reply.detail !== 'response:request'
)
  throw new Error('RPC response was not routed.')
broker.markAuthenticated(caller)
if (!broker.channelAttachments.get(caller)?.authenticated)
  throw new Error('Attachment was not updated.')
try {
  broker.addChannel(caller)
  throw new Error('Duplicate channel was accepted.')
} catch (error) {
  if (
    !(error instanceof ActorChannelError) ||
    error.code !== 'CHANNEL_ALREADY_ADDED'
  )
    throw error
}
broker.deleteChannel(subscriber)
subscriber.frames.length = 0
broker.handleMessage(
  publisher,
  frame({
    kind: 'publish',
    topic: 'topic',
    from: 'client',
    detail: 'after-delete',
  })
)
if (subscriber.frames.length !== 0)
  throw new Error('Deleted subscriber received a publication.')

;(
  globalThis as typeof globalThis & { __actorChannelRuntimePassed?: boolean }
).__actorChannelRuntimePassed = true
