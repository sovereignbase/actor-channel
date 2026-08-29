import { decode, encode } from '@msgpack/msgpack'
import { ChannelBroker } from '../../../dist/index.js'

const frame = (context) => encode(context).slice().buffer
const received = (channel) => channel.frames.map((data) => decode(data))
const channel = () => ({
  readyState: 1,
  frames: [],
  send(data) {
    this.frames.push(data)
  },
})

const broker = new ChannelBroker()
const publisher = channel()
const subscriber = channel()
const outsider = channel()

broker.addChannel(publisher, { topics: new Set(['topic']) })
broker.addChannel(subscriber, { topics: new Set(['topic']) })
broker.handleMessage(
  publisher,
  frame({ kind: 'publish', topic: 'topic', from: 'client', detail: 'hello' })
)

const publisherMessages = received(publisher)
const subscriberMessages = received(subscriber)

if (publisherMessages.at(-1)?.detail !== 'hello')
  throw new Error('Publisher did not receive the publication.')
if (subscriberMessages.at(-1)?.detail !== 'hello')
  throw new Error('Subscriber did not receive the publication.')
if (outsider.frames.length !== 0)
  throw new Error('Publication leaked outside the topic.')

globalThis.__actorChannelRuntimePassed = true
