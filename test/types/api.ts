import {
  ActorChannel,
  ActorChannelError,
  ChannelBroker,
} from '@sovereignbase/actor-channel'
import type {
  ActorChannelErrorCode,
  ActorChannelPair,
} from '@sovereignbase/actor-channel'

declare const channel: ActorChannel<
  'documents',
  { id: string },
  { method: string },
  { result: string }
>
declare const broker: ChannelBroker<
  'documents',
  { id: string },
  { method: string },
  { result: string }
>
declare const socket: ActorChannelPair

const cleanup: void = channel.emptyBrokers()
const maintenance: Promise<void> = channel.addBroker('wss://example.test', true)
const requestId: string = channel.request({ method: 'sync' })
channel.publish('documents', { id: 'one' })
channel.addEventListener('message', (event) => {
  const topic: 'documents' = event.detail[0]
  const message: { id: string } = event.detail[1]
  void [topic, message]
})
broker.addChannel(socket, { topics: new Set(['documents']), rpcEnabled: true })
broker.addEventListener('request', (event) => {
  const method: string = event.detail[0].method
  event.detail[1]({ result: method })
  // @ts-expect-error Response payload must match RPCResponse.
  event.detail[1]({ id: 'wrong' })
})
const code: ActorChannelErrorCode = new ActorChannelError(
  'CHANNEL_ALREADY_ADDED'
).code
// @ts-expect-error Topic names are constrained by the channel type.
channel.subscribe('unknown')
// @ts-expect-error Request payload must match RPCRequest.
channel.request({ id: 'wrong' })
// @ts-expect-error Only documented error codes are accepted.
new ActorChannelError('UNDEFINED_CODE')
void [cleanup, maintenance, requestId, code]
