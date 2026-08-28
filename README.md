[![npm version](https://img.shields.io/npm/v/@sovereignbase/actor-channel)](https://www.npmjs.com/package/@sovereignbase/actor-channel)
[![JSR](https://jsr.io/badges/@sovereignbase/actor-channel)](https://jsr.io/@sovereignbase/actor-channel)
[![CI](https://github.com/sovereignbase/actor-channel/actions/workflows/ci.yaml/badge.svg?branch=master)](https://github.com/sovereignbase/actor-channel/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/sovereignbase/actor-channel/branch/master/graph/badge.svg)](https://codecov.io/gh/sovereignbase/actor-channel)
[![license](https://img.shields.io/npm/l/@sovereignbase/actor-channel)](LICENSE)

# actor-channel

Bidirectional actor channels with pub/sub, peer relaying, and RPC.

## Installation

```sh
npm install @sovereignbase/actor-channel
# or
pnpm add @sovereignbase/actor-channel
# or
yarn add @sovereignbase/actor-channel
# or
bun add @sovereignbase/actor-channel
# or
deno add jsr:@sovereignbase/actor-channel
# or
vlt install jsr:@sovereignbase/actor-channel
```

## Usage

### Actor channel

```ts
import { ActorChannel } from '@sovereignbase/actor-channel'

const channel = new ActorChannel<
  'documents',
  { id: string },
  { method: string },
  { result: string }
>()

channel.addEventListener('message', (event) => {
  const [topic, message] = event.detail
  console.log(topic, message)
})

channel.addEventListener('response', (event) => {
  const [id, response] = event.detail
  console.log(id, response)
})

channel.subscribe('documents')
channel.publish('documents', { id: 'document-1' })

const requestId = channel.request({ method: 'sync' })
console.log(requestId)

void channel.addBroker('wss://broker.example', true)
```

### Broker

```ts
import { ChannelBroker } from '@sovereignbase/actor-channel'

const broker = new ChannelBroker<
  'documents',
  { id: string },
  { method: string },
  { result: string }
>()

broker.addEventListener('request', (event) => {
  const [request, respond] = event.detail
  respond({ result: request.method })
})

broker.addChannel(socket, true)

socket.addEventListener('message', (event) => {
  broker.handleMessage(socket, event.data)
})

socket.addEventListener('close', () => {
  broker.deleteChannel(socket)
})
```

The transport passed to `ChannelBroker` must expose `send(ArrayBuffer)` and
`readyState`. Incoming messages must be provided as `ArrayBuffer` values.

## API

### `ActorChannel`

- `rpcAvailable` reports whether an RPC-enabled broker is available.
- `request(detail)` sends a fire-and-forget RPC request and returns its ID.
- `publish(topic, detail)` publishes locally and to subscribed broker peers.
- `subscribe(topic)` subscribes the current window to a topic.
- `unsubscribe(topic)` removes the current window's subscription.
- `addBroker(url, rpcEnabled?)` repeatedly attempts to acquire a URL-specific
  Web Lock and maintains the WebSocket while it owns the lock.

### `ChannelBroker`

- `addChannel(channel, rpcEnabled?, topics?)` adds a transport and optional
  initial subscriptions.
- `deleteChannel(channel)` removes a transport and its subscriptions.
- `handleMessage(channel, message)` handles an encoded client message.
- `request` events receive the request and a response callback.
- `violation` events receive a protocol violation description.

## Behavior

- `ActorChannel` is window-only and requires Broadcast Channel, Web Locks, Web
  Crypto, and WebSocket APIs.
- Same-origin tabs share topic subscription counts and elect at most one
  connection to each broker URL.
- Broker connections are retried every 10 seconds after closing or when their
  Web Lock is unavailable.
- Topic subscriptions are forwarded to brokers on the first local subscription
  and removed after the final local unsubscription.
- Requests are not queued or replayed when no RPC-enabled connection is open.
- `rpcAvailable` is `true` when the channel knows of at least one open
  RPC-enabled broker WebSocket.
- Protocol messages are MessagePack-encoded `ArrayBuffer` values.
- `ChannelBroker` uses standard web platform primitives and does not require a
  specific server framework.

## Tests

- Unit and integration tests in Vitest with 100% statement, branch, function,
  and line coverage.
- Browser E2E tests in Chromium, Firefox, WebKit, Mobile Chromium, Mobile
  Firefox, and Mobile WebKit.
- `ChannelBroker` runtime tests in Node.js, Bun, Deno, Vercel Edge Runtime, and
  Cloudflare Workers (`workerd`).

## License

Apache-2.0
