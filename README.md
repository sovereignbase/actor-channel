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
  const [request, respond, sender] = event.detail

  if (!request.method) {
    broker.dispatchEvent('violation', {
      violator: sender,
      description: 'Off protocol.',
    })
    return
  }

  respond({ result: request.method })
})

broker.addChannel(socket, { rpcEnabled: true })

// Verify application-level identity using your own protocol and policy.
// Transport authentication is handled separately by HTTPS/WebSocket.
broker.markAuthenticated(socket)

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
- `emptyBrokers()` closes this instance's open and connecting broker sockets,
  cancels their reconnection loops and releases their Web Locks. Local
  subscriptions, listeners and the broadcast channel remain active. Brokers
  owned by other instances are unaffected, and new brokers can be added afterward.

### `ChannelBroker`

- `addChannel(channel, attachment?)` adds a transport with optional metadata,
  RPC access, and initial subscriptions.
- `deleteChannel(channel)` removes a transport and its subscriptions.
- `markAuthenticated(channel)` records that the host application has verified
  the channel's application-level identity and emits an `attachment` event.
  The method is intentionally one-way and provides no unset operation.
- `handleMessage(channel, message)` handles an encoded client message.
- `dispatchEvent(type, detail)` dispatches a typed broker event to registered
  listeners.
- `attachment` events receive the channel and its updated attachment.
- `request` events receive the request, a response callback, and the sender.
- `violation` events receive the violating channel and a description.

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
- `authenticated` does not describe HTTPS/WebSocket transport authentication.
  The host sets it through `markAuthenticated(channel)` after verifying an
  application-level identity with its own protocol, cryptosuite, and policy.
- `ChannelBroker` uses standard web platform primitives and does not require a
  specific server framework.

## Errors

`ActorChannelError` exposes a stable `code`, a package-prefixed message and an
optional original `cause`. It is exported with the `ActorChannelErrorCode` type.

| Code                    | Meaning                                                                                 |
| ----------------------- | --------------------------------------------------------------------------------------- |
| `CHANNEL_ALREADY_ADDED` | The same transport was added to a broker twice.                                         |
| `UNKNOWN_ERROR`         | An unexpected channel or broker operation failed; `cause` retains the original failure. |

Operational failures in channel construction, requests, publication,
subscription changes, broker cleanup and broker message handling are wrapped
once. Broker connection attempts remain best-effort: unavailable Web Locks,
socket construction failures and aborted connection loops do not reject
`addBroker()`. Invalid broker frames are ignored; invalid client frames and
unauthorized requests produce broker `violation` events. Application event
listeners follow the platform's `EventTarget` error behavior.

## Tests

Vitest: 54 passed, 0 failed, 0 skipped, 0 todo. Result: passed.

### Coverage

| Statements | Branches | Functions | Lines |
| ---------- | -------- | --------- | ----- |
| 100%       | 100%     | 100%      | 100%  |

### Browsers

| Browser                             | Result | Passed | Failed | Flaky | Skipped |
| ----------------------------------- | ------ | ------ | ------ | ----- | ------- |
| Chromium                            | Passed | 4      | 0      | 0     | 0       |
| Firefox                             | Failed | 0      | 4      | 0     | 0       |
| WebKit                              | Passed | 4      | 0      | 0     | 0       |
| Mobile Chromium (Pixel 7 emulation) | Passed | 4      | 0      | 0     | 0       |
| Mobile Firefox (touch viewport)     | Failed | 0      | 4      | 0     | 0       |
| Mobile WebKit (iPhone 15 emulation) | Passed | 4      | 0      | 0     | 0       |

Browser suite: failed.

### Broker runtimes

| Runtime                      | Version                                              | Result |
| ---------------------------- | ---------------------------------------------------- | ------ |
| Node.js                      | v24.16.0                                             | Passed |
| Bun                          | 1.3.10                                               | Passed |
| Deno                         | deno 2.8.3 (stable, release, x86_64-pc-windows-msvc) | Passed |
| Vercel Edge Runtime          | 4.0.1                                                | Passed |
| Cloudflare Workers (workerd) | 1.20261023.1                                         | Passed |

Results are updated automatically after each test run. Browser tests use native Broadcast Channel, Web Locks and WebSocket APIs with a local broker. Mobile projects use emulation; runtime tests cover `ChannelBroker`. “Not run” describes missing test results, not lack of platform support.

## License

Apache-2.0
