[![npm version](https://img.shields.io/npm/v/@sovereignbase/actor-channel)](https://www.npmjs.com/package/@sovereignbase/actor-channel)
[![JSR](https://jsr.io/badges/@sovereignbase/actor-channel)](https://jsr.io/@sovereignbase/actor-channel)
[![CI](https://github.com/sovereignbase/actor-channel/actions/workflows/ci.yaml/badge.svg?branch=master)](https://github.com/sovereignbase/actor-channel/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/sovereignbase/actor-channel/branch/master/graph/badge.svg)](https://codecov.io/gh/sovereignbase/actor-channel)
[![license](https://img.shields.io/npm/l/@sovereignbase/actor-channel)](LICENSE)

# actor-channel

Client ▣ ActorChannel ⇄ ChannelBroker ☷ Server

## Installation

## Usage

## API

## Behavior

`invoke`, `gossip`, `subscribe`, and `unsubscribe` return whether the operation
was accepted while the shared upstream connection was online. A `true` value is
not a server acknowledgement.

Pending requests are rejected with a `NetworkError` when the upstream
connection changes and are not replayed automatically. The server may already
have processed a request whose response was lost, so mutating operations
should be idempotent.

The client emits `online` when the shared WebSocket opens and `offline` when
that connection is lost.

## Tests

## License
