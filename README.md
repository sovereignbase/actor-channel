[![npm version](https://img.shields.io/npm/v/@sovereignbase/actor-socket)](https://www.npmjs.com/package/@sovereignbase/actor-socket)
[![JSR](https://jsr.io/badges/@sovereignbase/actor-socket)](https://jsr.io/@sovereignbase/actor-socket)
[![CI](https://github.com/sovereignbase/actor-socket/actions/workflows/ci.yaml/badge.svg?branch=master)](https://github.com/sovereignbase/actor-socket/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/sovereignbase/actor-socket/branch/master/graph/badge.svg)](https://codecov.io/gh/sovereignbase/actor-socket)
[![license](https://img.shields.io/npm/l/@sovereignbase/actor-socket)](LICENSE)

# actor-socket

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
