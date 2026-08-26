[![npm version](https://img.shields.io/npm/v/@sovereignbase/package-name)](https://www.npmjs.com/package/@sovereignbase/package-name)
[![JSR](https://jsr.io/badges/@sovereignbase/package-name)](https://jsr.io/@sovereignbase/package-name)
[![CI](https://github.com/sovereignbase/package-name/actions/workflows/ci.yaml/badge.svg?branch=master)](https://github.com/sovereignbase/package-name/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/sovereignbase/package-name/branch/master/graph/badge.svg)](https://codecov.io/gh/sovereignbase/package-name)
[![license](https://img.shields.io/npm/l/@sovereignbase/package-name)](LICENSE)

# package-name

## Installation

## Usage

## API

## Behavior

`invoke`, `gossip`, `subscribe`, and `unsubscribe` return whether the operation
was accepted while the shared upstream connection was online. A `true` value is
not a server acknowledgement.

Pending transactions are rejected with a `NetworkError` when the upstream
connection changes and are not replayed automatically. The server may already
have processed a transaction whose response was lost, so mutating operations
should be idempotent.

The client emits `online` when the shared WebSocket opens and `offline` when
that connection is lost.

## Tests

## License
