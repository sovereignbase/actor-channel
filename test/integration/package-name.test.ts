import { encode } from '@msgpack/msgpack'
import { describe, expect, it } from 'vitest'
import { decodeContext } from '../../src/index.js'

const buffer = (value: unknown): ArrayBuffer => {
  const encoded = encode(value)
  return encoded.buffer.slice(
    encoded.byteOffset,
    encoded.byteOffset + encoded.byteLength
  ) as ArrayBuffer
}

describe('decodeContext', () => {
  it.each([
    { kind: 'invoke', payload: null },
    { kind: 'offer', id: '1', payload: null },
    { kind: 'withdraw', id: '1' },
    { kind: 'answer', id: '1', payload: null },
    { kind: 'gossip', topic: 'news', from: 'client', payload: null },
    {
      kind: 'transact',
      id: '1',
      phase: 'request',
      payload: null,
    },
    { kind: 'subscribe', topic: 'news', from: 'server' },
    { kind: 'unsubscribe', topic: 'news', from: 'client' },
  ])('decodes and validates $kind', (ctx) => {
    expect(decodeContext(buffer(ctx))).toEqual(ctx)
  })

  it.each([
    null,
    [],
    {},
    { kind: 'unknown' },
    { kind: 'invoke' },
    { kind: 'offer', id: 1, payload: null },
    { kind: 'withdraw', id: null },
    { kind: 'answer', id: '1' },
    { kind: 'gossip', topic: 1, from: 'client', payload: null },
    { kind: 'gossip', topic: 'news', from: 'peer', payload: null },
    { kind: 'transact', id: '1', phase: 'pending', payload: null },
    { kind: 'subscribe', topic: 'news', from: 'peer' },
    { kind: 'unsubscribe', topic: null, from: 'client' },
  ])('rejects a value that is not a Context', (value) => {
    expect(decodeContext(buffer(value))).toBe(false)
  })

  it('rejects malformed MessagePack', () => {
    expect(decodeContext(new Uint8Array([0xc1]).buffer)).toBe(false)
  })
})
