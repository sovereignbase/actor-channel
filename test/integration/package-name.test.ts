import { decode, decodeMulti, encode } from '@msgpack/msgpack'
import { describe, expect, it, vi } from 'vitest'
import { ChannelBroker } from '../../src/index.js'
import type { ActorChannelPair, Context } from '../../src/types/index.js'

type TestContext = Context<string, { value: number }, string, string>
const message = (ctx: unknown): ArrayBuffer => {
  const data = encode(ctx)
  return data.buffer.slice(
    data.byteOffset,
    data.byteOffset + data.byteLength
  ) as ArrayBuffer
}

class TestChannel implements ActorChannelPair {
  readyState: number = WebSocket.OPEN
  send = vi.fn<(data: ArrayBuffer) => void>()
}

const decoded = (channel: TestChannel): TestContext[] =>
  channel.send.mock.calls.map(
    ([data]) => decodeMulti(data)[Symbol.iterator]().next().value as TestContext
  )

describe('ChannelBroker', () => {
  it('reports malformed messages and unauthorized RPC requests', () => {
    const broker = new ChannelBroker<string, unknown, string, string>()
    const channel = new TestChannel()
    const violations: Array<{
      violator: ActorChannelPair
      description: string
    }> = []
    const listener = (
      event: CustomEvent<{
        violator: ActorChannelPair
        description: string
      }>
    ) => {
      violations.push(event.detail)
    }
    broker.addEventListener('violation', listener)
    broker.handleMessage(channel, new Uint8Array([1]) as unknown as ArrayBuffer)
    broker.handleMessage(channel, new Uint8Array([0xc1]).buffer)
    broker.handleMessage(
      channel,
      message({ kind: 'request', id: '1', detail: 'x' })
    )
    broker.removeEventListener('violation', listener)
    broker.handleMessage(channel, new Uint8Array([0xc1]).buffer)
    expect(violations).toEqual([
      { violator: channel, description: 'Wrong message encoding.' },
      { violator: channel, description: 'Wrong message encoding.' },
      { violator: channel, description: 'Unauthorized.' },
    ])
  })

  it('dispatches authorized RPC and responds only while open', () => {
    const broker = new ChannelBroker<string, unknown, string, string>()
    const channel = new TestChannel()
    const requests: string[] = []
    broker.addChannel(channel, { rpcEnabled: true })
    broker.addEventListener('request', (event) => {
      const [request, respond] = event.detail
      requests.push(request)
      respond('ok')
      channel.readyState = WebSocket.CLOSED
      respond('ignored')
    })
    broker.handleMessage(
      channel,
      message({ kind: 'request', id: '1', detail: 'run' })
    )
    expect(requests).toEqual(['run'])
    expect(decoded(channel)).toEqual([
      { kind: 'response', id: '1', detail: 'ok' },
    ])
  })

  it('fans amounts and publications only to topic subscribers', () => {
    const broker = new ChannelBroker<
      string,
      { value: number },
      string,
      string
    >()
    const first = new TestChannel()
    const second = new TestChannel()
    const outsider = new TestChannel()
    broker.handleMessage(
      first,
      message({ kind: 'subscribe', topic: 'a', from: 'client' })
    )
    broker.handleMessage(
      second,
      message({ kind: 'subscribe', topic: 'a', from: 'client' })
    )
    broker.handleMessage(
      outsider,
      message({ kind: 'subscribe', topic: 'b', from: 'client' })
    )
    broker.handleMessage(
      first,
      message({
        kind: 'publish',
        topic: 'a',
        from: 'client',
        detail: { value: 1 },
      })
    )
    expect(decoded(first)).toEqual([
      { kind: 'subscribe', topic: 'a', from: 'server', amount: 1 },
      { kind: 'subscribe', topic: 'a', from: 'server', amount: 2 },
      { kind: 'publish', topic: 'a', from: 'server', detail: { value: 1 } },
    ])
    expect(decoded(second)).toEqual([
      { kind: 'subscribe', topic: 'a', from: 'server', amount: 2 },
      { kind: 'publish', topic: 'a', from: 'server', detail: { value: 1 } },
    ])
    expect(decoded(outsider)).toEqual([
      { kind: 'subscribe', topic: 'b', from: 'server', amount: 1 },
    ])
  })

  it('sends independently decodable protocol frames', () => {
    const broker = new ChannelBroker<string, unknown, string, string>()
    const channel = new TestChannel()
    broker.handleMessage(
      channel,
      message({ kind: 'subscribe', topic: 'a', from: 'client' })
    )
    expect(() => decode(channel.send.mock.calls[0]![0])).not.toThrow()
  })

  it('fans unsubscribe amounts and removes empty topics', () => {
    const broker = new ChannelBroker<string, unknown, string, string>()
    const first = new TestChannel()
    const second = new TestChannel()
    broker.handleMessage(
      first,
      message({ kind: 'subscribe', topic: 'a', from: 'client' })
    )
    broker.handleMessage(
      second,
      message({ kind: 'subscribe', topic: 'a', from: 'client' })
    )
    first.send.mockClear()
    second.send.mockClear()
    broker.handleMessage(
      first,
      message({ kind: 'unsubscribe', topic: 'a', from: 'client' })
    )
    broker.handleMessage(
      second,
      message({ kind: 'unsubscribe', topic: 'a', from: 'client' })
    )
    broker.deleteChannel(second)
    expect(decoded(first)).toEqual([
      { kind: 'unsubscribe', topic: 'a', from: 'server', amount: 1 },
    ])
    expect(decoded(second)).toEqual([
      { kind: 'unsubscribe', topic: 'a', from: 'server', amount: 1 },
      { kind: 'unsubscribe', topic: 'a', from: 'server', amount: 0 },
    ])
    expect((broker as any).topicSubscribers.size).toBe(0)
  })

  it('reports unknown topics and message kinds without sending frames', () => {
    const broker = new ChannelBroker<string, unknown, string, string>()
    const channel = new TestChannel()
    const violations: string[] = []
    broker.addEventListener('violation', (event) => {
      violations.push(event.detail.description)
    })
    broker.handleMessage(
      channel,
      message({
        kind: 'publish',
        topic: 'missing',
        from: 'client',
        detail: null,
      })
    )
    broker.handleMessage(
      channel,
      message({ kind: 'unsubscribe', topic: 'missing', from: 'client' })
    )
    broker.handleMessage(channel, message({ kind: 'unknown' }))
    broker.deleteChannel(channel)
    expect(channel.send).not.toHaveBeenCalled()
    expect(violations).toEqual(['Unauthorized.', 'Off protocol.'])
  })

  it('does not advertise a lower amount for a non-subscriber unsubscribe', () => {
    const broker = new ChannelBroker<string, unknown, string, string>()
    const subscriber = new TestChannel()
    const outsider = new TestChannel()
    broker.handleMessage(
      subscriber,
      message({ kind: 'subscribe', topic: 'a', from: 'client' })
    )
    subscriber.send.mockClear()
    broker.handleMessage(
      outsider,
      message({ kind: 'unsubscribe', topic: 'a', from: 'client' })
    )
    expect(decoded(subscriber)).toEqual([])
    expect((broker as any).topicSubscribers.get('a').size).toBe(1)
  })

  it('applies initial addChannel topics through the subscription protocol', () => {
    const broker = new ChannelBroker<string, unknown, string, string>()
    const channel = new TestChannel()
    broker.addChannel(channel, { topics: new Set(['a']) })
    expect(decoded(channel)).toEqual([
      { kind: 'subscribe', topic: 'a', from: 'server', amount: 1 },
    ])
  })

  it('keeps attachment topics synchronized with subscription messages', () => {
    const broker = new ChannelBroker<string, unknown, string, string>()
    const channel = new TestChannel()
    const attachment: { ipAddress: string; topics?: Set<string> } = {
      ipAddress: '192.0.2.1',
    }
    broker.addChannel(channel, attachment)

    expect(broker.channelAttachments.get(channel)).toBe(attachment)
    expect(attachment.topics).toBeUndefined()

    broker.handleMessage(
      channel,
      message({ kind: 'subscribe', topic: 'a', from: 'client' })
    )
    broker.handleMessage(
      channel,
      message({ kind: 'subscribe', topic: 'b', from: 'client' })
    )
    expect(attachment.topics).toEqual(new Set(['a', 'b']))

    broker.handleMessage(
      channel,
      message({ kind: 'unsubscribe', topic: 'a', from: 'client' })
    )
    expect(attachment.topics).toEqual(new Set(['b']))
  })

  it('throws when the same channel is added twice', () => {
    const broker = new ChannelBroker<string, unknown, string, string>()
    const channel = new TestChannel()
    const attachment = { ipAddress: '192.0.2.1' }
    broker.addChannel(channel, attachment)

    expect(() => broker.addChannel(channel)).toThrow(
      'addChannel MUST be used only once per channel.'
    )
    expect(broker.channelAttachments.get(channel)).toBe(attachment)
  })

  it('notifies subscribers when deleteChannel removes a channel', () => {
    const broker = new ChannelBroker<string, unknown, string, string>()
    const first = new TestChannel()
    const second = new TestChannel()
    broker.addChannel(first, { topics: new Set(['a']) })
    broker.addChannel(second, { topics: new Set(['a']) })
    first.send.mockClear()
    broker.deleteChannel(second)
    expect(decoded(first)).toEqual([
      { kind: 'unsubscribe', topic: 'a', from: 'server', amount: 1 },
    ])
    expect(broker.channelAttachments.has(second)).toBe(false)
  })

  it('cleans an empty topic and tolerates a direct unknown removal', () => {
    const broker = new ChannelBroker<string, unknown, string, string>()
    const channel = new TestChannel()
    broker.handleMessage(
      channel,
      message({ kind: 'subscribe', topic: 'a', from: 'client' })
    )
    broker.handleMessage(
      channel,
      message({ kind: 'unsubscribe', topic: 'a', from: 'client' })
    )
    ;(broker as any).unsubscribeTopic(channel, 'missing')
    broker.deleteChannel(channel)
    expect((broker as any).topicSubscribers.size).toBe(0)
  })
})
