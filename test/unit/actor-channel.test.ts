import { decode, decodeMulti, encode } from '@msgpack/msgpack'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ActorChannel, ActorChannelError } from '../../src/index.js'
import type { Context } from '../../src/types/type.js'

type TestContext = Context<string, string, string, string>
type Client = ActorChannel<string, string, string, string>

class FakeBroadcastChannel {
  static instances: FakeBroadcastChannel[] = []
  readonly messages: TestContext[] = []
  onmessage: ((event: MessageEvent<TestContext>) => void) | null = null
  constructor(readonly name: string) {
    FakeBroadcastChannel.instances.push(this)
  }
  postMessage(ctx: TestContext): void {
    this.messages.push(ctx)
  }
  receive(ctx: TestContext): void {
    this.onmessage?.({ data: ctx } as MessageEvent<TestContext>)
  }
}

class FakeWebSocket extends EventTarget {
  static readonly OPEN = 1
  static readonly CLOSED = 3
  static instances: FakeWebSocket[] = []
  static throwOnConstruct = false
  readonly sent: ArrayBuffer[] = []
  binaryType = ''
  readyState = 0
  onmessage: ((event: MessageEvent<ArrayBuffer>) => void) | null = null
  constructor(readonly url: string) {
    super()
    if (FakeWebSocket.throwOnConstruct) throw new Error('constructor')
    FakeWebSocket.instances.push(this)
  }
  send(data: ArrayBuffer): void {
    this.sent.push(data)
  }
  open(): void {
    this.readyState = FakeWebSocket.OPEN
    this.dispatchEvent(new Event('open'))
  }
  receive(ctx: unknown): void {
    const data = encode(ctx)
    this.onmessage?.({
      data: data.buffer.slice(
        data.byteOffset,
        data.byteOffset + data.byteLength
      ) as ArrayBuffer,
    } as MessageEvent<ArrayBuffer>)
  }
  receiveMalformed(): void {
    this.onmessage?.({
      data: new Uint8Array([0xc1]).buffer,
    } as MessageEvent<ArrayBuffer>)
  }
  close(): void {
    this.readyState = FakeWebSocket.CLOSED
    this.dispatchEvent(new Event('close'))
  }
}

class FakeLockManager {
  available = true
  brokerRequests = 0
  throwOnBrokerRequest = 1
  async request(
    name: string,
    options: LockOptions,
    callback: (lock: Lock | null) => Promise<void>
  ): Promise<void> {
    if (options.ifAvailable && options.signal)
      throw new DOMException(
        'ifAvailable and signal cannot be combined',
        'NotSupportedError'
      )
    if (name.includes('actor-channel')) this.brokerRequests++
    await callback(this.available ? ({} as Lock) : null)
    if (
      name.includes('actor-channel') &&
      this.brokerRequests >= this.throwOnBrokerRequest
    )
      throw new Error('stop loop')
  }
}

let windowTarget: EventTarget
let locks: FakeLockManager
const broadcast = (client: Client): FakeBroadcastChannel =>
  (client as any).broadcastChannel
const contexts = (socket: FakeWebSocket): TestContext[] =>
  socket.sent.map(
    (data) => decodeMulti(data)[Symbol.iterator]().next().value as TestContext
  )
const tick = () => new Promise<void>((resolve) => setTimeout(resolve))

beforeEach(() => {
  FakeBroadcastChannel.instances = []
  FakeWebSocket.instances = []
  FakeWebSocket.throwOnConstruct = false
  locks = new FakeLockManager()
  windowTarget = new EventTarget()
  Object.assign(windowTarget, {
    crypto: { randomUUID: () => 'request-id' },
    navigator: { locks },
  })
  vi.stubGlobal('window', windowTarget)
  vi.stubGlobal('BroadcastChannel', FakeBroadcastChannel)
  vi.stubGlobal('WebSocket', FakeWebSocket)
})

afterEach(() => {
  vi.restoreAllMocks()
  vi.useRealTimers()
  vi.unstubAllGlobals()
})

describe('ActorChannel local behavior', () => {
  it('uses the supplied storage access handle for its broadcast channel', () => {
    const factory = vi.fn((name: string) => new FakeBroadcastChannel(name))
    const client = new ActorChannel({
      BroadcastChannel: factory,
    } as unknown as StorageAccessHandle)
    expect(factory).toHaveBeenCalledWith(
      '@sovereignbase/actor-channel:broadcast-channel'
    )
    expect(broadcast(client)).toBe(FakeBroadcastChannel.instances[0])
  })
  it('wraps failures when creating the broadcast channel', () => {
    const cause = new Error('BroadcastChannel unavailable')
    vi.stubGlobal(
      'BroadcastChannel',
      class {
        constructor() {
          throw cause
        }
      }
    )
    expect(() => new ActorChannel()).toThrow(
      expect.objectContaining({
        name: 'ActorChannelError',
        code: 'UNKNOWN_ERROR',
        cause,
      })
    )
  })

  it.each(['request', 'publish', 'subscribe', 'unsubscribe'] as const)(
    'preserves a broadcast failure from %s',
    (method) => {
      const client = new ActorChannel<string, string, string, string>()
      client.subscribe('a')
      const cause = new Error('Broadcast failed')
      vi.spyOn(broadcast(client), 'postMessage').mockImplementation(() => {
        throw cause
      })
      const run = () =>
        method === 'publish'
          ? client.publish('a', 'out')
          : method === 'subscribe'
            ? client.subscribe('b')
            : client[method]('a')
      expect(run).toThrow(
        expect.objectContaining({
          name: 'ActorChannelError',
          code: 'UNKNOWN_ERROR',
          cause,
        })
      )
    }
  )

  it('does not wrap an existing library error twice', () => {
    const client = new ActorChannel<string, string, string, string>()
    const error = new ActorChannelError('UNKNOWN_ERROR', 'Original failure')
    vi.spyOn(broadcast(client), 'postMessage').mockImplementation(() => {
      throw error
    })
    expect(() => client.publish('a', 'out')).toThrow(error)
  })

  it('retains the cause if closing a broker fails', () => {
    const client = new ActorChannel<string, string, string, string>()
    const socket = new FakeWebSocket('ws://broker')
    ;(client as any).allBrokers.add(socket)
    const cause = new Error('Close failed')
    vi.spyOn(socket, 'close').mockImplementation(() => {
      throw cause
    })
    expect(() => client.emptyBrokers()).toThrow(
      expect.objectContaining({
        name: 'ActorChannelError',
        code: 'UNKNOWN_ERROR',
        cause,
      })
    )
  })

  it('tracks subscriptions and dispatches tab publications', () => {
    const client = new ActorChannel<string, string, string, string>()
    const received: unknown[] = []
    const listener = (event: CustomEvent<[string, string]>) =>
      received.push(event.detail)
    client.addEventListener('message', listener)
    client.subscribe('a')
    client.subscribe('a')
    broadcast(client).receive({
      kind: 'publish',
      topic: 'a',
      from: 'server',
      detail: 'one',
    })
    broadcast(client).receive({
      kind: 'publish',
      topic: 'b',
      from: 'server',
      detail: 'two',
    })
    client.removeEventListener('message', listener)
    client.unsubscribe('a')
    client.unsubscribe('a')
    expect(received).toEqual([['a', 'one']])
    expect(broadcast(client).messages.map((ctx) => ctx.kind)).toEqual([
      'internal',
      'subscribe',
      'unsubscribe',
    ])
  })

  it('fans client traffic received from other tabs', () => {
    const client = new ActorChannel<string, string, string, string>()
    const socket = new FakeWebSocket('ws://broker')
    socket.readyState = FakeWebSocket.OPEN
    ;(client as any).allBrokers.add(socket)
    ;(client as any).brokerTopics.set(socket, new Map([['a', 1]]))
    broadcast(client).receive({ kind: 'subscribe', topic: 'a', from: 'client' })
    broadcast(client).receive({ kind: 'subscribe', topic: 'a', from: 'client' })
    broadcast(client).receive({
      kind: 'publish',
      topic: 'a',
      from: 'client',
      detail: 'x',
    })
    broadcast(client).receive({
      kind: 'unsubscribe',
      topic: 'a',
      from: 'client',
    })
    broadcast(client).receive({
      kind: 'unsubscribe',
      topic: 'a',
      from: 'client',
    })
    broadcast(client).receive({
      kind: 'unsubscribe',
      topic: 'a',
      from: 'client',
    })
    expect(contexts(socket)).toEqual([
      { kind: 'subscribe', topic: 'a', from: 'client' },
      { kind: 'publish', topic: 'a', from: 'client', detail: 'x' },
      { kind: 'unsubscribe', topic: 'a', from: 'client' },
    ])
  })

  it('routes requests and accepts each response once', () => {
    const client = new ActorChannel<string, string, string, string>()
    const socket = new FakeWebSocket('ws://broker')
    socket.readyState = FakeWebSocket.OPEN
    ;(client as any).rpcEnabled.add(socket)
    const responses: unknown[] = []
    client.addEventListener('response', (event) => responses.push(event.detail))
    expect(client.request('run')).toBe('request-id')
    broadcast(client).receive({
      kind: 'request',
      id: 'foreign',
      detail: 'relay',
    })
    broadcast(client).receive({ kind: 'response', id: 'other', detail: 'no' })
    broadcast(client).receive({
      kind: 'response',
      id: 'request-id',
      detail: 'ok',
    })
    broadcast(client).receive({
      kind: 'response',
      id: 'request-id',
      detail: 'again',
    })
    expect(contexts(socket)).toEqual([
      { kind: 'request', id: 'request-id', detail: 'run' },
      { kind: 'request', id: 'foreign', detail: 'relay' },
    ])
    expect(responses).toEqual([['request-id', 'ok']])
  })

  it('handles lifecycle topic and RPC fanout', () => {
    const client = new ActorChannel<string, string, string, string>()
    const socket = new FakeWebSocket('ws://broker')
    socket.readyState = FakeWebSocket.OPEN
    ;(client as any).allBrokers.add(socket)
    ;(client as any).rpcEnabled.add(socket)
    ;(client as any).rpcOnline.add(socket)
    const closed = new FakeWebSocket('ws://closed')
    ;(client as any).rpcEnabled.add(closed)
    ;(client as any).rpcBrokers = 1
    client.subscribe('a')
    socket.sent.length = 0
    windowTarget.dispatchEvent(new Event('pagehide'))
    const ignored = new Event('pageshow') as PageTransitionEvent
    Object.defineProperty(ignored, 'persisted', { value: false })
    windowTarget.dispatchEvent(ignored)
    const restored = new Event('pageshow') as PageTransitionEvent
    Object.defineProperty(restored, 'persisted', { value: true })
    windowTarget.dispatchEvent(restored)
    expect(contexts(socket)).toEqual([
      { kind: 'unsubscribe', topic: 'a', from: 'client' },
      { kind: 'subscribe', topic: 'a', from: 'client' },
    ])
    expect((client as any).rpcOnline.has(socket)).toBe(true)
  })

  it('converges RPC counts and requests a snapshot for a broken chain', async () => {
    const client = new ActorChannel<string, string, string, string>()
    const bc = broadcast(client)
    ;(client as any).rpcBrokers = -1
    bc.receive({
      kind: 'internal',
      detail: { var: 'rpcBrokers', prev: 2, count: 3 },
    })
    expect(client.rpcAvailable).toBe(true)
    bc.receive({
      kind: 'internal',
      detail: { var: 'rpcBrokers', prev: 3, count: 2 },
    })
    bc.receive({
      kind: 'internal',
      detail: { var: 'rpcBrokers', prev: 8, count: 7 },
    })
    bc.receive({
      kind: 'internal',
      detail: { var: 'rpcBrokers', prev: 7, count: 7 },
    })
    bc.receive({ kind: 'internal', detail: { var: 'rpcBrokers', ping: true } })
    await tick()
    expect(
      bc.messages.some((ctx) => ctx.kind === 'internal' && 'ping' in ctx.detail)
    ).toBe(true)
    expect((client as any).rpcBrokers).toBe(7)
  })

  it('covers defensive no-op fanout, lifecycle and RPC branches', async () => {
    const client = new ActorChannel<string, string, string, string>()
    const closed = new FakeWebSocket('ws://closed')
    ;(client as any).allBrokers.add(closed)
    ;(client as any).rpcEnabled.add(closed)
    ;(client as any).brokerTopics.set(closed, new Map([['a', 1]]))
    ;(client as any).publishFanout({ kind: 'request', id: 'x', detail: 'x' })
    ;(client as any).subscribeFanout({ kind: 'request', id: 'x', detail: 'x' })
    ;(client as any).subscribeFanout({
      kind: 'subscribe',
      topic: 'a',
      from: 'client',
    })
    broadcast(client).receive(null as unknown as TestContext)
    windowTarget.dispatchEvent(new Event('pagehide'))
    ;(client as any).rpcFanout(-10)
    ;(client as any).rpcBrokers = -1
    ;(client as any).respondRpcBrokers()
    ;(client as any).rpcBrokers = 0
    locks.available = false
    ;(client as any).respondRpcBrokers()
    await tick()
    expect(client.rpcAvailable).toBe(false)
    expect(closed.sent).toHaveLength(0)
  })
})

describe('ActorChannel broker behavior', () => {
  it('empties open and connecting brokers, preserves subscriptions and allows reuse', async () => {
    locks.throwOnBrokerRequest = Infinity
    const client = new ActorChannel<string, string, string, string>()
    client.subscribe('a')
    const first = client.addBroker('ws://first', true)
    const second = client.addBroker('ws://second', true)
    const [open, connecting] = FakeWebSocket.instances
    open!.open()
    open!.receive({ kind: 'subscribe', topic: 'a', from: 'server', amount: 2 })
    expect(client.rpcAvailable).toBe(true)
    const messages = vi.fn()
    client.addEventListener('message', messages)
    const bc = broadcast(client)
    bc.messages.length = 0

    client.emptyBrokers()
    expect(client.rpcAvailable).toBe(false)
    expect(open!.readyState).toBe(FakeWebSocket.CLOSED)
    expect(connecting!.readyState).toBe(FakeWebSocket.CLOSED)
    const snapshot = [...bc.messages]
    // Late browser events from the old sockets cannot revive cleared state.
    connecting!.open()
    open!.receive({
      kind: 'publish',
      topic: 'a',
      from: 'server',
      detail: 'late',
    })
    expect(messages).not.toHaveBeenCalled()
    await Promise.all([first, second])
    expect(bc.messages).toEqual(snapshot)
    expect(snapshot).toEqual([
      { kind: 'internal', detail: { var: 'rpcBrokers', prev: 1, count: 0 } },
    ])
    client.emptyBrokers()
    const next = client.addBroker('ws://first', true)
    const replacement = FakeWebSocket.instances.at(-1)!
    replacement.open()
    expect(contexts(replacement)).toEqual([
      { kind: 'subscribe', topic: 'a', from: 'client' },
    ])
    expect(client.rpcAvailable).toBe(true)
    bc.receive({ kind: 'publish', topic: 'a', from: 'client', detail: 'local' })
    expect(messages.mock.calls[0]![0].detail).toEqual(['a', 'local'])
    client.emptyBrokers()
    await next
  })

  it('cancels the retry delay without acquiring another lock', async () => {
    vi.useFakeTimers()
    locks.available = false
    locks.throwOnBrokerRequest = Infinity
    const client = new ActorChannel<string, string, string, string>()
    const running = client.addBroker('ws://broker')
    await vi.advanceTimersByTimeAsync(0)
    client.emptyBrokers()
    await running
    await vi.advanceTimersByTimeAsync(20_000)
    expect(locks.brokerRequests).toBe(1)
  })

  it('ignores a lock callback delivered after its connection loop was aborted', async () => {
    const client = new ActorChannel<string, string, string, string>()
    vi.spyOn(locks, 'request').mockImplementationOnce(
      async (_name, _options, callback) => {
        client.emptyBrokers()
        await callback({} as Lock)
      }
    )
    await client.addBroker('ws://broker')
    expect(FakeWebSocket.instances).toHaveLength(0)
  })

  it('handles cancellation during retry listener registration', async () => {
    locks.available = false
    locks.throwOnBrokerRequest = Infinity
    const client = new ActorChannel<string, string, string, string>()
    const signal = (client as any).brokerController.signal as AbortSignal
    const add = signal.addEventListener.bind(signal)
    vi.spyOn(signal, 'addEventListener').mockImplementation((...args) => {
      client.emptyBrokers()
      add(...args)
    })
    await client.addBroker('ws://broker')
    expect(locks.brokerRequests).toBe(1)
  })

  it('rejects invalid broker input and unavailable locks', async () => {
    const client = new ActorChannel<string, string, string, string>()
    await client.addBroker(null as unknown as string)
    ;(windowTarget as any).navigator.locks = undefined
    await client.addBroker('ws://broker')
    expect(FakeWebSocket.instances).toHaveLength(0)
  })

  it('returns when another tab owns the lock or construction fails', async () => {
    const client = new ActorChannel<string, string, string, string>()
    locks.available = false
    await client.addBroker('ws://broker')
    locks.available = true
    FakeWebSocket.throwOnConstruct = true
    await client.addBroker('ws://broker')
    expect(FakeWebSocket.instances).toHaveLength(0)
  })

  it('opens a broker, restores subscriptions, tracks amounts and cleans up', async () => {
    const client = new ActorChannel<string, string, string, string>()
    client.subscribe('a')
    const running = client.addBroker('ws://broker', true)
    const socket = FakeWebSocket.instances[0]!
    socket.open()
    socket.receive({ kind: 'subscribe', topic: 'a', from: 'server', amount: 2 })
    socket.receive({ kind: 'subscribe', topic: 'b', from: 'server', amount: 1 })
    client.publish('a', 'out')
    socket.receive({
      kind: 'unsubscribe',
      topic: 'a',
      from: 'server',
      amount: 1,
    })
    socket.receive({
      kind: 'unsubscribe',
      topic: 'a',
      from: 'server',
      amount: 0,
    })
    socket.receive({ kind: 'unsubscribe', topic: 'b', from: 'server' })
    socket.receive({
      kind: 'unsubscribe',
      topic: 'b',
      from: 'server',
      amount: 0,
    })
    socket.close()
    await running
    expect(contexts(socket)).toEqual([
      { kind: 'subscribe', topic: 'a', from: 'client' },
      { kind: 'publish', topic: 'a', from: 'client', detail: 'out' },
    ])
    expect((client as any).allBrokers.size).toBe(0)
    expect((client as any).brokerTopics.size).toBe(0)
  })

  it('handles broker messages and malformed data', async () => {
    const client = new ActorChannel<string, string, string, string>()
    client.subscribe('a')
    const received: unknown[] = []
    client.addEventListener('message', (event) => received.push(event.detail))
    client.addEventListener('response', (event) => received.push(event.detail))
    client.request('run')
    const running = client.addBroker('ws://broker')
    const socket = FakeWebSocket.instances[0]!
    socket.open()
    socket.receiveMalformed()
    socket.receive(null)
    socket.receive({ kind: 'subscribe', topic: 'a', from: 'server' })
    socket.receive({
      kind: 'unsubscribe',
      topic: 'a',
      from: 'server',
      amount: 0,
    })
    socket.receive({
      kind: 'publish',
      topic: 'a',
      from: 'server',
      detail: 'in',
    })
    socket.receive({
      kind: 'publish',
      topic: 'b',
      from: 'server',
      detail: 'skip',
    })
    socket.receive({ kind: 'response', id: 'request-id', detail: 'ok' })
    socket.receive({ kind: 'response', id: 'foreign', detail: 'relay' })
    socket.receive({ kind: 'unknown' })
    socket.close()
    await running
    expect(received).toEqual([
      ['a', 'in'],
      ['request-id', 'ok'],
    ])
    expect(broadcast(client).messages).toContainEqual({
      kind: 'response',
      id: 'foreign',
      detail: 'relay',
    })
  })

  it('does not send through closed or unrelated sockets', () => {
    const client = new ActorChannel<string, string, string, string>()
    const closed = new FakeWebSocket('ws://closed')
    const unrelated = new FakeWebSocket('ws://other')
    unrelated.readyState = FakeWebSocket.OPEN
    ;(client as any).rpcEnabled.add(closed)
    ;(client as any).brokerTopics.set(closed, new Map([['a', 1]]))
    ;(client as any).brokerTopics.set(unrelated, new Map([['b', 1]]))
    client.request('run')
    client.publish('a', 'out')
    expect(closed.sent).toHaveLength(0)
    expect(unrelated.sent).toHaveLength(0)
  })

  it('sends independently decodable socket frames', () => {
    const client = new ActorChannel<string, string, string, string>()
    const socket = new FakeWebSocket('ws://broker')
    socket.readyState = FakeWebSocket.OPEN
    ;(client as any).brokerTopics.set(socket, new Map([['a', 1]]))
    client.publish('a', 'out')
    expect(() => decode(socket.sent[0]!)).not.toThrow()
  })

  it('waits before retrying broker lock acquisition', async () => {
    vi.useFakeTimers()
    const client = new ActorChannel<string, string, string, string>()
    locks.available = false
    locks.throwOnBrokerRequest = 2
    const running = client.addBroker('ws://broker')
    await vi.advanceTimersByTimeAsync(10_000)
    await running
    expect(locks.brokerRequests).toBe(2)
    vi.useRealTimers()
  })
})
