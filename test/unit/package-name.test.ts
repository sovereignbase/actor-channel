import { decode, encode } from '@msgpack/msgpack'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { OriginSocket } from '../../src/index.js'

type Client = OriginSocket<
  string,
  { message: string },
  { roomId: string },
  { candidate: string },
  { method: string },
  { result: string }
>

class FakeWebSocket extends EventTarget {
  static readonly OPEN = 1
  static instances: FakeWebSocket[] = []
  static throwOnConstruct = false

  readonly sent: Uint8Array[] = []
  throwOnSend = false
  throwOnClose = false
  binaryType = ''
  readyState = 0
  onopen: (() => void) | null = null
  onmessage: ((event: MessageEvent<ArrayBuffer>) => void) | null = null
  onclose: (() => void) | null = null

  constructor(readonly url: string) {
    super()
    if (FakeWebSocket.throwOnConstruct) throw new Error('constructor failed')
    FakeWebSocket.instances.push(this)
    queueMicrotask(() => {
      this.readyState = FakeWebSocket.OPEN
      this.onopen?.()
    })
  }

  send(data: Uint8Array): void {
    if (this.throwOnSend) throw new Error('send failed')
    this.sent.push(data.slice())
  }

  receive(ctx: unknown): void {
    const data = encode(ctx)
    this.onmessage?.(
      new MessageEvent('message', {
        data: data.buffer.slice(
          data.byteOffset,
          data.byteOffset + data.byteLength
        ) as ArrayBuffer,
      })
    )
  }

  close(): void {
    if (this.throwOnClose) throw new Error('close failed')
    if (this.readyState !== FakeWebSocket.OPEN) return
    this.readyState = 3
    this.onclose?.()
    this.dispatchEvent(new Event('close'))
  }
}

class FakeLockManager {
  private held = false
  private queue: Array<() => void> = []

  request(
    _name: string,
    callback: (lock: Lock | null) => Promise<void>
  ): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const run = async () => {
        this.held = true
        try {
          await callback({} as Lock)
          resolve()
        } catch (error) {
          reject(error)
        } finally {
          this.held = false
          this.queue.shift()?.()
        }
      }

      if (this.held) this.queue.push(() => void run())
      else void run()
    })
  }
}

const clients: Client[] = []

const createClient = () => {
  const client = new OriginSocket<
    string,
    { message: string },
    { roomId: string },
    { candidate: string },
    { method: string },
    { result: string }
  >('ws://origin.test')
  clients.push(client)
  return client
}

const createOfflineClient = () => {
  const client = new OriginSocket<
    string,
    { message: string },
    { roomId: string },
    { candidate: string },
    { method: string },
    { result: string }
  >()
  clients.push(client)
  return client
}

const sentContexts = (socket: FakeWebSocket) =>
  socket.sent.map((message) => decode(message) as Record<string, unknown>)

const waitUntilOnline = (client: Client) =>
  vi.waitFor(() =>
    expect((client as unknown as { isOnline: boolean }).isOnline).toBe(true)
  )

const channelMessage = (client: Client, data: unknown) => {
  const channel = (client as unknown as { broadcastChannel: BroadcastChannel })
    .broadcastChannel
  channel.onmessage?.(new MessageEvent('message', { data }))
}

beforeEach(() => {
  FakeWebSocket.instances = []
  FakeWebSocket.throwOnConstruct = false
  const navigator = { onLine: true, locks: new FakeLockManager() }
  const worker = Object.assign(new EventTarget(), { navigator })

  vi.stubGlobal('navigator', navigator)
  vi.stubGlobal('self', worker)
  vi.stubGlobal('WebSocket', FakeWebSocket)
})

afterEach(() => {
  for (const client of clients.splice(0)) client.close()
  vi.unstubAllGlobals()
})

describe('OriginSocket client routing', () => {
  it('forwards invoke and resolves a follower transaction response', async () => {
    const leader = createClient()
    const follower = createClient()
    const socket = FakeWebSocket.instances[0]!
    await vi.waitFor(() => expect(socket.readyState).toBe(FakeWebSocket.OPEN))
    await waitUntilOnline(follower)

    expect(follower.invoke({ method: 'refresh' })).toBe(true)
    await vi.waitFor(() => expect(socket.sent).toHaveLength(1))
    expect(sentContexts(socket)[0]).toEqual({
      kind: 'invoke',
      payload: { method: 'refresh' },
    })

    const response = follower.transact({ method: 'read' })
    await vi.waitFor(() => expect(socket.sent).toHaveLength(2))
    const request = sentContexts(socket)[1]!

    socket.receive({
      kind: 'transact',
      phase: 'response',
      id: request.id,
      payload: { result: 'done' },
    })

    await expect(response).resolves.toEqual({ result: 'done' })
    expect(leader).toBeDefined()
  })

  it('routes answers only to the instance that owns the offer', async () => {
    const leader = createClient()
    const follower = createClient()
    const socket = FakeWebSocket.instances[0]!
    await waitUntilOnline(follower)
    const leaderAnswers: unknown[] = []
    const followerAnswers: unknown[] = []
    leader.addEventListener('answer', (event) =>
      leaderAnswers.push(event.detail)
    )
    follower.addEventListener('answer', (event) =>
      followerAnswers.push(event.detail)
    )

    const withdraw = follower.offer({ roomId: 'room-1' })
    await vi.waitFor(() => expect(socket.sent).toHaveLength(1))
    const offer = sentContexts(socket)[0]!

    socket.receive({
      kind: 'answer',
      id: offer.id,
      payload: { candidate: 'candidate-1' },
    })
    await vi.waitFor(() =>
      expect(followerAnswers).toEqual([{ candidate: 'candidate-1' }])
    )
    expect(leaderAnswers).toEqual([])

    withdraw()
    withdraw()
    await vi.waitFor(() => expect(socket.sent).toHaveLength(2))
    expect(sentContexts(socket)[1]).toEqual({
      kind: 'withdraw',
      id: offer.id,
    })

    socket.receive({
      kind: 'answer',
      id: offer.id,
      payload: { candidate: 'late' },
    })
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(followerAnswers).toHaveLength(1)
  })

  it('tracks subscriptions in both directions and routes gossip', async () => {
    const leader = createClient()
    const follower = createClient()
    const socket = FakeWebSocket.instances[0]!
    await waitUntilOnline(follower)
    const leaderGossip: unknown[] = []
    const followerGossip: unknown[] = []
    leader.addEventListener('gossip', (event) =>
      leaderGossip.push(event.detail)
    )
    follower.addEventListener('gossip', (event) =>
      followerGossip.push(event.detail)
    )

    expect(leader.subscribe('news')).toBe(true)
    expect(leader.subscribe('news')).toBe(false)
    expect(follower.subscribe('news')).toBe(true)
    await vi.waitFor(() => expect(socket.sent).toHaveLength(2))

    socket.receive({
      kind: 'gossip',
      from: 'server',
      topic: 'news',
      payload: { message: 'hello' },
    })
    await vi.waitFor(() => {
      expect(leaderGossip).toEqual([{ message: 'hello' }])
      expect(followerGossip).toEqual([{ message: 'hello' }])
    })

    leader.unsubscribe('news')
    leader.unsubscribe('news')
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(socket.sent).toHaveLength(2)

    follower.unsubscribe('news')
    await vi.waitFor(() => expect(socket.sent).toHaveLength(3))
    expect(sentContexts(socket)[2]).toMatchObject({
      kind: 'unsubscribe',
      topic: 'news',
      from: 'client',
    })

    socket.receive({ kind: 'subscribe', from: 'server', topic: 'presence' })
    socket.receive({ kind: 'subscribe', from: 'server', topic: 'presence' })
    follower.gossip('presence', { message: 'one' })
    await vi.waitFor(() => expect(socket.sent).toHaveLength(4))

    socket.receive({ kind: 'unsubscribe', from: 'server', topic: 'presence' })
    follower.gossip('presence', { message: 'two' })
    await vi.waitFor(() => expect(socket.sent).toHaveLength(5))

    socket.receive({ kind: 'unsubscribe', from: 'server', topic: 'presence' })
    follower.gossip('presence', { message: 'three' })
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(socket.sent).toHaveLength(5)
  })

  it('withdraws active offers and subscriptions when an instance closes', async () => {
    createClient()
    const follower = createClient()
    const socket = FakeWebSocket.instances[0]!
    await waitUntilOnline(follower)

    follower.subscribe('news')
    follower.offer({ roomId: 'room-1' })
    await vi.waitFor(() => expect(socket.sent).toHaveLength(2))
    socket.sent.length = 0

    follower.close()
    await vi.waitFor(() => expect(socket.sent).toHaveLength(2))
    expect(
      sentContexts(socket)
        .map(({ kind }) => kind)
        .sort()
    ).toEqual(['unsubscribe', 'withdraw'])
  })

  it('rejects a transaction without replaying it after failover', async () => {
    const leader = createClient()
    const follower = createClient()
    const firstSocket = FakeWebSocket.instances[0]!
    await waitUntilOnline(follower)

    const response = follower.transact({ method: 'write' })
    await vi.waitFor(() => expect(firstSocket.sent).toHaveLength(1))

    leader.close()
    await expect(response).rejects.toMatchObject({ name: 'NetworkError' })
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(2))
    const secondSocket = FakeWebSocket.instances[1]!
    await vi.waitFor(() =>
      expect(secondSocket.readyState).toBe(FakeWebSocket.OPEN)
    )
    expect(secondSocket.sent).toHaveLength(0)
  })

  it('keeps replicated origin and upstream topic counts after failover', async () => {
    const leader = createClient()
    const first = createClient()
    const second = createClient()
    const firstSocket = FakeWebSocket.instances[0]!
    await waitUntilOnline(second)

    first.subscribe('news')
    second.subscribe('news')
    await vi.waitFor(() => expect(firstSocket.sent).toHaveLength(2))
    firstSocket.receive({
      kind: 'subscribe',
      from: 'server',
      topic: 'presence',
    })
    await new Promise((resolve) => setTimeout(resolve, 0))

    leader.close()
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(2))
    const secondSocket = FakeWebSocket.instances[1]!
    await vi.waitFor(() =>
      expect(secondSocket.readyState).toBe(FakeWebSocket.OPEN)
    )
    await waitUntilOnline(first)

    expect(first.gossip('presence', { message: 'online' })).toBe(true)
    await vi.waitFor(() => expect(secondSocket.sent).toHaveLength(1))

    first.unsubscribe('news')
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(secondSocket.sent).toHaveLength(1)

    second.unsubscribe('news')
    await vi.waitFor(() => expect(secondSocket.sent).toHaveLength(2))
    expect(sentContexts(secondSocket)[1]).toMatchObject({
      kind: 'unsubscribe',
      topic: 'news',
    })
  })

  it('covers public no-op, closed, abort, and listener behavior', async () => {
    const offline = createOfflineClient()
    expect(offline.invoke({ method: 'write' })).toBe(false)
    expect(offline.gossip('news', { message: 'hello' })).toBe(false)
    expect(offline.subscribe('news')).toBe(false)
    expect(offline.unsubscribe('news')).toBe(false)
    await expect(offline.transact({ method: 'write' })).resolves.toBe(false)

    const listener = vi.fn()
    offline.addEventListener('online', listener)
    offline.removeEventListener('online', listener)
    channelMessage(offline, { kind: 'online', id: 'connection' })
    expect(listener).not.toHaveBeenCalled()

    offline.close()
    offline.close()
    const withdraw = offline.offer({ roomId: 'closed' })
    withdraw()
    expect(offline.invoke({ method: 'closed' })).toBe(false)
    expect(offline.gossip('news', { message: 'closed' })).toBe(false)
    expect(offline.subscribe('news')).toBe(false)
    expect(offline.unsubscribe('news')).toBe(false)
    await expect(offline.transact({ method: 'closed' })).resolves.toBe(false)

    const leader = createClient()
    const follower = createClient()
    const socket = FakeWebSocket.instances[0]!
    await waitUntilOnline(follower)

    const alreadyAborted = new AbortController()
    const abortReason = new Error('already aborted')
    alreadyAborted.abort(abortReason)
    await expect(
      follower.transact({ method: 'abort' }, alreadyAborted.signal)
    ).rejects.toBe(abortReason)

    const fallbackSignal = {
      aborted: true,
      reason: undefined,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    } as unknown as AbortSignal
    await expect(
      follower.transact({ method: 'abort' }, fallbackSignal)
    ).rejects.toMatchObject({ name: 'AbortError' })

    const controller = new AbortController()
    const pending = follower.transact({ method: 'abort' }, controller.signal)
    await vi.waitFor(() => expect(socket.sent).toHaveLength(1))
    controller.abort()
    await expect(pending).rejects.toMatchObject({ name: 'AbortError' })
    expect(leader).toBeDefined()
  })

  it('handles every BroadcastChannel message direction', () => {
    const client = createOfflineClient()
    const state = client as unknown as {
      isLeader: boolean
      isOnline: boolean
      connectionId: string | null
      webSocketUrl: string
      webSocket: FakeWebSocket | null
      upstreamTopics: Map<string, number>
      originTopics: Map<string, number>
      myTopics: Set<string>
      myOffers: Set<string>
      myTransacts: Map<
        string,
        { resolve: (value: unknown) => void; reject: () => void; cleanup: () => void }
      >
    }
    const socket = new FakeWebSocket('ws://origin.test')
    socket.readyState = FakeWebSocket.OPEN
    state.webSocketUrl = 'ws://origin.test'
    state.webSocket = socket

    channelMessage(client, null)
    channelMessage(client, { kind: 'status' })
    channelMessage(client, { kind: 'online', id: 'first' })
    channelMessage(client, { kind: 'online', id: 'first' })
    channelMessage(client, { kind: 'offline', id: 'stale' })
    state.isOnline = false
    state.connectionId = 'offline'
    channelMessage(client, { kind: 'offline', id: 'offline' })

    const reject = vi.fn()
    const cleanup = vi.fn()
    state.isOnline = true
    state.connectionId = 'active'
    state.myTransacts.set('pending', {
      resolve: vi.fn(),
      reject,
      cleanup,
    })
    channelMessage(client, { kind: 'offline', id: 'active' })
    expect(reject).toHaveBeenCalledOnce()
    expect(cleanup).toHaveBeenCalledOnce()

    channelMessage(client, { kind: 'invoke', payload: { method: 'ignored' } })
    channelMessage(client, {
      kind: 'offer',
      id: 'ignored',
      payload: { roomId: 'ignored' },
    })
    channelMessage(client, { kind: 'withdraw', id: 'ignored' })
    channelMessage(client, {
      kind: 'transact',
      id: 'ignored',
      phase: 'request',
      payload: { method: 'ignored' },
    })

    state.isLeader = true
    state.isOnline = true
    state.connectionId = 'leader'
    channelMessage(client, { kind: 'status' })
    channelMessage(client, { kind: 'invoke', payload: { method: 'invoke' } })
    channelMessage(client, {
      kind: 'offer',
      id: 'offer',
      payload: { roomId: 'room' },
    })
    channelMessage(client, { kind: 'withdraw', id: 'offer' })

    const answers: unknown[] = []
    const gossip: unknown[] = []
    client.addEventListener('answer', (event) => answers.push(event.detail))
    client.addEventListener('gossip', (event) => gossip.push(event.detail))
    channelMessage(client, {
      kind: 'answer',
      id: 'missing',
      payload: { candidate: 'missing' },
    })
    state.myOffers.add('owned')
    channelMessage(client, {
      kind: 'answer',
      id: 'owned',
      payload: { candidate: 'owned' },
    })

    channelMessage(client, {
      kind: 'gossip',
      from: 'client',
      topic: 'ignored',
      payload: { message: 'ignored' },
    })
    state.upstreamTopics.set('news', 1)
    state.myTopics.add('news')
    channelMessage(client, {
      kind: 'gossip',
      from: 'client',
      topic: 'news',
      payload: { message: 'client' },
    })
    channelMessage(client, {
      kind: 'gossip',
      from: 'server',
      topic: 'news',
      payload: { message: 'server' },
    })

    channelMessage(client, {
      kind: 'transact',
      id: 'request',
      phase: 'request',
      payload: { method: 'request' },
    })
    channelMessage(client, {
      kind: 'transact',
      id: 'missing',
      phase: 'response',
      payload: { result: 'missing' },
    })
    const resolve = vi.fn()
    state.myTransacts.set('owned', { resolve, reject: vi.fn(), cleanup })
    channelMessage(client, {
      kind: 'transact',
      id: 'owned',
      phase: 'response',
      payload: { result: 'owned' },
    })

    channelMessage(client, { kind: 'subscribe', from: 'server', topic: 'up' })
    channelMessage(client, { kind: 'subscribe', from: 'server', topic: 'up' })
    channelMessage(client, { kind: 'subscribe', from: 'client', topic: 'local' })
    channelMessage(client, {
      kind: 'unsubscribe',
      from: 'server',
      topic: 'missing',
    })
    channelMessage(client, { kind: 'unsubscribe', from: 'server', topic: 'up' })
    channelMessage(client, { kind: 'unsubscribe', from: 'server', topic: 'up' })
    channelMessage(client, {
      kind: 'unsubscribe',
      from: 'client',
      topic: 'local',
    })

    expect(answers).toEqual([{ candidate: 'owned' }])
    expect(gossip).toEqual([{ message: 'client' }, { message: 'server' }])
    expect(resolve).toHaveBeenCalledWith({ result: 'owned' })
  })

  it('handles transport queues and WebSocket send failures', async () => {
    const client = createOfflineClient()
    const state = client as any
    const context = { kind: 'invoke', payload: { method: 'write' } }

    expect(state.sendUpstream(context)).toBe(false)
    state.isLeader = true
    expect(state.sendUpstream(context)).toBe(false)
    state.webSocketUrl = 'ws://origin.test'
    expect(state.sendUpstream(context)).toBe(false)
    expect(state.upstreamQueue).toHaveLength(1)

    for (let index = 0; index < 65; index++) state.sendUpstream(context)
    expect(state.upstreamQueue).toHaveLength(64)

    ;(navigator as unknown as { onLine: boolean }).onLine = false
    state.upstreamQueue.length = 0
    expect(state.sendUpstream(context)).toBe(false)
    expect(state.upstreamQueue).toHaveLength(0)
    ;(navigator as unknown as { onLine: boolean }).onLine = true

    state.flushUpstreamQueue()
    const socket = new FakeWebSocket('ws://origin.test')
    state.webSocket = socket
    state.flushUpstreamQueue()
    socket.readyState = FakeWebSocket.OPEN
    state.upstreamQueue.push(undefined, context)
    state.flushUpstreamQueue()
    expect(socket.sent).toHaveLength(1)

    socket.throwOnSend = true
    expect(state.sendUpstream(context)).toBe(false)
    state.upstreamQueue.push(context)
    state.flushUpstreamQueue()
    expect(state.upstreamQueue).toHaveLength(1)
    socket.throwOnSend = false
    expect(state.sendUpstream(context)).toBe(true)
    await new Promise((resolve) => setTimeout(resolve, 0))
  })

  it('covers direct leader operations and inbound WebSocket branches', async () => {
    const leader = createClient()
    const socket = FakeWebSocket.instances[0]!
    await waitUntilOnline(leader)

    expect(leader.invoke({ method: 'invoke' })).toBe(true)
    const withdraw = leader.offer({ roomId: 'room' })
    const offer = sentContexts(socket).find(({ kind }) => kind === 'offer')!
    const answers: unknown[] = []
    leader.addEventListener('answer', (event) => answers.push(event.detail))
    socket.receive({
      kind: 'answer',
      id: offer.id,
      payload: { candidate: 'answer' },
    })
    withdraw()

    const response = leader.transact({ method: 'read' })
    const transaction = sentContexts(socket).find(
      ({ kind }) => kind === 'transact'
    )!
    socket.receive({
      kind: 'transact',
      phase: 'response',
      id: transaction.id,
      payload: { result: 'done' },
    })
    await expect(response).resolves.toEqual({ result: 'done' })

    socket.throwOnSend = true
    await expect(leader.transact({ method: 'failed' })).resolves.toBe(false)
    socket.throwOnSend = false

    expect(leader.subscribe('direct')).toBe(true)
    expect(leader.unsubscribe('direct')).toBe(true)
    socket.receive(1)
    socket.receive({
      kind: 'transact',
      phase: 'request',
      id: 'request',
      payload: { method: 'ignored' },
    })
    socket.receive({
      kind: 'gossip',
      from: 'client',
      topic: 'ignored',
      payload: { message: 'ignored' },
    })
    socket.receive({
      kind: 'gossip',
      from: 'server',
      topic: 'unsubscribed',
      payload: { message: 'ignored' },
    })
    socket.receive({ kind: 'subscribe', from: 'client', topic: 'ignored' })
    socket.receive({ kind: 'unsubscribe', from: 'client', topic: 'ignored' })
    socket.receive({ kind: 'unsubscribe', from: 'server', topic: 'missing' })

    expect(answers).toEqual([{ candidate: 'answer' }])
  })

  it('rejects a leader transaction when its socket closes', async () => {
    const leader = createClient()
    const socket = FakeWebSocket.instances[0]!
    await waitUntilOnline(leader)
    const response = leader.transact({ method: 'pending' })
    socket.close()
    await expect(response).rejects.toMatchObject({ name: 'NetworkError' })
  })

  it('rejects follower transactions when that instance closes', async () => {
    createClient()
    const follower = createClient()
    await waitUntilOnline(follower)
    const response = follower.transact({ method: 'pending' })
    follower.close()
    await expect(response).rejects.toBeUndefined()
  })

  it('covers connection guards, constructor failure, and online retry', async () => {
    const offline = createOfflineClient()
    const state = offline as any
    await state.upstreamConnect()
    state.isConnecting = true
    state.webSocketUrl = 'ws://origin.test'
    await state.upstreamConnect()
    state.isConnecting = false
    state.isClosed = true
    await state.upstreamConnect()
    state.isClosed = false

    const navigatorState = navigator as unknown as {
      onLine: boolean
      locks?: FakeLockManager
    }
    const locks = navigatorState.locks
    navigatorState.locks = undefined
    await state.upstreamConnect()
    navigatorState.locks = locks
    navigatorState.onLine = false
    await state.upstreamConnect()
    self.dispatchEvent(new Event('online'))
    navigatorState.onLine = true

    FakeWebSocket.throwOnConstruct = true
    const failed = createClient()
    await new Promise((resolve) => setTimeout(resolve, 0))
    failed.close()
    FakeWebSocket.throwOnConstruct = false

    const originalLocks = navigatorState.locks
    navigatorState.locks = {
      request: (_name: string, callback: (lock: Lock | null) => Promise<void>) =>
        callback(null),
    } as unknown as FakeLockManager
    const noLock = createClient()
    await new Promise((resolve) => setTimeout(resolve, 0))
    noLock.close()
    navigatorState.locks = originalLocks
  })

  it('retries after the reconnect delay and handles close failures', async () => {
    vi.useFakeTimers()
    const client = createClient()
    const socket = FakeWebSocket.instances[0]!
    await vi.runAllTicks()
    socket.close()
    await vi.advanceTimersByTimeAsync(10_000)
    expect(FakeWebSocket.instances.length).toBeGreaterThan(1)
    client.close()
    vi.useRealTimers()

    const closeFailure = createClient()
    const failedSocket = FakeWebSocket.instances.at(-1)!
    await waitUntilOnline(closeFailure)
    failedSocket.throwOnClose = true
    closeFailure.close()

    const channelFailure = createOfflineClient()
    const channel = (channelFailure as any).broadcastChannel as BroadcastChannel
    const close = channel.close.bind(channel)
    channel.close = () => {
      throw new Error('close failed')
    }
    channelFailure.close()
    close()
  })

  it('covers the remaining event and connection branches', async () => {
    const offline = createOfflineClient()
    await (offline as any).onlineHandler()
    channelMessage(offline, { kind: 'unknown' })

    const first = createClient()
    const firstSocket = FakeWebSocket.instances.at(-1)!
    await waitUntilOnline(first)
    firstSocket.onopen?.()
    firstSocket.receive({ kind: 'unknown' })

    const firstState = first as any
    firstState.isClosed = true
    firstState.isOnline = false
    firstState.webSocket = null
    firstSocket.onclose?.()
    firstSocket.dispatchEvent(new Event('close'))
    await Promise.resolve()

    const second = createClient()
    await waitUntilOnline(second)
    const secondSocket = FakeWebSocket.instances.at(-1)!
    const secondState = second as any
    secondState.isClosed = true
    secondSocket.onclose?.()
    secondState.webSocket = secondSocket
    secondSocket.dispatchEvent(new Event('close'))
    await vi.waitFor(() => expect(secondState.webSocket).toBeNull())
  })
})
