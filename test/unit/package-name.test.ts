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

  readonly sent: Uint8Array[] = []
  binaryType = ''
  readyState = 0
  onopen: (() => void) | null = null
  onmessage: ((event: MessageEvent<ArrayBuffer>) => void) | null = null
  onclose: (() => void) | null = null

  constructor(readonly url: string) {
    super()
    FakeWebSocket.instances.push(this)
    queueMicrotask(() => {
      this.readyState = FakeWebSocket.OPEN
      this.onopen?.()
    })
  }

  send(data: Uint8Array): void {
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

const sentContexts = (socket: FakeWebSocket) =>
  socket.sent.map((message) => decode(message) as Record<string, unknown>)

const waitUntilOnline = (client: Client) =>
  vi.waitFor(() =>
    expect((client as unknown as { isOnline: boolean }).isOnline).toBe(true)
  )

beforeEach(() => {
  FakeWebSocket.instances = []
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
})
