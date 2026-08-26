import { encode, decode } from '@msgpack/msgpack'
import {
  Context,
  OriginSocketEventMap,
  OriginSocketEventListenerFor,
  TransactionPromise,
} from './types/index.js'

type ChannelMessage<T> =
  | Context<T>
  | { kind: 'online'; id: string }
  | { kind: 'offline'; id: string | null }
  | { kind: 'status' }

export class OriginSocket<
  Topic extends string,
  Gossip,
  Offer,
  Answer,
  RPCRequest,
  RPCResponse,
> {
  private readonly eventTarget = new EventTarget()
  private readonly webSocketUrl: string
  private readonly onlineHandler = async () => {
    void this.upstreamConnect()
  }

  private isLeader: boolean = false
  private isOnline: boolean = false
  private connectionId: string | null = null
  private isClosed: boolean = false
  private isConnecting: boolean = false
  //
  private broadcastChannel: BroadcastChannel | null = null
  private webSocket: WebSocket | null = null

  // Replicated topics ordered by local instances.
  private originTopics: Map<Topic, number> | null = null
  // A best effort offline queue mainly to allow calls before websocket is ready
  private upstreamQueue: Array<
    Context<RPCRequest | RPCResponse | Gossip | Offer | Topic>
  > | null = null
  // Replicated topics ordered by upstream.
  private upstreamTopics: Map<Topic, number> | null = null

  // Pending transaction promises of this instance
  private myTransacts: Map<string, TransactionPromise<RPCResponse>> | null =
    null
  private myOffers: Set<string> | null = null
  // Topics subscribed by  this instance
  private myTopics: Set<Topic> | null = null

  constructor(webSocketUrl: string = '') {
    this.webSocketUrl = webSocketUrl
    this.broadcastChannel = new BroadcastChannel(
      '@sovereignbase/origin-socket:broadcast-channel'
    )
    this.myTopics = new Set()
    this.myTransacts = new Map()
    this.myOffers = new Set()
    this.originTopics = new Map()
    this.upstreamQueue = []
    this.upstreamTopics = new Map()

    this.broadcastChannel.onmessage = (
      event: MessageEvent<
        ChannelMessage<
          Topic | RPCRequest | RPCResponse | Gossip | Offer | Answer
        >
      >
    ) => {
      const ctx = event.data
      if (!ctx) return

      if (ctx.kind === 'status') {
        if (this.isLeader && this.isOnline)
          void this.broadcastChannel!.postMessage({
            kind: 'online',
            id: this.connectionId!,
          })
        return
      }
      if (ctx.kind === 'online') {
        this.connectionId = ctx.id
        if (!this.isOnline) {
          this.isOnline = true
          void this.eventTarget.dispatchEvent(new CustomEvent('online'))
        }
        return
      }
      if (ctx.kind === 'offline') {
        if (ctx.id !== this.connectionId) return
        if (!this.isOnline) return
        this.isOnline = false
        this.connectionId = null
        const reason = new DOMException(
          'The upstream connection was lost.',
          'NetworkError'
        )
        for (const transaction of this.myTransacts!.values()) {
          void transaction.cleanup()
          void transaction.reject(reason)
        }
        void this.myTransacts!.clear()
        void this.eventTarget.dispatchEvent(new CustomEvent('offline'))
        return
      }

      if (ctx.kind === 'invoke') {
        if (!this.isLeader) return
        void this.sendUpstream(ctx as Context<RPCRequest>)
        return
      }

      if (ctx.kind === 'offer' || ctx.kind === 'withdraw') {
        if (!this.isLeader) return
        return void this.sendUpstream(ctx as Context<Offer>)
      }

      if (ctx.kind === 'answer') {
        if (!this.myOffers!.has(ctx.id)) return

        return void this.eventTarget.dispatchEvent(
          new CustomEvent('answer', { detail: ctx.payload as Answer })
        )
      }

      if (ctx.kind === 'gossip') {
        if (
          ctx.from === 'client' &&
          this.isLeader &&
          this.upstreamTopics!.has(ctx.topic as Topic)
        )
          void this.sendUpstream(ctx as Context<Gossip>)

        if (!this.myTopics!.has(ctx.topic as Topic)) return

        return void this.eventTarget.dispatchEvent(
          new CustomEvent('gossip', { detail: ctx.payload as Gossip })
        )
      }

      if (ctx.kind === 'transact') {
        if (ctx.phase === 'request') {
          if (!this.isLeader) return
          else void this.sendUpstream(ctx as Context<RPCRequest>)
        }
        if (ctx.phase === 'response') {
          const transaction = this.myTransacts!.get(ctx.id)
          if (!transaction) return

          void this.myTransacts!.delete(ctx.id)
          void transaction.cleanup()
          void transaction.resolve(ctx.payload as RPCResponse)
          return
        }
        return
      }
      if (ctx.kind === 'subscribe') {
        const topic = ctx.topic as Topic
        const topics =
          ctx.from === 'server' ? this.upstreamTopics! : this.originTopics!
        void topics.set(topic, (topics.get(topic) ?? 0) + 1)
        if (this.isLeader && ctx.from === 'client')
          void this.sendUpstream(ctx as Context<Topic>)
        return
      }
      if (ctx.kind === 'unsubscribe') {
        const topic = ctx.topic as Topic
        const topics =
          ctx.from === 'server' ? this.upstreamTopics! : this.originTopics!
        const subscribers = topics.get(topic)
        if (!subscribers) return
        if (subscribers > 1) {
          void topics.set(topic, subscribers - 1)
          return
        }

        void topics.delete(topic)
        if (this.isLeader && ctx.from === 'client')
          void this.sendUpstream(ctx as Context<Topic>)
        return
      }
    }

    void this.broadcastChannel.postMessage({ kind: 'status' })

    if (this.webSocketUrl && navigator.onLine) void this.upstreamConnect()
    if (this.webSocketUrl) {
      void self.addEventListener('online', this.onlineHandler)
    }
  }

  invoke(payload: RPCRequest): boolean {
    if (this.isClosed || !this.isOnline) return false

    if (this.isLeader) return this.sendUpstream({ kind: 'invoke', payload })

    void this.broadcastChannel!.postMessage({ kind: 'invoke', payload })
    return true
  }

  /**
   * @example const withdraw = socket.offer(payload)
   */
  offer(payload: Offer): () => void {
    if (this.isClosed) return () => {}

    const id = crypto.randomUUID()
    void this.myOffers!.add(id)

    const ctx: Context<Offer> = { kind: 'offer', id, payload }

    if (this.isLeader) void this.sendUpstream(ctx)
    else void this.broadcastChannel!.postMessage(ctx)

    return () => {
      if (!this.myOffers?.delete(id)) return

      const ctx: Context<Offer> = { kind: 'withdraw', id }

      if (this.isLeader) void this.sendUpstream(ctx)
      else void this.broadcastChannel?.postMessage(ctx)
    }
  }

  gossip(topic: Topic, payload: Gossip): boolean {
    if (this.isClosed || !this.isOnline) return false

    const ctx: Context<Gossip> = {
      kind: 'gossip',
      from: 'client',
      topic,
      payload,
    }

    if (this.isLeader && this.upstreamTopics!.has(topic))
      void this.sendUpstream(ctx)

    void this.broadcastChannel!.postMessage(ctx)
    return true
  }

  transact(
    payload: RPCRequest,
    signal?: AbortSignal
  ): Promise<RPCResponse | false> {
    if (this.isClosed || !this.isOnline) return Promise.resolve(false)

    const transactionId = crypto.randomUUID()

    return new Promise<RPCResponse | false>((resolve, reject) => {
      const abortReason = () =>
        signal?.reason ??
        new DOMException('The operation was aborted.', 'AbortError')

      if (signal?.aborted) {
        void reject(abortReason())
        return
      }

      const handleAbort = () => {
        void this.myTransacts!.delete(transactionId)
        void signal?.removeEventListener('abort', handleAbort)

        void reject(abortReason())
      }

      this.myTransacts!.set(transactionId, {
        resolve,
        reject,
        cleanup: () => {
          void signal?.removeEventListener('abort', handleAbort)
        },
      })
      void signal?.addEventListener('abort', handleAbort, { once: true })

      const ctx: Context<RPCRequest> = {
        kind: 'transact',
        id: transactionId,
        phase: 'request',
        payload,
      }

      if (this.isLeader) {
        if (this.sendUpstream(ctx)) return

        void this.myTransacts!.delete(transactionId)
        void signal?.removeEventListener('abort', handleAbort)
        void resolve(false)
        return
      }

      void this.broadcastChannel?.postMessage(ctx)
    })
  }

  subscribe(topic: Topic): boolean {
    if (this.isClosed || !this.isOnline) return false
    if (this.myTopics!.has(topic)) return false

    void this.myTopics!.add(topic)

    const ctx: Context<Topic> = {
      kind: 'subscribe',
      topic,
      from: 'client',
    }

    void this.originTopics!.set(topic, (this.originTopics!.get(topic) ?? 0) + 1)
    if (this.isLeader) void this.sendUpstream(ctx)

    void this.broadcastChannel?.postMessage(ctx)
    return true
  }

  unsubscribe(topic: Topic): boolean {
    if (this.isClosed) return false
    const online = this.isOnline
    if (!this.myTopics!.delete(topic)) return false

    const ctx: Context<Topic> = {
      kind: 'unsubscribe',
      topic,
      from: 'client',
    }

    const subscribers = this.originTopics!.get(topic)
    if (subscribers === 1) {
      void this.originTopics!.delete(topic)
      if (this.isLeader) void this.sendUpstream(ctx)
    } else if (subscribers) {
      void this.originTopics!.set(topic, subscribers - 1)
    }

    void this.broadcastChannel?.postMessage(ctx)
    return online
  }

  //HELPER
  private sendUpstream(
    ctx: Context<Topic | Gossip | Offer | RPCRequest | RPCResponse>
  ): boolean {
    if (!this.isLeader || !this.webSocketUrl) return false

    if (!this.webSocket || this.webSocket.readyState !== WebSocket.OPEN) {
      if (self.navigator.onLine) {
        // Limit outbound queue to 64 entries
        if (this.upstreamQueue!.length >= 64) void this.upstreamQueue!.shift()
        void this.upstreamQueue!.push(ctx)
      }
      return false
    }

    try {
      void this.webSocket.send(encode(ctx))
      return true
    } catch {
      return false
    }
  }

  private flushUpstreamQueue() {
    if (!this.webSocket || this.webSocket.readyState !== WebSocket.OPEN) return

    while (this.upstreamQueue!.length > 0) {
      const message = this.upstreamQueue!.shift()
      if (!message) continue

      try {
        void this.webSocket.send(encode(message))
      } catch {
        void this.upstreamQueue!.unshift(message)
        return
      }
    }
  }

  private async upstreamConnect() {
    if (this.isClosed || this.isConnecting || !this.webSocketUrl) return
    if (!self.navigator.locks) return

    this.isConnecting = true

    try {
      while (!this.isClosed) {
        if (self.navigator.onLine !== true) return

        void (await self.navigator.locks.request(
          '@sovereignbase/origin-socket:web-lock',
          async (lockHandle) => {
            if (!lockHandle || this.isClosed) return
            this.isLeader = true
            const previousConnectionId = this.connectionId
            if (this.isOnline) {
              this.isOnline = false
              this.connectionId = null
              const reason = new DOMException(
                'The upstream connection was lost.',
                'NetworkError'
              )
              for (const transaction of this.myTransacts!.values()) {
                void transaction.cleanup()
                void transaction.reject(reason)
              }
              void this.myTransacts!.clear()
              void this.eventTarget.dispatchEvent(new CustomEvent('offline'))
            }
            void this.broadcastChannel!.postMessage({
              kind: 'offline',
              id: previousConnectionId,
            })

            let socket: WebSocket

            try {
              socket = new WebSocket(this.webSocketUrl)
            } catch {
              this.isLeader = false
              this.webSocket = null
              return
            }

            socket.binaryType = 'arraybuffer'
            this.webSocket = socket

            socket.onopen = () => {
              void this.flushUpstreamQueue()
              this.connectionId = crypto.randomUUID()
              if (!this.isOnline) {
                this.isOnline = true
                void this.eventTarget.dispatchEvent(new CustomEvent('online'))
              }
              void this.broadcastChannel!.postMessage({
                kind: 'online',
                id: this.connectionId!,
              })
            }

            socket.onmessage = (event: MessageEvent<ArrayBuffer>) => {
              const ctx = decode(event.data) as Context<unknown>
              if (ctx === undefined || typeof ctx !== 'object') return

              if (ctx?.kind === 'transact') {
                if (ctx.phase !== 'response') return

                const transaction = this.myTransacts!.get(ctx.id)

                if (transaction) {
                  void this.myTransacts!.delete(ctx.id)
                  void transaction.cleanup()
                  void transaction.resolve(ctx.payload as RPCResponse)
                  return
                }
                return void this.broadcastChannel!.postMessage(
                  ctx as Context<RPCResponse>
                )
              }

              if (ctx?.kind === 'answer') {
                if (this.myOffers!.has(ctx.id)) {
                  return void this.eventTarget.dispatchEvent(
                    new CustomEvent('answer', {
                      detail: ctx.payload as Answer,
                    })
                  )
                }
                return void this.broadcastChannel!.postMessage(ctx)
              }

              if (ctx?.kind === 'gossip') {
                if (ctx.from !== 'server') return

                void this.broadcastChannel!.postMessage(ctx)
                if (!this.myTopics!.has(ctx.topic as Topic)) return

                return void this.eventTarget.dispatchEvent(
                  new CustomEvent('gossip', {
                    detail: ctx.payload as Gossip,
                  })
                )
              }

              if (ctx?.kind === 'subscribe') {
                if (ctx?.from === 'server') {
                  const topicSubscribers = this.upstreamTopics!.get(
                    ctx.topic as Topic
                  )
                  void this.upstreamTopics!.set(
                    ctx.topic as Topic,
                    topicSubscribers ? topicSubscribers + 1 : 1
                  )
                  return void this.broadcastChannel!.postMessage(ctx)
                }
                return
              }

              if (ctx?.kind === 'unsubscribe') {
                if (ctx?.from === 'server') {
                  const topic = ctx.topic as Topic
                  const topicSubscribers = this.upstreamTopics!.get(topic)
                  if (!topicSubscribers) return

                  if (topicSubscribers === 1)
                    void this.upstreamTopics!.delete(topic)
                  else
                    void this.upstreamTopics!.set(topic, topicSubscribers - 1)

                  return void this.broadcastChannel!.postMessage(ctx)
                }
                return
              }
            }

            socket.onclose = () => {
              if (this.webSocket === socket) this.webSocket = null
              const connectionId = this.connectionId
              if (this.isOnline) {
                this.isOnline = false
                this.connectionId = null
                const reason = new DOMException(
                  'The upstream connection was lost.',
                  'NetworkError'
                )
                for (const transaction of this.myTransacts!.values()) {
                  void transaction.cleanup()
                  void transaction.reject(reason)
                }
                void this.myTransacts!.clear()
                void this.eventTarget.dispatchEvent(new CustomEvent('offline'))
              }
              void this.broadcastChannel?.postMessage({
                kind: 'offline',
                id: connectionId,
              })
              this.isLeader = false
            }

            await new Promise<void>((resolve) => {
              void socket.addEventListener('close', () => resolve(), {
                once: true,
              })
            })

            this.isLeader = false
            if (this.webSocket === socket) this.webSocket = null
          }
        ))

        if (this.isClosed || self.navigator.onLine !== true) return
        await new Promise<void>((resolve) => setTimeout(resolve, 10_000))
      }
    } finally {
      this.isConnecting = false
    }
  }
  // UTIL
  /**
   * Closes the client and releases its local and remote resources.
   */
  close(): void {
    if (this.isClosed) return

    if (this.isLeader && this.isOnline)
      void this.broadcastChannel!.postMessage({
        kind: 'offline',
        id: this.connectionId,
      })

    for (const id of this.myOffers!) {
      const ctx: Context<Offer> = { kind: 'withdraw', id }
      if (this.isLeader) void this.sendUpstream(ctx)
      else void this.broadcastChannel!.postMessage(ctx)
    }

    for (const topic of this.myTopics!) void this.unsubscribe(topic)

    this.isClosed = true
    void self.removeEventListener('online', this.onlineHandler)

    if (this.isLeader) {
      try {
        void this.webSocket?.close(1000, 'closed')
      } catch {}
      this.isLeader = false
      this.webSocket = null
    }

    try {
      for (const transaction of this.myTransacts!.values()) {
        void transaction.cleanup()
        void transaction.reject()
      }
      void this.myTopics!.clear()
      void this.myTransacts!.clear()
      void this.myOffers!.clear()
      void this.broadcastChannel!.close()
      this.myTopics = null
      this.myTransacts = null
      this.myOffers = null
      this.broadcastChannel = null
    } catch {}

    void this.originTopics!.clear()
    void this.upstreamTopics!.clear()
    this.upstreamQueue!.length = 0
    this.originTopics = null
    this.upstreamTopics = null
    this.upstreamQueue = null
  }

  /**
   * Appends an event listener for events whose type attribute value is `type`.
   *
   * @param type The event type to listen for.
   * @param listener The callback that receives the event.
   * @param options An options object that specifies characteristics about the event listener.
   */
  addEventListener<K extends keyof OriginSocketEventMap<Gossip, Answer>>(
    type: K,
    listener: OriginSocketEventListenerFor<Gossip, Answer, K> | null,
    options?: boolean | AddEventListenerOptions
  ): void {
    void this.eventTarget.addEventListener(
      type,
      listener as EventListenerOrEventListenerObject | null,
      options
    )
  }

  /**
   * Removes an event listener previously registered with {@link addEventListener}.
   *
   * @param type The event type to remove.
   * @param listener The callback to remove.
   * @param options An options object that specifies characteristics about the event listener.
   */
  removeEventListener<K extends keyof OriginSocketEventMap<Gossip, Answer>>(
    type: K,
    listener: OriginSocketEventListenerFor<Gossip, Answer, K> | null,
    options?: boolean | EventListenerOptions
  ): void {
    void this.eventTarget.removeEventListener(
      type,
      listener as EventListenerOrEventListenerObject | null,
      options
    )
  }
}

export function decodeContext<
  Topic extends string,
  Gossip,
  Offer,
  Answer,
  RPCRequest,
  RPCResponse,
>(
  buffer: ArrayBuffer
): Context<Topic | Gossip | Offer | Answer | RPCRequest | RPCResponse> | false {
  let ctx: unknown

  try {
    ctx = decode(buffer)
  } catch {
    return false
  }

  if (!ctx || typeof ctx !== 'object' || Array.isArray(ctx)) return false

  const value = ctx as Record<string, unknown>
  const hasPayload = Object.hasOwn(value, 'payload')

  switch (value.kind) {
    case 'invoke':
      if (!hasPayload) return false
      break
    case 'offer':
    case 'answer':
      if (typeof value.id !== 'string' || !hasPayload) return false
      break
    case 'withdraw':
      if (typeof value.id !== 'string') return false
      break
    case 'gossip':
      if (
        typeof value.topic !== 'string' ||
        (value.from !== 'client' && value.from !== 'server') ||
        !hasPayload
      )
        return false
      break
    case 'transact':
      if (
        typeof value.id !== 'string' ||
        (value.phase !== 'request' && value.phase !== 'response') ||
        !hasPayload
      )
        return false
      break
    case 'subscribe':
    case 'unsubscribe':
      if (
        typeof value.topic !== 'string' ||
        (value.from !== 'client' && value.from !== 'server')
      )
        return false
      break
    default:
      return false
  }

  return value as Context<
    Topic | Gossip | Offer | Answer | RPCRequest | RPCResponse
  >
}

export * from './types/index.js'
