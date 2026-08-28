import { encode, decode } from '@msgpack/msgpack'
import type {
  Context,
  OriginSocketEventMap,
  OriginSocketEventListenerFor,
  RequestPromise,
  ChannelMessage,
} from './types/index.js'

export class OriginSocket<
  Topic extends string,
  Message,
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

  private originTopics: Map<Topic, number> | null = null
  // A best effort disconnected queue mainly to allow calls before websocket is ready
  private upstreamQueue: Array<
    Context<Topic | Message | RPCRequest | RPCResponse>
  > | null = null
  // Replicated topics ordered by upstream.
  private upstreamTopics: Map<Topic, number> | null = null

  // Pending request promises of this instance
  private myRequests: Map<string, RequestPromise<RPCResponse>> | null = null
  private myOffers: Set<string> | null = null
  // Topics subscribed by  this instance
  private myTopics: Set<Topic> | null = null

  /**
   * Creates an OriginSocket instance.
   *
   * @param webSocketUrl - Upstream WebSocket URL. An empty URL creates an
   *   instance that can participate locally but cannot own the upstream
   *   connection.
   */
  constructor(webSocketUrl: string = '') {
    this.webSocketUrl = webSocketUrl
    this.broadcastChannel = new BroadcastChannel(
      '@sovereignbase/origin-socket:broadcast-channel'
    )
    this.myTopics = new Set()
    this.myRequests = new Map()
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
            kind: 'connected',
            id: this.connectionId!,
          })
        return
      }
      if (ctx.kind === 'connected') {
        this.connectionId = ctx.id
        if (!this.isOnline) {
          this.isOnline = true
          void this.eventTarget.dispatchEvent(new CustomEvent('connected'))
        }
        return
      }
      if (ctx.kind === 'disconnected') {
        if (ctx.id !== this.connectionId) return
        if (!this.isOnline) return
        this.isOnline = false
        this.connectionId = null
        const reason = new DOMException(
          'The upstream connection was lost.',
          'NetworkError'
        )
        for (const request of this.myRequests!.values()) {
          void request.cleanup()
          void request.reject(reason)
        }
        void this.myRequests!.clear()
        void this.eventTarget.dispatchEvent(new CustomEvent('disconnected'))
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
          new CustomEvent('answer', { detail: ctx.detail as Answer })
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
          new CustomEvent('gossip', { detail: ctx.detail as Gossip })
        )
      }

      if (ctx.kind === 'request') {
        if (ctx.phase === 'request') {
          if (!this.isLeader) return
          else void this.sendUpstream(ctx as Context<RPCRequest>)
        }
        if (ctx.phase === 'response') {
          const request = this.myRequests!.get(ctx.id)
          if (!request) return

          void this.myRequests!.delete(ctx.id)
          void request.cleanup()
          void request.resolve(ctx.detail as RPCResponse)
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
      void self.addEventListener('connected', this.onlineHandler)
    }
  }

  /**
   * Sends a fire-and-forget request upstream.
   *
   * @param detail - Request detail.
   * @returns `true` when the operation was accepted while the shared
   *   connection was online; this is not a server acknowledgement.
   */
  invoke(detail: RPCRequest): boolean {
    if (this.isClosed || !this.isOnline) return false

    if (this.isLeader) return this.sendUpstream({ kind: 'invoke', detail })

    void this.broadcastChannel!.postMessage({ kind: 'invoke', detail })
    return true
  }

  /**
   * Publishes an offer that remains active until withdrawn or the instance is
   * closed.
   *
   * Offer identifiers are managed internally. Matching answers are emitted as
   * `answer` events on this instance.
   *
   * @param detail - Offer detail.
   * @returns An idempotent function that withdraws the offer.
   * @example
   * ```ts
   * const withdraw = socket.offer(detail)
   * withdraw()
   * ```
   */
  offer(detail: Offer): () => void {
    if (this.isClosed) return () => {}

    const id = crypto.randomUUID()
    void this.myOffers!.add(id)

    const ctx: Context<Offer> = { kind: 'offer', id, detail }

    if (this.isLeader) void this.sendUpstream(ctx)
    else void this.broadcastChannel!.postMessage(ctx)

    return () => {
      if (!this.myOffers?.delete(id)) return

      const ctx: Context<Offer> = { kind: 'withdraw', id }

      if (this.isLeader) void this.sendUpstream(ctx)
      else void this.broadcastChannel?.postMessage(ctx)
    }
  }

  /**
   * Publishes a detail to a topic.
   *
   * @param topic - Destination topic.
   * @param detail - Gossip detail.
   * @returns `true` when the operation was accepted while the shared
   *   connection was online; this is not a server acknowledgement.
   */
  gossip(topic: Topic, detail: Gossip): boolean {
    if (this.isClosed || !this.isOnline) return false

    const ctx: Context<Gossip> = {
      kind: 'gossip',
      from: 'client',
      topic,
      detail,
    }

    if (this.isLeader && this.upstreamTopics!.has(topic))
      void this.sendUpstream(ctx)

    void this.broadcastChannel!.postMessage(ctx)
    return true
  }

  /**
   * Sends a request and waits for its matching response.
   *
   * Pending requests are not replayed. They reject with a `NetworkError`
   * when the shared connection changes, even when the server may already have
   * processed the request.
   *
   * @param detail - Request detail.
   * @param signal - Optional signal used to abort the request.
   * @returns The response detail, or `false` when the operation cannot be sent
   *   because the instance is closed or the shared connection is disconnected.
   * @throws The abort reason when `signal` is aborted.
   * @throws A `DOMException` named `NetworkError` if the connection is lost.
   */
  request(
    detail: RPCRequest,
    signal?: AbortSignal
  ): Promise<RPCResponse | false> {
    if (this.isClosed || !this.isOnline) return Promise.resolve(false)

    const requestId = crypto.randomUUID()

    return new Promise<RPCResponse | false>((resolve, reject) => {
      const abortReason = () =>
        signal?.reason ??
        new DOMException('The operation was aborted.', 'AbortError')

      if (signal?.aborted) {
        void reject(abortReason())
        return
      }

      const handleAbort = () => {
        void this.myRequests!.delete(requestId)
        void signal?.removeEventListener('abort', handleAbort)

        void reject(abortReason())
      }

      this.myRequests!.set(requestId, {
        resolve,
        reject,
        cleanup: () => {
          void signal?.removeEventListener('abort', handleAbort)
        },
      })
      void signal?.addEventListener('abort', handleAbort, { once: true })

      const ctx: Context<RPCRequest> = {
        kind: 'request',
        id: requestId,
        phase: 'request',
        detail,
      }

      if (this.isLeader) {
        if (this.sendUpstream(ctx)) return

        void this.myRequests!.delete(requestId)
        void signal?.removeEventListener('abort', handleAbort)
        void resolve(false)
        return
      }

      void this.broadcastChannel?.postMessage(ctx)
    })
  }

  /**
   * Subscribes this instance to a topic.
   *
   * Repeated local subscriptions are ignored. The shared upstream subscription
   * is removed only after every local subscriber has unsubscribed.
   *
   * @param topic - Topic to subscribe to.
   * @returns `true` when a new subscription was accepted while online, or
   *   `false` when closed, disconnected, or already subscribed.
   */
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

  /**
   * Removes this instance's subscription to a topic.
   *
   * @param topic - Topic to unsubscribe from.
   * @returns `true` when the subscription existed and the shared connection was
   *   online. Returns `false` when closed, not subscribed, or disconnected.
   */
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

  /** Sends or queues a context for the connection-owning instance. */
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

  /** Sends queued contexts in insertion order while the socket is open. */
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

  /** Acquires leadership and maintains the shared upstream connection. */
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
              for (const request of this.myRequests!.values()) {
                void request.cleanup()
                void request.reject(reason)
              }
              void this.myRequests!.clear()
              void this.eventTarget.dispatchEvent(
                new CustomEvent('disconnected')
              )
            }
            void this.broadcastChannel!.postMessage({
              kind: 'disconnected',
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

              if (ctx?.kind === 'request') {
                if (ctx.phase !== 'response') return

                const request = this.myRequests!.get(ctx.id)

                if (request) {
                  void this.myRequests!.delete(ctx.id)
                  void request.cleanup()
                  void request.resolve(ctx.detail as RPCResponse)
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
                      detail: ctx.detail as Answer,
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
                    detail: ctx.detail as Gossip,
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
                for (const request of this.myRequests!.values()) {
                  void request.cleanup()
                  void request.reject(reason)
                }
                void this.myRequests!.clear()
                void this.eventTarget.dispatchEvent(
                  new CustomEvent('disconnected')
                )
              }
              void this.broadcastChannel?.postMessage({
                kind: 'disconnected',
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

  /**
   * Closes the instance and releases its local and shared resources.
   *
   * Active offers and subscriptions are withdrawn, and pending requests
   * are rejected. Calling `close` more than once has no effect.
   */
  close(): void {
    if (this.isClosed) return

    if (this.isLeader && this.isOnline)
      void this.broadcastChannel!.postMessage({
        kind: 'disconnected',
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
      for (const request of this.myRequests!.values()) {
        void request.cleanup()
        void request.reject()
      }
      void this.myTopics!.clear()
      void this.myRequests!.clear()
      void this.myOffers!.clear()
      void this.broadcastChannel!.close()
      this.myTopics = null
      this.myRequests = null
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
   * Registers a typed OriginSocket event listener.
   *
   * @param type - Event type to listen for.
   * @param listener - Callback or listener object that receives the event.
   * @param options - Event listener options.
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
   * @param type - Event type to remove.
   * @param listener - Callback or listener object to remove.
   * @param options - Event listener options.
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

/**
 * Decodes and validates a MessagePack-encoded OriginSocket context.
 *
 * Validation covers the context discriminator and routing fields. Details are
 * intentionally opaque and are returned as the caller-provided generic types.
 *
 * @typeParam Topic - Topic name type.
 * @typeParam Gossip - Gossip detail type.
 * @typeParam Offer - Offer detail type.
 * @typeParam Answer - Answer detail type.
 * @typeParam RPCRequest - Request and invocation detail type.
 * @typeParam RPCResponse - Response detail type.
 * @param buffer - MessagePack-encoded context.
 * @returns The decoded context, or `false` when decoding or validation fails.
 */
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
  const hasDetail = Object.hasOwn(value, 'detail')

  switch (value.kind) {
    case 'invoke':
      if (!hasDetail) return false
      break
    case 'offer':
    case 'answer':
      if (typeof value.id !== 'string' || !hasDetail) return false
      break
    case 'withdraw':
      if (typeof value.id !== 'string') return false
      break
    case 'gossip':
      if (
        typeof value.topic !== 'string' ||
        (value.from !== 'client' && value.from !== 'server') ||
        !hasDetail
      )
        return false
      break
    case 'request':
      if (
        typeof value.id !== 'string' ||
        (value.phase !== 'request' && value.phase !== 'response') ||
        !hasDetail
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
