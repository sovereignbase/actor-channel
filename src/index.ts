import { encode, decode } from '@msgpack/msgpack'
import {
  Context,
  OriginSocketEventMap,
  OriginSocketEventListenerFor,
  TransactionPromise,
} from './types/index.js'

export class OriginSocket<
  Topic extends string,
  Gossip,
  Signal,
  RPCRequest,
  RPCResponse,
> {
  private readonly eventTarget = new EventTarget()
  private readonly webSocketUrl: string
  private readonly onlineHandler = async () => {
    void this.upstreamConnect()
  }

  private isLeader: boolean = false
  private isClosed: boolean = false
  private isConnecting: boolean = false
  //
  private broadcastChannel: BroadcastChannel | null = null
  private webSocket: WebSocket | null = null

  // Leader keeps track of what others have ordered.
  private originTopics: Map<Topic, number> | null = null
  // A best effort offline queue mainly to allow calls before websocket is ready
  private upstreamQueue: Array<
    Context<RPCRequest | RPCResponse | Gossip | Topic>
  > | null = null
  // Leader keeps track of what upstream has ordered.
  private upstreamTopics: Map<Topic, number> | null = null

  // Pending transaction promises of this instance
  private myTransacts: Map<string, TransactionPromise<RPCResponse>> | null =
    null
  // Topics subscribed by  this instance
  private myTopics: Set<Topic> | null = null

  constructor(webSocketUrl: string = '') {
    this.webSocketUrl = webSocketUrl
    this.broadcastChannel = new BroadcastChannel(
      '@sovereignbase/origin-socket:channel'
    )
    this.myTopics = new Set()
    this.myTransacts = new Map()

    this.broadcastChannel.onmessage = (
      event: MessageEvent<Context<RPCRequest | RPCResponse | Gossip | Signal>>
    ) => {
      const ctx = event.data
      if (!ctx) return

      if (ctx.kind === 'invoke') {
        if (!this.isLeader) return
        void this.sendUpstream(ctx as Context<RPCRequest>)
        return
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
          return
        }
        return
      }
      if (ctx.kind === 'subscribe') {
      }
    }

    if (this.webSocketUrl && navigator.onLine) void this.upstreamConnect()
    if (this.webSocketUrl) {
      void self.addEventListener('online', this.onlineHandler)
    }
  }

  invoke(payload: RPCRequest): void {
    if (this.isClosed) return

    if (this.isLeader)
      return void this.sendUpstream({ kind: 'invoke', payload })

    return void this.broadcastChannel!.postMessage({ kind: 'invoke', payload })
  }

  gossip(topic: Topic, payload: Gossip): void {
    if (this.isClosed) return

    const ctx: Context<Gossip> = {
      kind: 'gossip',
      from: 'client',
      topic,
      payload,
    }

    if (this.isLeader) void this.sendUpstream(ctx)

    return void this.broadcastChannel!.postMessage(ctx)
  }

  /**
   * Sends a RPCRequest to the base station and resolves with the corresponding response message.
   *
   * @param message The message to send.
   * @param options Options that control cancellation and stale follower cleanup.
   * @returns A promise that resolves with the response message, or `false` when the RPCRequest cannot be issued.
   */
  transact(
    payload: RPCRequest,
    signal?: AbortSignal
  ): Promise<RPCResponse | false> {
    if (this.isClosed) return Promise.resolve(false)

    const transactionId = crypto.randomUUID()

    return new Promise<RPCResponse | false>((resolve, reject) => {
      const abortReason = () =>
        signal?.reason ??
        new DOMException('The operation was aborted.', 'AbortError')

      if (signal?.aborted) {
        void reject(abortReason())
        return
      }

      if (!this.webSocketUrl || self.navigator.onLine !== true) {
        void resolve(false)
        return
      }

      if (
        this.isLeader &&
        (!this.webSocket || this.webSocket.readyState !== WebSocket.OPEN)
      ) {
        void resolve(false)
        return
      }

      const handleAbort = () => {
        void this.myTransacts!.delete(transactionId)
        signal?.removeEventListener('abort', handleAbort)

        reject(abortReason())
      }

      this.myTransacts!.set(transactionId, {
        resolve,
        reject,
        cleanup: () => {
          signal?.removeEventListener('abort', handleAbort)
        },
      })
      signal?.addEventListener('abort', handleAbort, { once: true })

      const ctx: Context<RPCRequest> = {
        kind: 'transact',
        id: transactionId,
        phase: 'request',
        payload,
      }

      if (this.isLeader) {
        return void this.sendUpstream(ctx)
      }

      return void this.broadcastChannel?.postMessage(ctx)
    })
  }

  subscribe(topic: Topic): void {
    if (this.isClosed) return

    void this.myTopics!.add(topic)

    const ctx: Context<Topic> = {
      kind: 'subscribe',
      topic,
      from: 'client',
    }

    if (this.isLeader) {
      const topicSubscibers = this.originTopics!.get(topic)
      void this.originTopics!.set(
        topic,
        topicSubscibers ? topicSubscibers + 1 : 1
      )
      void this.sendUpstream(ctx)
    }

    return void this.broadcastChannel?.postMessage(ctx)
  }

  unsubscribe(topic: Topic): void {
    if (this.isClosed) return

    void this.myTopics!.delete(topic)

    const ctx: Context<Topic> = {
      kind: 'unsubscribe',
      topic,
      from: 'client',
    }

    if (this.isLeader) {
      let topicSubscibers = this.originTopics!.get(topic)
      if (topicSubscibers) {
        topicSubscibers -= 1

        if (topicSubscibers == 0) {
          this.originTopics!.delete(topic)
          return void this.sendUpstream(ctx)
        } else {
          return void this.originTopics!.set(topic, topicSubscibers)
        }
      }
    }

    return void this.broadcastChannel?.postMessage(ctx)
  }

  //HELPER
  private sendUpstream(
    ctx: Context<Topic | Gossip | RPCRequest | RPCResponse>
  ) {
    if (!this.isLeader || !this.webSocketUrl) return

    if (!this.webSocket || this.webSocket.readyState !== WebSocket.OPEN) {
      if (self.navigator.onLine) {
        // Limit outbound queue to 64 entries
        if (this.upstreamQueue!.length >= 64) this.upstreamQueue!.shift()
        this.upstreamQueue!.push(ctx)
      }
      return
    }

    try {
      this.webSocket.send(encode(ctx))
    } catch {}
  }

  private flushUpstreamQueue() {
    if (!this.webSocket || this.webSocket.readyState !== WebSocket.OPEN) return

    while (this.upstreamQueue!.length > 0) {
      const message = this.upstreamQueue!.shift()
      if (!message) continue

      try {
        this.webSocket.send(encode(message))
      } catch {
        this.upstreamQueue!.unshift(message)
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

        await self.navigator.locks.request(
          '@sovereignbase/origin-socket:leader',
          { ifAvailable: true },
          async (lockHandle) => {
            if (!lockHandle || this.isClosed) return
            this.isLeader = true

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
              this.flushUpstreamQueue()
            }

            socket.onmessage = (event: MessageEvent<ArrayBuffer>) => {
              const ctx = decode(event.data) as Context<unknown>
              if (ctx === undefined || typeof ctx !== 'object') return

              if (ctx?.kind === 'transact') {
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

              if (ctx?.kind === 'gossip') {
              }

              if (ctx?.kind === 'subscribe') {
                if (ctx?.from === 'server') {
                  return void this.upstreamTopics!.set(ctx.topic as Topic, 1)
                }
                return
              }
            }

            socket.onclose = () => {
              if (this.webSocket === socket) this.webSocket = null
              this.isLeader = false
            }

            await new Promise<void>((resolve) => {
              socket.addEventListener('close', () => resolve(), { once: true })
            })

            this.isLeader = false
            if (this.webSocket === socket) this.webSocket = null
          }
        )

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
    this.isClosed = true
    void self.removeEventListener('online', this.onlineHandler)

    try {
      void this.myTopics!.clear()
      void this.myTransacts!.clear()
      void this.broadcastChannel!.close()
      this.myTopics = null
      this.myTransacts = null
      this.broadcastChannel = null
    } catch {}

    if (this.isLeader) {
      try {
        void this.webSocket!.close(1000, 'closed')
        void this.upstreamTopics!.clear()
        this.isLeader = false
        this.upstreamQueue!.length = 0
        this.webSocket = null
        this.upstreamQueue = null
      } catch {}
    }
  }

  /**
   * Appends an event listener for events whose type attribute value is `type`.
   *
   * @param type The event type to listen for.
   * @param listener The callback that receives the event.
   * @param options An options object that specifies characteristics about the event listener.
   */
  addEventListener<K extends keyof OriginSocketEventMap<Gossip | Signal>>(
    type: K,
    listener: OriginSocketEventListenerFor<Gossip | Signal, K> | null,
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
  removeEventListener<K extends keyof OriginSocketEventMap<Gossip | Signal>>(
    type: K,
    listener: OriginSocketEventListenerFor<Gossip | Signal, K> | null,
    options?: boolean | EventListenerOptions
  ): void {
    void this.eventTarget.removeEventListener(
      type,
      listener as EventListenerOrEventListenerObject | null,
      options
    )
  }
}

export function serverHandle<
  Topic extends string,
  Gossip,
  Signal,
  RPCRequest,
  RPCResponse,
>(
  buffer: ArrayBuffer
): Context<Topic | Gossip | Signal | RPCRequest | RPCResponse> | false {
  const ctx = decode(buffer) as Context<
    Topic | Gossip | RPCRequest | RPCResponse
  >

  if (!ctx) return false
  return ctx
}

export * from './types/index.js'
