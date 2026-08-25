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
  RPCRequest,
  RPCResponse,
> {
  private readonly eventTarget = new EventTarget()
  private readonly webSocketUrl: string
  private readonly onlineHandler = async () => {
    void this.upstreamConnect()
  }
  private broadcastChannel: BroadcastChannel = new BroadcastChannel(
    '@sovereignbase/origin-socket:channel'
  )
  private webSocket: WebSocket | null = null
  private isLeader: boolean = false
  private isClosed: boolean = false
  private isConnecting: boolean = false

  private readonly originTopics: Map<Topic, number> | undefined

  private readonly upstreamQueue:
    Array<Context<RPCRequest | RPCResponse | Gossip | Topic>> | undefined
  private readonly upstreamTopics: Map<Topic, number> | undefined

  private readonly myTransacts = new Map<
    string,
    TransactionPromise<RPCResponse>
  >()
  private readonly myTopics: Set<Topic> = new Set()

  /**
   * Initializes a new {@link StationClient} instance.
   *
   * @param webSocketUrl The base station WebSocket URL. When omitted, the instance operates in local-only mode.
   */
  constructor(webSocketUrl: string = '') {
    this.webSocketUrl = webSocketUrl

    this.broadcastChannel.onmessage = (
      event: MessageEvent<Context<RPCRequest | RPCResponse | Gossip>>
    ) => {
      const ctx = event.data
      if (!ctx) return

      if (ctx.kind === 'invoke') {
        if (!this.isLeader) return
        void this.sendUpstream(ctx)
        return
      }

      if (ctx.kind === 'gossip') {
        if (
          ctx.from === 'client' &&
          this.isLeader &&
          this.upstreamTopics!.has(ctx.topic as Topic)
        )
          void this.sendUpstream(ctx)

        if (!this.myTopics.has(ctx.topic as Topic)) return

        return void this.eventTarget.dispatchEvent(
          new CustomEvent('gossip', { detail: ctx.payload as Gossip })
        )
      }

      if (ctx.kind === 'transact') {
        if (ctx.phase === 'request') {
          if (!this.isLeader) return
          else void this.sendUpstream(ctx)
        }
        if (ctx.phase === 'response') {
          const transaction = this.myTransacts.get(ctx.id)
          if (!transaction) return

          void this.myTransacts.delete(ctx.id)
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

  /**
   * Broadcasts a message to other same-origin contexts and opportunistically forwards it to the base station.
   *
   * @param message The message to broadcast.
   */
  invoke(payload: RPCRequest): void {
    if (this.isClosed) return

    if (this.isLeader)
      return void this.sendUpstream({ kind: 'invoke', payload })

    return void this.broadcastChannel.postMessage({ kind: 'invoke', payload })
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

    return void this.broadcastChannel.postMessage(ctx)
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
        void this.myTransacts.delete(transactionId)
        signal?.removeEventListener('abort', handleAbort)

        reject(abortReason())
      }

      this.myTransacts.set(transactionId, {
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
        from: 'client',
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

    void this.myTopics.add(topic)

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

    void this.myTopics.delete(topic)

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
    ctx: Context<RPCRequest | RPCResponse | Gossip | Topic>
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
                const transaction = this.myTransacts.get(ctx.id)

                if (transaction) {
                  void this.myTransacts.delete(ctx.id)
                  void transaction.cleanup()
                  void transaction.resolve(ctx.payload as RPCResponse)
                  return
                }
                return void this.broadcastChannel.postMessage(
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
    this.isClosed = true
    void self.removeEventListener('online', this.onlineHandler)

    try {
      void this.myTopics.clear()
      void this.myTransacts.clear()
      void this.broadcastChannel.close()
    } catch {}

    if (this.isLeader) {
      try {
        void this.webSocket?.close(1000, 'closed')
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
  addEventListener<K extends keyof OriginSocketEventMap<Gossip>>(
    type: K,
    listener: OriginSocketEventListenerFor<Gossip, K> | null,
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
  removeEventListener<K extends keyof OriginSocketEventMap<Gossip>>(
    type: K,
    listener: OriginSocketEventListenerFor<Gossip, K> | null,
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
  RPCRequest,
  RPCResponse,
>(buffer: ArrayBuffer) {
  const ctx = decode(buffer) as Context<
    Topic | Gossip | RPCRequest | RPCResponse
  >
}

export * from './types/index.js'
