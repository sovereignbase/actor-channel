import { encode, decode } from '@msgpack/msgpack'
import { Context, TransactOptions } from './types/index.js'

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
  private broadcastChannel: BroadcastChannel | null = null
  private webSocket: WebSocket | null = null
  private isLeader: boolean = false
  private isClosed: boolean = false
  private isConnecting: boolean = false
  private readonly originTopics: Set<Topic> = new Set()

  private readonly upstreamQueue: Array<Context<RPCRequest>> | undefined
  private readonly upstreamTopics: Set<Topic> | undefined
  private readonly upstreamTransacts = new Map<string, string>()

  private readonly myTransacts = new Map<string, Promise<RPCResponse>>()
  private readonly myTopics: Set<Topic> = new Set()

  /**
   * Initializes a new {@link StationClient} instance.
   *
   * @param webSocketUrl The base station WebSocket URL. When omitted, the instance operates in local-only mode.
   */
  constructor(webSocketUrl: string = '') {
    this.webSocketUrl = webSocketUrl

    this.broadcastChannel = new BroadcastChannel(
      '@sovereignbase/origin-socket:channel'
    )
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
        if (ctx.from === 'client' && this.isLeader) void this.sendUpstream(ctx)

        if (!this.myTopics.has(ctx.topic as Topic)) return

        return void this.eventTarget.dispatchEvent(
          new CustomEvent('gossip', { detail: ctx.payload as Gossip })
        )
      }

      if (ctx.kind === 'transact') {
        if (ctx.phase === 'request') {
          if (!this.isLeader) return
          else this.sendUpstream(ctx)
        }
        if (ctx.phase === 'response') {
          const transaction = this.myTransacts.get(ctx.id)
          if (!transaction) return

          this.myTransacts.delete(ctx.id)
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

    return void this.broadcastChannel?.postMessage({ kind: 'invoke', payload })
  }

  gossip(topic: Topic, payload: Gossip): void {
    if (this.isClosed) return

    const context: Context<Gossip> = {
      kind: 'gossip',
      from: 'client',
      topic,
      payload,
    }

    if (this.isLeader) void this.sendUpstream(context)

    return void this.broadcastChannel?.postMessage(context)
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
    options?: TransactOptions
  ): Promise<RPCResponse | false> {
    if (this.isClosed) return Promise.resolve(false)

    const transactionId = crypto.randomUUID()
    const { signal, ttlMs } = options

    return new Promise<RPCResponse | false>((resolve, reject) => {
      const abortReason = () =>
        signal?.reason ??
        new DOMException('The operation was aborted.', 'AbortError')

      if (signal?.aborted) {
        reject(abortReason())
        return
      }

      if (!this.webSocketUrl || self.navigator.onLine !== true) {
        resolve(false)
        return
      }

      if (
        this.isLeader &&
        (!this.webSocket || this.webSocket.readyState !== WebSocket.OPEN)
      ) {
        resolve(false)
        return
      }

      const handleAbort = () => {
        this.pendingfetchs.delete(id)
        const pendingTarget = this.pendingfetchTargets.get(id)
        if (pendingTarget) clearTimeout(pendingTarget.timeoutId)
        this.pendingfetchTargets.delete(id)
        signal?.removeEventListener('abort', handleAbort)

        if (!this.isLeader) {
          this.broadcastChannel?.postMessage({ kind: 'fetch-abort', id })
        }

        reject(abortReason())
      }

      this.pendingfetchs.set(id, {
        resolve,
        reject,
        cleanup: () => {
          signal?.removeEventListener('abort', handleAbort)
        },
      })
      signal?.addEventListener('abort', handleAbort, { once: true })

      if (this.isLeader) {
        this.sendUpstream(['station-client-RPCRequest', id, message])
        return
      }

      this.broadcastChannel?.postMessage({
        kind: 'fetch',
        id,
        source: this.instanceId,
        ttlMs,
        message,
      })
    })
  }

  subscribe(topic: Topic): void {
    if (this.isClosed) return

    void this.myTopics.add(topic)

    const context: Context<Topic> = {
      kind: 'subscribe',
      topic,
      from: 'client',
    }

    if (this.isLeader) void this.sendUpstream(context)

    return void this.broadcastChannel?.postMessage(context)
  }

  unsubscribe(topic: Topic): void {
    if (this.isClosed) return

    void this.myTopics.delete(topic)

    const context: Context<Topic> = {
      kind: 'unsubscribe',
      topic,
      from: 'client',
    }

    if (this.isLeader) {
      void this.sendUpstream(context)
    }

    return void this.broadcastChannel?.postMessage(context)
  }

  //HELPER
  private sendUpstream(
    context: Context<RPCRequest | RPCResponse | Gossip | Topic>
  ) {
    if (!this.isLeader || !this.webSocketUrl) return

    if (!this.webSocket || this.webSocket.readyState !== WebSocket.OPEN) {
      if (self.navigator.onLine) {
        // Limit outbound queue to 64 entries
        if (this.upstreamQueue!.length >= 64) this.upstreamQueue!.shift()
        this.upstreamQueue!.push(context)
      }
      return
    }

    try {
      this.webSocket.send(encode(context))
    } catch {}
  }

  private flushUpstreamQueue() {
    if (!this.webSocket || this.webSocket.readyState !== WebSocket.OPEN) return

    while (this.upstreamQueue.length > 0) {
      const message = this.upstreamQueue.shift()
      if (!message) continue

      try {
        this.webSocket.send(encode(message))
      } catch {
        this.upstreamQueue.unshift(message)
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
              const message = decode(event.data)
              if (!message) return

              if (
                Array.isArray(message) &&
                message[0] === 'station-client-response' &&
                typeof message[1] === 'string'
              ) {
                const id = message[1]
                const pendingTarget = this.pendingfetchTargets.get(id)
                if (pendingTarget) {
                  clearTimeout(pendingTarget.timeoutId)
                  this.pendingfetchTargets.delete(id)

                  this.broadcastChannel?.postMessage({
                    kind: 'fetch-response',
                    id,
                    target: pendingTarget.target,
                    message: message[2] as T,
                  })
                  return
                }

                const pending = this.pendingfetchs.get(id)
                if (!pending) return

                this.pendingfetchs.delete(id)
                pending.cleanup()
                pending.resolve(message[2] as T)
                return
              }

              this.eventTarget.dispatchEvent(
                new CustomEvent('message', { detail: message })
              )

              this.broadcastChannel?.postMessage({
                kind: 'post',
                message: message as T,
              })
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
    const wasLeader = this.isLeader
    const broadcastChannel = this.broadcastChannel
    this.isClosed = true
    self.removeEventListener('online', this.onlineHandler)

    if (!wasLeader) {
      for (const id of this.myTransacts.keys()) {
        try {
          broadcastChannel?.postMessage({ kind: 'fetch-abort', id })
        } catch {}
      }
    }

    try {
      broadcastChannel?.close()
    } catch {}
    try {
      this.webSocket?.close(1000, 'closed')
    } catch {}

    this.broadcastChannel = null
    this.webSocket = null
    this.isLeader = false
    this.upstreamQueue.length = 0
    for (const pending of this.pendingfetchs.values()) {
      pending.cleanup()
      pending.reject(new Error('Station client closed'))
    }
    this.pendingfetchs.clear()
    for (const pendingTarget of this.pendingfetchTargets.values()) {
      clearTimeout(pendingTarget.timeoutId)
    }
    this.pendingfetchTargets.clear()
  }

  /**
   * Appends an event listener for events whose type attribute value is `type`.
   *
   * @param type The event type to listen for.
   * @param listener The callback that receives the event.
   * @param options An options object that specifies characteristics about the event listener.
   */
  addEventListener<K extends keyof OriginSocketEventMap<T>>(
    type: K,
    listener: OriginSocketEventListenerFor<T, K> | null,
    options?: boolean | AddEventListenerOptions
  ): void {
    this.eventTarget.addEventListener(
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
  removeEventListener<K extends keyof OriginSocketEventMap<T>>(
    type: K,
    listener: OriginSocketEventListenerFor<T, K> | null,
    options?: boolean | EventListenerOptions
  ): void {
    this.eventTarget.removeEventListener(
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
