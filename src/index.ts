import { encode, decode } from '@msgpack/msgpack'

export class OriginSocket<T> {
  private readonly eventTarget = new EventTarget()
  private readonly lockName: string
  private readonly channelName: string
  private readonly webSocketUrl: string
  private readonly instanceId = self.crypto.randomUUID()
  private readonly onlineHandler = () => {
    void this.upstreamConnect()
  }
  private broadcastChannel: BroadcastChannel | null = null
  private webSocket: WebSocket | null = null
  private isLeader: boolean = false
  private isClosed: boolean = false
  private isConnecting: boolean = false
  private readonly upstreamQueue: StationClientRemoteMessageShape<T>[] = []
  private readonly pendingfetchs = new Map<
    string,
    StationClientPendingFetch<T>
  >()
  private readonly pendingfetchTargets = new Map<
    string,
    StationClientPendingFetchTarget
  >()

  /**
   * Initializes a new {@link StationClient} instance.
   *
   * @param webSocketUrl The base station WebSocket URL. When omitted, the instance operates in local-only mode.
   */
  constructor(webSocketUrl: string = '') {
    this.webSocketUrl = webSocketUrl
    this.channelName = `origin-channel-lock::${this.webSocketUrl}`
    this.lockName = `origin-channel-lock::${this.webSocketUrl}`

    this.broadcastChannel = new BroadcastChannel(this.channelName)
    this.broadcastChannel.onmessage = (
      event: MessageEvent<StationClientLocalMessageShape<T>>
    ) => {
      const envelope = event.data
      if (!envelope) return

      if (envelope.kind === 'post') {
        this.eventTarget.dispatchEvent(
          new CustomEvent('message', { detail: envelope.message })
        )
        if (!this.isLeader) return

        this.sendUpstream(envelope.message)
        return
      }

      if (envelope.kind === 'fetch-response') {
        if (envelope.target !== this.instanceId) return

        const pending = this.pendingfetchs.get(envelope.id)
        if (!pending) return

        this.pendingfetchs.delete(envelope.id)
        pending.cleanup()
        pending.resolve(envelope.message)
        return
      }

      if (envelope.kind === 'fetch-abort') {
        if (!this.isLeader) return

        const pendingTarget = this.pendingfetchTargets.get(envelope.id)
        if (pendingTarget) clearTimeout(pendingTarget.timeoutId)
        this.pendingfetchTargets.delete(envelope.id)
        return
      }

      if (!this.isLeader) return

      if (
        !this.webSocketUrl ||
        self.navigator.onLine !== true ||
        !this.webSocket ||
        this.webSocket.readyState !== WebSocket.OPEN
      ) {
        this.broadcastChannel?.postMessage({
          kind: 'fetch-response',
          id: envelope.id,
          target: envelope.source,
          message: false,
        })
        return
      }

      const pendingTarget = this.pendingfetchTargets.get(envelope.id)
      if (pendingTarget) clearTimeout(pendingTarget.timeoutId)

      this.pendingfetchTargets.set(envelope.id, {
        target: envelope.source,
        timeoutId: setTimeout(() => {
          this.pendingfetchTargets.delete(envelope.id)
        }, envelope.ttlMs ?? 30_000),
      })
      this.sendUpstream([
        'station-client-request',
        envelope.id,
        envelope.message,
      ])
    }

    if (this.webSocketUrl && navigator.onLine) void this.opportunisticConnect()
    if (this.webSocketUrl) {
      self.addEventListener('online', this.onlineHandler)
    }
  }
  gossip(): void {}

  /**
   * Broadcasts a message to other same-origin contexts and opportunistically forwards it to the base station.
   *
   * @param message The message to broadcast.
   */
  invoke(message: T) {
    if (this.isClosed) return

    this.broadcastChannel?.postMessage({ kind: 'post', message })
    this.sendUpstream(message)
  }

  /**
   * Sends a request to the base station and resolves with the corresponding response message.
   *
   * @param message The message to send.
   * @param options Options that control cancellation and stale follower cleanup.
   * @returns A promise that resolves with the response message, or `false` when the request cannot be issued.
   */
  transact(
    message: T,
    options: StationClientFetchOptions = {}
  ): Promise<T | false> {
    if (this.isClosed) return Promise.resolve(false)

    const id = self.crypto.randomUUID()
    const { signal, ttlMs } = options

    return new Promise<T | false>((resolve, reject) => {
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
        this.sendUpstream(['station-client-request', id, message])
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

  subscribe(): void {}

  unsubscribe(): void {}

  //HELPER
  private sendUpstream(message: StationClientRemoteMessageShape<T>) {
    if (!this.isLeader || !this.webSocketUrl) return

    if (!this.webSocket || this.webSocket.readyState !== WebSocket.OPEN) {
      if (self.navigator.onLine) {
        // Limit outbound queue to 64 entries
        if (this.upstreamQueue.length >= 64) this.upstreamQueue.shift()
        this.upstreamQueue.push(message)
      }
      return
    }

    try {
      this.webSocket.send(encode(message))
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
          'origin-socket-leader',
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
      for (const id of this.pendingfetchs.keys()) {
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
  addEventListener<K extends keyof StationClientEventMap<T>>(
    type: K,
    listener: StationClientEventListenerFor<T, K> | null,
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
  removeEventListener<K extends keyof StationClientEventMap<T>>(
    type: K,
    listener: StationClientEventListenerFor<T, K> | null,
    options?: boolean | EventListenerOptions
  ): void {
    this.eventTarget.removeEventListener(
      type,
      listener as EventListenerOrEventListenerObject | null,
      options
    )
  }
}
