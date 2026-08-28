import { encode, decode } from '@msgpack/msgpack'
import type {
  Context,
  ActorChannelEventMap,
  ActorChannelEventListenerFor,
} from '../types/index.js'

export class ActorChannel<
  Topic extends string,
  Message,
  RPCRequest,
  RPCResponse,
> {
  private readonly eventTarget = new EventTarget()
  //
  private readonly broadcastChannel: BroadcastChannel = new BroadcastChannel(
    '@sovereignbase/actor-socket:broadcast-channel'
  )
  //
  private readonly localSubscriptions: Map<Topic, number> = new Map()
  private readonly remoteSubscriptions: Map<Topic, number> = new Map()
  //
  private readonly myRequests: Set<string> = new Set()
  private readonly myTopics: Set<Topic> = new Set()
  //
  private readonly relayOnly: Set<WebSocket> = new Set()
  private readonly rpcEnabled: Set<WebSocket> = new Set()
  private readonly contextQueue: Array<
    Context<Topic | Message | RPCRequest | RPCResponse>
  > | null = null

  constructor() {
    this.broadcastChannel.onmessage = (
      event: MessageEvent<Context<Topic | Message | RPCRequest | RPCResponse>>
    ) => {
      const ctx = event.data
      if (!ctx) return

      ////////////////
      //  REQUEST  //
      //////////////
      if (ctx.kind === 'request') {
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
          this.upstreamSubscriptions!.has(ctx.topic as Topic)
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
          ctx.from === 'server'
            ? this.upstreamSubscriptions!
            : this.originTopics!
        void topics.set(topic, (topics.get(topic) ?? 0) + 1)
        if (this.isLeader && ctx.from === 'client')
          void this.sendUpstream(ctx as Context<Topic>)
        return
      }
      if (ctx.kind === 'unsubscribe') {
        const topic = ctx.topic as Topic
        const topics =
          ctx.from === 'server'
            ? this.upstreamSubscriptions!
            : this.originTopics!
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
  }

  async addBroker(channelBrokerUrl: string) {
    if (typeof this.channelManagerUrl !== 'string' || this.readyState) return
    if (!self.navigator.locks) return

    this.isConnecting = true

    try {
      while (!this.isClosed) {
        if (self.navigator.onLine !== true) return

        void (await self.navigator.locks.request(
          '@sovereignbase/actor-socket:web-lock',
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
              socket = new WebSocket(this.channelManagerUrl)
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
                  const topicSubscribers = this.upstreamSubscriptions!.get(
                    ctx.topic as Topic
                  )
                  void this.upstreamSubscriptions!.set(
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
                  const topicSubscribers =
                    this.upstreamSubscriptions!.get(topic)
                  if (!topicSubscribers) return

                  if (topicSubscribers === 1)
                    void this.upstreamSubscriptions!.delete(topic)
                  else
                    void this.upstreamSubscriptions!.set(
                      topic,
                      topicSubscribers - 1
                    )

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
    void this.upstreamSubscriptions!.clear()
    this.upstreamQueue!.length = 0
    this.originTopics = null
    this.upstreamSubscriptions = null
    this.upstreamQueue = null
  }

  //      //  //////  //////////  //    //      //      ////       ////
  ////  ////  //          //      //    //    //  //    //  //  ///
  //  //  //  //////      //      ////////  //      //  //    /   ///
  //      //  //          //      //    //    //  //    //  //      ///
  //      //  //////      //      //    //      //      ////    ////

  request(detail: RPCRequest): string | false {
    if (this.isClosed) return false

    if (this.isLeader) return void this.sendUpstream({ kind: 'invoke', detail })

    void this.broadcastChannel!.postMessage({ kind: 'invoke', detail })
    return
  }

  publish(topic: Topic, detail: Message, peerOnly: boolean = false): void {
    if (this.isClosed || !this.isOnline) return false

    const ctx: Context<Message> = {
      kind: 'publish',
      from: 'client',
      topic,
      detail,
      peerOnly,
    }

    if (this.isLeader && this.upstreamSubscriptions!.has(topic))
      void this.sendUpstream(ctx)

    void this.broadcastChannel!.postMessage(ctx)
    return true
  }

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

  //    //  //////  //      ////    //////  ////      /////
  //    //  //      //      //  //  //      //  //  ///
  ////////  //////  //      ////    //////  ////      ///
  //    //  //      //      //      //      //  //      ///
  //    //  //////  //////  //      //////  //  //  /////

  /** Sends or queues a context for the connection-owning instance. */
  private sendUpstream(
    ctx: Context<Topic | Gossip | Offer | RPCRequest | RPCResponse>
  ): boolean {
    if (!this.isLeader || !this.channelManagerUrl) return false

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
  private dispatchEvent<
    K extends keyof ActorChannelEventMap<RPCResponse, Topic, Message>,
  >(
    type: K,
    detail: ActorChannelEventMap<RPCResponse, Topic, Message>[K]
  ): void {
    return void this.eventTarget.dispatchEvent(
      new CustomEvent(type, { detail })
    )
  }

  public addEventListener<
    K extends keyof ActorChannelEventMap<RPCResponse, Topic, Message>,
  >(
    type: K,
    listener: ActorChannelEventListenerFor<
      RPCResponse,
      Topic,
      Message,
      K
    > | null,
    options?: boolean | AddEventListenerOptions
  ): void {
    return void this.eventTarget.addEventListener(
      type,
      listener as EventListenerOrEventListenerObject | null,
      options
    )
  }

  public removeEventListener<
    K extends keyof ActorChannelEventMap<RPCResponse, Topic, Message>,
  >(
    type: K,
    listener: ActorChannelEventListenerFor<
      RPCResponse,
      Topic,
      Message,
      K
    > | null,
    options?: boolean | EventListenerOptions
  ): void {
    return void this.eventTarget.removeEventListener(
      type,
      listener as EventListenerOrEventListenerObject | null,
      options
    )
  }
}
