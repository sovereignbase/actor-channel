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
  //
  private readonly eventTarget = new EventTarget()
  //
  private readonly broadcastChannel: BroadcastChannel = new BroadcastChannel(
    '@sovereignbase/actor-socket:broadcast-channel'
  )
  //
  private readonly allBrokers: Set<WebSocket> = new Set()
  private readonly rpcEnabled: Set<WebSocket> = new Set()
  private readonly rpcOnline: Set<WebSocket> = new Set()
  //
  private readonly brokerTopics: Map<Topic, Set<WebSocket>> = new Map()
  private readonly channelTopic: Map<Topic, number> = new Map()
  //
  private readonly myRequests: Set<string> = new Set()
  private readonly myTopics: Set<Topic> = new Set()
  //
  private rpcBrokers: number = 0

  public get rpcAvailable(): boolean {
    return this.rpcBrokers > 0
  }

  //      //  //////  //////////  //    //      //      ////       ////
  ////  ////  //          //      //    //    //  //    //  //  ///
  //  //  //  //////      //      ////////  //      //  //    /   ///
  //      //  //          //      //    //    //  //    //  //      ///
  //      //  //////      //      //    //      //      ////    ////

  request(detail: RPCRequest): string {
    const id = window.crypto.randomUUID()
    void this.myRequests.add(id)
    const ctx: Context<Topic, Message, RPCRequest, RPCResponse> = {
      kind: 'request',
      id,
      detail,
    }
    void void this.broadcastChannel.postMessage(ctx)
    const buffer = encode<typeof ctx>(ctx).buffer
    void this.remoteProcedureCall(buffer)
    return id
  }

  publish(topic: Topic, detail: Message, peerOnly: boolean = false): void {
    const ctx: Context<Topic, Message, RPCRequest, RPCResponse> = {
      kind: 'publish',
      from: 'client',
      topic,
      detail,
      peerOnly,
    }
    void this.broadcastChannel.postMessage(ctx)
    return void this.publishFanout(ctx)
  }

  subscribe(topic: Topic): void {
    if (this.myTopics.has(topic)) return

    void this.myTopics.add(topic)

    const ctx: Context<Topic, Message, RPCRequest, RPCResponse> = {
      kind: 'subscribe',
      topic,
      from: 'client',
    }
    void this.broadcastChannel.postMessage(ctx)
    return void this.subscribeFanout(ctx)
  }

  unsubscribe(topic: Topic): void {
    if (!this.myTopics.delete(topic)) return

    const ctx: Context<Topic, Message, RPCRequest, RPCResponse> = {
      kind: 'unsubscribe',
      topic,
      from: 'client',
    }
    void this.broadcastChannel.postMessage(ctx)
    return void this.subscribeFanout(ctx)
  }
  ///////  //////  //  //  //////  ////
  //   //    //    //  //  //      //  //
  //   //    //    //////  //////  ////
  //   //    //    //  //  //      ////
  ///////    //    //  //  //////  //  //

  constructor() {
    //////////////////////////////////
    // Best effort truth keeper xD //
    ////////////////////////////////
    const fanoutTopics = (kind: 'subscribe' | 'unsubscribe') => {
      for (const topic of this.myTopics) {
        const ctx = {
          kind,
          topic,
          from: 'client',
        } as Context<Topic, Message, RPCRequest, RPCResponse>
        void this.broadcastChannel.postMessage(ctx)
        void this.subscribeFanout(ctx)
      }
    }

    void window.addEventListener('pagehide', () => {
      void fanoutTopics('unsubscribe')
      const diff = -this.rpcOnline.size
      void this.rpcOnline.clear()
      if (diff) void this.rpcFanout(diff)
    })
    window.addEventListener('pageshow', (event) => {
      if (!event.persisted) return
      void fanoutTopics('subscribe')
      for (const socket of this.rpcEnabled) {
        if (socket.readyState !== WebSocket.OPEN || this.rpcOnline.has(socket))
          continue
        void this.rpcOnline.add(socket)
        void this.rpcFanout(1)
      }
    })

    this.broadcastChannel.onmessage = (
      event: MessageEvent<Context<Topic, Message, RPCRequest, RPCResponse>>
    ) => {
      const ctx = event.data
      if (!ctx) return

      if (ctx.kind === 'rpc') {
        this.rpcBrokers += ctx.diff
        return
      }

      ////////////////
      //  REQUEST  //
      //////////////
      if (ctx.kind === 'request') {
        void this.remoteProcedureCall(encode<typeof ctx>(ctx).buffer)
      }
      ////////////////
      //  RESPONSE //
      //////////////
      if (ctx.kind === 'response') {
        if (!this.myRequests.has(ctx.id)) return
        return void this.dispatchEvent('response', [ctx.id, ctx.detail])
      }
      ////////////////
      //  PUBLISH  //
      //////////////
      if (ctx.kind === 'publish') {
        if (this.myTopics.has(ctx.topic)) {
          void this.dispatchEvent('message', [ctx.topic, ctx.detail])
        }
        if (ctx.from === 'client') {
          return void this.publishFanout(ctx)
        }
      }
      ///////////////////////////////
      //  SUBSCRIBE / UNSUBSCRIBE //
      /////////////////////////////
      if (
        (ctx.kind === 'subscribe' || ctx.kind === 'unsubscribe') &&
        ctx.from === 'client'
      ) {
        return void this.subscribeFanout(ctx)
      }
    }
  }

  async addBroker(
    brokerUrl: string,
    rpcEnabled: boolean = false
  ): Promise<void> {
    if (typeof brokerUrl !== 'string' || !window.navigator.locks) return

    try {
      while (true) {
        await window.navigator.locks.request(
          `@sovereignbase/actor-channel:web-lock:${brokerUrl}`,
          { ifAvailable: true },
          async (lockHandle) => {
            // Some channel already has a connection to the broker.
            if (!lockHandle) return

            let socket: WebSocket

            try {
              socket = new WebSocket(brokerUrl)
            } catch {
              return
            }

            socket.binaryType = 'arraybuffer'
            void this.allBrokers.add(socket)
            if (rpcEnabled) void this.rpcEnabled.add(socket)

            void socket.addEventListener('open', () => {
              if (rpcEnabled) {
                void this.rpcOnline.add(socket)
                void this.rpcFanout(1)
              }
              for (const topic of this.channelTopic.keys()) {
                void socket.send(
                  encode<Context<Topic, Message, RPCRequest, RPCResponse>>({
                    kind: 'subscribe',
                    topic,
                    from: 'client',
                  }).buffer
                )
              }
            })

            socket.onmessage = (event: MessageEvent<ArrayBuffer>) => {
              let ctx: Context<Topic, Message, RPCRequest, RPCResponse>
              try {
                ctx = decode(event.data) as Context<
                  Topic,
                  Message,
                  RPCRequest,
                  RPCResponse
                >
              } catch {
                return
              }
              if (!ctx) return

              ////////////////
              //  RESPONSE //
              //////////////
              if (ctx.kind === 'response') {
                if (this.myRequests.has(ctx.id)) {
                  void this.dispatchEvent('response', [ctx.id, ctx.detail])
                  return void this.myRequests.delete(ctx.id)
                }
                return void this.broadcastChannel.postMessage(ctx)
              }
              ////////////////
              //  PUBLISH  //
              //////////////
              if (ctx.kind === 'publish' && ctx.from === 'server') {
                if (this.myTopics.has(ctx.topic)) {
                  void this.dispatchEvent('message', [ctx.topic, ctx.detail])
                }
                return void this.broadcastChannel.postMessage(ctx)
              }
              /////////////////
              //  SUBSCRIBE //
              ///////////////
              if (ctx.kind === 'subscribe' && ctx.from === 'server') {
                const topicSubscribers = this.brokerTopics.get(ctx.topic)
                if (!topicSubscribers)
                  return void this.brokerTopics.set(
                    ctx.topic,
                    new Set([socket])
                  )
                return void topicSubscribers.add(socket)
              }

              ///////////////////
              //  UNSUBSCRIBE //
              /////////////////
              if (ctx.kind === 'unsubscribe' && ctx.from === 'server') {
                return void this.brokerTopics.get(ctx.topic)?.delete(socket)
              }
              return
            }
            await new Promise<void>((resolve) => {
              void socket.addEventListener('close', () => resolve(), {
                once: true,
              })
            })

            if (this.rpcOnline.delete(socket)) void this.rpcFanout(-1)

            void this.allBrokers.delete(socket)
            void this.rpcEnabled.delete(socket)

            for (const sockets of this.brokerTopics.values()) {
              void sockets.delete(socket)
            }
            return
          }
        )

        await new Promise<void>((resolve) => setTimeout(resolve, 10_000))
      }
    } catch {
      return
    }
  }

  //    //  //////  //      ////    //////  ////      /////
  //    //  //      //      //  //  //      //  //  ///
  ////////  //////  //      ////    //////  ////      ///
  //    //  //      //      //      //      //  //      ///
  //    //  //////  //////  //      //////  //  //  /////

  private remoteProcedureCall(buffer: ArrayBuffer): void {
    if (this.rpcEnabled.size < 1) return
    for (const socket of this.rpcEnabled.values())
      if (socket.readyState == WebSocket.OPEN) return void socket.send(buffer)
  }

  private rpcFanout(diff: number): void {
    this.rpcBrokers += diff
    void this.broadcastChannel.postMessage({ kind: 'rpc', diff })
  }

  private publishFanout(
    ctx: Context<Topic, Message, RPCRequest, RPCResponse>
  ): void {
    if (ctx.kind !== 'publish') return
    const topicSubscribers = this.brokerTopics.get(ctx.topic)
    if (!topicSubscribers || topicSubscribers.size < 1) return
    const buffer = encode<typeof ctx>(ctx).buffer
    for (const socket of topicSubscribers.values())
      if (socket.readyState === WebSocket.OPEN) void socket.send(buffer)
    return
  }
  private subscribeFanout(
    ctx: Context<Topic, Message, RPCRequest, RPCResponse>
  ): void {
    if (ctx.kind !== 'subscribe' && ctx.kind !== 'unsubscribe') return

    const subscriberCount = this.channelTopic.get(ctx.topic) ?? 0
    if (ctx.kind === 'subscribe') {
      void this.channelTopic.set(ctx.topic, subscriberCount + 1)
      if (subscriberCount > 0) return
    } else {
      if (subscriberCount < 1) return
      if (subscriberCount > 1) {
        void this.channelTopic.set(ctx.topic, subscriberCount - 1)
        return
      }
      void this.channelTopic.delete(ctx.topic)
    }

    if (this.allBrokers.size < 1) return

    const buffer = encode<typeof ctx>(ctx).buffer
    for (const socket of this.allBrokers.values())
      if (socket.readyState === WebSocket.OPEN) void socket.send(buffer)
    return
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
