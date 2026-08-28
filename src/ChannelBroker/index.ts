import type {
  Context,
  ActorChannelPair,
  ChannelBrokerEventMap,
} from '../types/index.js'
import { decode, encode } from '@msgpack/msgpack'

export class ChannelBroker<
  Topic extends string,
  Message,
  RPCRequest,
  RPCResponse,
> {
  private readonly eventTarget: EventTarget = new EventTarget()
  ///
  private readonly syncTopics: Map<string, Set<ActorChannelPair>> = new Map()
  private readonly peerTopics: Map<string, Set<ActorChannelPair>> = new Map()
  ///
  private readonly rpcEnabled: Set<ActorChannelPair> = new Set()
  ///

  addChannel(
    channel: ActorChannelPair,
    rpcEnabled: boolean = false,
    subscriptions?: {
      syncTopics?: Array<string>
      peerTopics?: Array<string>
    }
  ): void {
    if (rpcEnabled) void this.rpcEnabled.add(channel)

    if (!subscriptions) return
    if (subscriptions.syncTopics) {
      for (const topic of subscriptions.syncTopics) {
        void this.subscribeSyncTopic(channel, topic)
      }
    }
    if (subscriptions.peerTopics) {
      for (const topic of subscriptions.peerTopics) {
        void this.subscribePeerTopic(channel, topic)
      }
    }
    return
  }

  handleMessage(sender: ActorChannelPair, message: ArrayBuffer) {
    let ctx: Context<Topic, Message, RPCRequest, RPCResponse>

    try {
      if (!(message instanceof ArrayBuffer)) throw null
      ctx = decode(message) as Context<Topic, Message, RPCRequest, RPCResponse>
    } catch {
      return void this.dispatchEvent('violation', 'Wrong message encoding.')
    }

    ////////////////
    //  REQUEST  //
    //////////////
    if (ctx?.kind === 'request') {
      if (!this.rpcEnabled.has(sender))
        return void this.dispatchEvent('violation', 'Unauthorized.')

      return void this.dispatchEvent('request', [
        ctx.detail as RPCRequest,
        /////////////////
        //  RESPONSE  //
        ///////////////
        (detail?: RPCResponse) => {
          if (!sender || sender?.readyState !== WebSocket.OPEN) return
          void sender.send(
            encode<Context<Topic, Message, RPCRequest, RPCResponse>>({
              kind: 'response',
              id: ctx.id,
              detail,
            }).buffer
          )
        },
      ])
    }

    ////////////////
    //  PUBLISH  //
    //////////////

    if (ctx?.kind === 'publish') {
      if (ctx.peerOnly) {
        const topicSubscribers = this.peerTopics.get(ctx.topic)
        if (!topicSubscribers) return
        const buffer = encode<Context<Topic, Message, RPCRequest, RPCResponse>>(
          {
            kind: 'publish',
            topic: ctx.topic,
            from: 'server',
            detail: ctx.detail,
            peerOnly: true,
          }
        ).buffer
        for (const channel of topicSubscribers.values()) {
          void channel.send(buffer)
        }
        return
      }

      const topicSubscribers = this.syncTopics.get(ctx.topic)
      if (!topicSubscribers) return
      const buffer = encode<Context<Topic, Message, RPCRequest, RPCResponse>>({
        kind: 'publish',
        topic: ctx.topic,
        from: 'server',
        detail: ctx.detail,
      }).buffer
      for (const channel of topicSubscribers.values()) {
        void channel.send(buffer)
      }
      return
    }

    //////////////////
    //  SUBSCRIBE  //
    ////////////////

    if (ctx?.kind === 'subscribe') {
      if (ctx.peerOnly) {
        void this.subscribePeerTopic(sender, ctx.topic)
        const topicSubscribers = this.peerTopics.get(ctx.topic)!
        const buffer = encode<Context<Topic, Message, RPCRequest, RPCResponse>>(
          {
            kind: 'subscribe',
            topic: ctx.topic,
            from: 'server',
            peerOnly: true,
          }
        ).buffer
        for (const channel of topicSubscribers.values()) {
          void channel.send(buffer)
        }
        return
      }
      void this.subscribeSyncTopic(sender, ctx.topic)
      const topicSubscribers = this.syncTopics.get(ctx.topic)!
      const buffer = encode<Context<Topic, Message, RPCRequest, RPCResponse>>({
        kind: 'subscribe',
        topic: ctx.topic,
        from: 'server',
      }).buffer
      for (const channel of topicSubscribers.values()) {
        void channel.send(buffer)
      }
      return
    }

    ////////////////////
    //  UNSUBSCRIBE  //
    //////////////////

    if (ctx?.kind === 'unsubscribe') {
      if (ctx.peerOnly) {
        const topicSubscribers = this.peerTopics.get(ctx.topic)
        if (!topicSubscribers) return
        const buffer = encode<Context<Topic, Message, RPCRequest, RPCResponse>>(
          {
            kind: 'unsubscribe',
            topic: ctx.topic,
            from: 'server',
            peerOnly: true,
          }
        ).buffer
        for (const channel of topicSubscribers.values()) {
          void channel.send(buffer)
        }
        return void this.unsubscribePeerTopic(sender, ctx.topic)
      }
      const topicSubscribers = this.syncTopics.get(ctx.topic)
      if (!topicSubscribers) return
      const buffer = encode<Context<Topic, Message, RPCRequest, RPCResponse>>({
        kind: 'unsubscribe',
        topic: ctx.topic,
        from: 'server',
      }).buffer
      for (const channel of topicSubscribers.values()) {
        void channel.send(buffer)
      }
      return void this.unsubscribeSyncTopic(sender, ctx.topic)
    }

    return
  }

  //    //  //////  //      ////    //////  ////      /////
  //    //  //      //      //  //  //      //  //  ///
  ////////  //////  //      ////    //////  ////      ///
  //    //  //      //      //      //      //  //      ///
  //    //  //////  //////  //      //////  //  //  /////

  private subscribeSyncTopic(
    subscriber: ActorChannelPair,
    topic: string
  ): void {
    const topicSubscribers = this.syncTopics.get(topic)
    if (!topicSubscribers)
      return void this.syncTopics.set(topic, new Set([subscriber]))
    else return void topicSubscribers.add(subscriber)
  }
  private unsubscribeSyncTopic(
    subscriber: ActorChannelPair,
    topic: string
  ): void {
    const topicSubscribers = this.syncTopics.get(topic)
    if (!topicSubscribers) return
    else return void topicSubscribers.delete(subscriber)
  }

  private subscribePeerTopic(
    subscriber: ActorChannelPair,
    topic: string
  ): void {
    const topicSubscribers = this.peerTopics.get(topic)
    if (!topicSubscribers)
      return void this.peerTopics.set(topic, new Set([subscriber]))
    else return void topicSubscribers.add(subscriber)
  }
  private unsubscribePeerTopic(
    subscriber: ActorChannelPair,
    topic: string
  ): void {
    const topicSubscribers = this.peerTopics.get(topic)
    if (!topicSubscribers) return
    else return void topicSubscribers.delete(subscriber)
  }
  private dispatchEvent<
    K extends keyof ChannelBrokerEventMap<RPCRequest, RPCResponse>,
  >(type: K, detail: ChannelBrokerEventMap<RPCRequest, RPCResponse>[K]): void {
    return void this.eventTarget.dispatchEvent(
      new CustomEvent(type, { detail })
    )
  }

  public addEventListener<
    K extends keyof ChannelBrokerEventMap<RPCRequest, RPCResponse>,
  >(
    type: K,
    listener: ChannelBrokerEventMap<RPCRequest, RPCResponse> | null,
    options?: boolean | AddEventListenerOptions
  ): void {
    return void this.eventTarget.addEventListener(
      type,
      listener as EventListenerOrEventListenerObject | null,
      options
    )
  }

  public removeEventListener<
    K extends keyof ChannelBrokerEventMap<RPCRequest, RPCResponse>,
  >(
    type: K,
    listener: ChannelBrokerEventMap<RPCRequest, RPCResponse> | null,
    options?: boolean | EventListenerOptions
  ): void {
    return void this.eventTarget.removeEventListener(
      type,
      listener as EventListenerOrEventListenerObject | null,
      options
    )
  }
}
