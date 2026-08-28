import type {
  Context,
  ActorChannelPair,
  ChannelBrokerEventMap,
  ChannelBrokerEventListenerFor,
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
  private readonly topicSubscribers: Map<string, Set<ActorChannelPair>> =
    new Map()
  ///
  private readonly rpcEnabled: Set<ActorChannelPair> = new Set()
  ///

  addChannel(
    channel: ActorChannelPair,
    rpcEnabled: boolean = false,
    topics?: Array<string>
  ): void {
    if (rpcEnabled) void this.rpcEnabled.add(channel)

    if (!topics) return
    for (const topic of topics)
      void this.handleSubscription(channel, 'subscribe', topic)
    return
  }

  deleteChannel(channel: ActorChannelPair): void {
    void this.rpcEnabled.delete(channel)

    for (const [topic, subscribers] of this.topicSubscribers) {
      if (subscribers.has(channel))
        void this.handleSubscription(channel, 'unsubscribe', topic)
      if (subscribers.size < 1) void this.topicSubscribers.delete(topic)
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
      const subscribers = this.topicSubscribers.get(ctx.topic)
      if (!subscribers) return
      const buffer = encode<Context<Topic, Message, RPCRequest, RPCResponse>>({
        kind: 'publish',
        topic: ctx.topic,
        from: 'server',
        detail: ctx.detail,
      }).buffer
      for (const channel of subscribers.values()) {
        void channel.send(buffer)
      }
      return
    }

    //////////////////
    //  SUBSCRIBE  //
    ////////////////

    if (ctx?.kind === 'subscribe') {
      void this.subscribeTopic(sender, ctx.topic)
      const subscribers = this.topicSubscribers.get(ctx.topic)!
      const buffer = encode<Context<Topic, Message, RPCRequest, RPCResponse>>({
        kind: 'subscribe',
        topic: ctx.topic,
        from: 'server',
        amount: subscribers.size,
      }).buffer
      for (const channel of subscribers.values()) {
        void channel.send(buffer)
      }
      return
    }

    ////////////////////
    //  UNSUBSCRIBE  //
    //////////////////

    if (ctx?.kind === 'unsubscribe') {
      const subscribers = this.topicSubscribers.get(ctx.topic)
      if (!subscribers) return
      const buffer = encode<Context<Topic, Message, RPCRequest, RPCResponse>>({
        kind: 'unsubscribe',
        topic: ctx.topic,
        from: 'server',
        amount: subscribers.size - 1,
      }).buffer
      for (const channel of subscribers.values()) {
        void channel.send(buffer)
      }
      return void this.unsubscribeTopic(sender, ctx.topic)
    }

    return
  }

  //    //  //////  //      ////    //////  ////      /////
  //    //  //      //      //  //  //      //  //  ///
  ////////  //////  //      ////    //////  ////      ///
  //    //  //      //      //      //      //  //      ///
  //    //  //////  //////  //      //////  //  //  /////

  private handleSubscription(
    sender: ActorChannelPair,
    kind: 'subscribe' | 'unsubscribe',
    topic: string
  ): void {
    return void this.handleMessage(
      sender,
      encode({ kind, topic, from: 'client' }).buffer
    )
  }

  private subscribeTopic(subscriber: ActorChannelPair, topic: string): void {
    const subscribers = this.topicSubscribers.get(topic)
    if (!subscribers)
      return void this.topicSubscribers.set(topic, new Set([subscriber]))
    else return void subscribers.add(subscriber)
  }
  private unsubscribeTopic(subscriber: ActorChannelPair, topic: string): void {
    const subscribers = this.topicSubscribers.get(topic)
    if (!subscribers) return
    else return void subscribers.delete(subscriber)
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
    listener: ChannelBrokerEventListenerFor<RPCRequest, RPCResponse, K> | null,
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
    listener: ChannelBrokerEventListenerFor<RPCRequest, RPCResponse, K> | null,
    options?: boolean | EventListenerOptions
  ): void {
    return void this.eventTarget.removeEventListener(
      type,
      listener as EventListenerOrEventListenerObject | null,
      options
    )
  }
}
