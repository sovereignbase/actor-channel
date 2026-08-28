import type {
  Context,
  ActorChannelPair,
  ChannelBrokerEventMap,
  ChannelBrokerEventListenerFor,
} from '../types/index.js'
import { decode, encode } from '@msgpack/msgpack'

/**
 * Routes RPC requests and topic messages between connected actor channels.
 *
 * @typeParam Topic - The topic identifier type.
 * @typeParam Message - The published message type.
 * @typeParam RPCRequest - The RPC request payload type.
 * @typeParam RPCResponse - The RPC response payload type.
 */
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

  /**
   * Adds a channel to the broker.
   *
   * @param channel - The channel transport to add.
   * @param rpcEnabled - Whether the channel may issue RPC requests.
   * @param topics - The topics initially subscribed by the channel.
   */
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

  /**
   * Removes a channel and all of its subscriptions from the broker.
   *
   * @param channel - The channel transport to remove.
   */
  deleteChannel(channel: ActorChannelPair): void {
    void this.rpcEnabled.delete(channel)

    for (const [topic, subscribers] of this.topicSubscribers) {
      if (subscribers.has(channel))
        void this.handleSubscription(channel, 'unsubscribe', topic)
      if (subscribers.size < 1) void this.topicSubscribers.delete(topic)
    }
    return
  }

  /**
   * Handles an encoded protocol message received from a channel.
   *
   * @param sender - The channel that sent the message.
   * @param message - The MessagePack-encoded protocol message.
   */
  handleMessage(sender: ActorChannelPair, message: ArrayBuffer): void {
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
            }).slice().buffer
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
      }).slice().buffer
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
      }).slice().buffer
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
      if (!subscribers?.has(sender))
        return void this.dispatchEvent('violation', 'Unauthorized.')
      const buffer = encode<Context<Topic, Message, RPCRequest, RPCResponse>>({
        kind: 'unsubscribe',
        topic: ctx.topic,
        from: 'server',
        amount: subscribers.size - 1,
      }).slice().buffer
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
      encode({ kind, topic, from: 'client' }).slice().buffer
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

  /**
   * Registers an event listener.
   *
   * @param type - The event type.
   * @param listener - The listener to register.
   * @param options - The event listener options.
   */
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

  /**
   * Removes a previously registered event listener.
   *
   * @param type - The event type.
   * @param listener - The listener to remove.
   * @param options - The event listener options.
   */
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
