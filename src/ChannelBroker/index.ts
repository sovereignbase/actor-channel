import { ActorChannel } from '../ActorChannel/index.js'
import type {
  Context,
  ActorChannelPair,
  ChannelAttachment,
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
  private readonly rpcEnabled: Set<ActorChannelPair> = new Set()
  ///
  private readonly topicSubscribers: Map<string, Set<ActorChannelPair>> =
    new Map()
  ///
  /** Attachments associated with the broker's channels. */
  public readonly channelAttachments: Map<
    ActorChannelPair,
    ChannelAttachment<Topic>
  > = new Map()

  /**
   * Adds a channel to the broker.
   *
   * @param channel - The channel transport to add.
   * @param attachment - Channel metadata, RPC access, and initial topics.
   */
  addChannel(
    channel: ActorChannelPair,
    attachment: ChannelAttachment<Topic> = {}
  ): void {
    if (this.channelAttachments.has(channel))
      throw new Error('addChannel MUST be used only once per channel.')
    if (attachment.rpcEnabled) void this.rpcEnabled.add(channel)
    void this.channelAttachments.set(channel, attachment)

    if (!attachment.topics) return
    for (const topic of attachment.topics)
      void this.handleSubscription(channel, 'subscribe', topic)
    return
  }

  /**
   * Removes a channel and all of its subscriptions from the broker.
   *
   * @param channel - The channel transport to remove.
   */
  deleteChannel(channel: ActorChannelPair): void {
    const channelAttachment = this.channelAttachments.get(channel)
    void this.rpcEnabled.delete(channel)
    void this.channelAttachments.delete(channel)
    if (!channelAttachment || !channelAttachment.topics) return
    for (const topic of channelAttachment.topics)
      void this.handleSubscription(channel, 'unsubscribe', topic)

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
      return void this.dispatchEvent('violation', {
        violator: sender,
        description: 'Wrong message encoding.',
      })
    }

    ////////////////
    //  REQUEST  //
    //////////////
    if (ctx?.kind === 'request') {
      if (!this.rpcEnabled.has(sender))
        return void this.dispatchEvent('violation', {
          violator: sender,
          description: 'Unauthorized.',
        })

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
        sender,
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

      const attachment = this.channelAttachments.get(sender)
      if (attachment) {
        attachment.topics ??= new Set()
        if (!attachment.topics.has(ctx.topic)) {
          void attachment.topics.add(ctx.topic)
          void this.dispatchEvent('attachment', {
            owner: sender,
            attachment,
          })
        }
      }

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

      // Only allow subscribers to unsubscribe otherwise subscription counters could be manipulated.
      if (!subscribers?.has(sender))
        return void this.dispatchEvent('violation', {
          violator: sender,
          description: 'Unauthorized.',
        })

      const buffer = encode<Context<Topic, Message, RPCRequest, RPCResponse>>({
        kind: 'unsubscribe',
        topic: ctx.topic,
        from: 'server',
        amount: subscribers.size - 1,
      }).slice().buffer
      for (const channel of subscribers.values()) {
        void channel.send(buffer)
      }

      void this.unsubscribeTopic(sender, ctx.topic)
      const attachment = this.channelAttachments.get(sender)
      if (attachment?.topics?.delete(ctx.topic))
        void this.dispatchEvent('attachment', {
          owner: sender,
          attachment,
        })
      return
    }

    return void this.dispatchEvent('violation', {
      violator: sender,
      description: 'Off protocol.',
    })
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
    else void subscribers.delete(subscriber)
    if (subscribers.size < 1) void this.topicSubscribers.delete(topic)
    return
  }

  /**
   * Dispatches a broker event to registered listeners.
   *
   * Request handlers can use this to report application-level protocol
   * violations associated with the request sender.
   *
   * @param type - The event type.
   * @param detail - The event payload.
   */
  public dispatchEvent<
    K extends keyof ChannelBrokerEventMap<RPCRequest, RPCResponse, Topic>,
  >(
    type: K,
    detail: ChannelBrokerEventMap<RPCRequest, RPCResponse, Topic>[K]
  ): void {
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
    K extends keyof ChannelBrokerEventMap<RPCRequest, RPCResponse, Topic>,
  >(
    type: K,
    listener: ChannelBrokerEventListenerFor<
      RPCRequest,
      RPCResponse,
      K,
      Topic
    > | null,
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
    K extends keyof ChannelBrokerEventMap<RPCRequest, RPCResponse, Topic>,
  >(
    type: K,
    listener: ChannelBrokerEventListenerFor<
      RPCRequest,
      RPCResponse,
      K,
      Topic
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
