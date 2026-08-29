/**
 * A message exchanged between an {@link ActorChannel} and a
 * {@link ChannelBroker}, or between browsing contexts belonging to the same
 * actor-channel swarm.
 *
 * @typeParam Topic - The topic identifier type.
 * @typeParam Message - The published message type.
 * @typeParam RPCRequest - The RPC request payload type.
 * @typeParam RPCResponse - The RPC response payload type.
 */
export type Context<Topic, Message, RPCRequest, RPCResponse> =
  //////////////
  // REQ/RES //
  ////////////
  | {
      kind: 'request'
      id: string
      detail: RPCRequest
    }
  | {
      kind: 'response'
      id: string
      detail?: RPCResponse
    }
  //////////////
  // PUB/SUB //
  ////////////
  | {
      kind: 'publish'
      topic: Topic
      from: 'server' | 'client'
      detail: Message
    }
  | {
      kind: 'subscribe'
      topic: Topic
      from: 'server' | 'client'
      amount?: number
    }
  | {
      kind: 'unsubscribe'
      topic: Topic
      from: 'server' | 'client'
      amount?: number
    }
  ///////////////
  // INTERNAL //
  /////////////
  | {
      kind: 'internal'
      detail:
        | { var: 'rpcBrokers'; prev: number; count: number }
        | { var: 'rpcBrokers'; ping: true }
    }

//=================//
//  ACTOR CHANNEL //
//===============//

/**
 * Maps each event dispatched by {@link ActorChannel} to its tuple payload.
 *
 * @typeParam RPCResponse - The RPC response payload type.
 * @typeParam Topic - The topic identifier type.
 * @typeParam Message - The published message type.
 */
export type ActorChannelEventMap<RPCResponse, Topic, Message> = {
  /** An RPC request identifier and its optional response payload. */
  response: [id: string, response?: RPCResponse]
  /** A subscribed topic and the message published to it. */
  message: [topic: Topic, Message]
}

/**
 * A function or object that handles an {@link ActorChannel} event.
 *
 * @typeParam RPCResponse - The RPC response payload type.
 * @typeParam Topic - The topic identifier type.
 * @typeParam Message - The published message type.
 * @typeParam K - The event type.
 */
export type ActorChannelEventListener<
  RPCResponse,
  Topic,
  Message,
  K extends keyof ActorChannelEventMap<RPCResponse, Topic, Message>,
> =
  | ((
      event: CustomEvent<ActorChannelEventMap<RPCResponse, Topic, Message>[K]>
    ) => void)
  | {
      handleEvent(
        event: CustomEvent<ActorChannelEventMap<RPCResponse, Topic, Message>[K]>
      ): void
    }

/**
 * Resolves a typed {@link ActorChannel} listener for known event types and a
 * standard DOM event listener for other event types.
 *
 * @typeParam RPCResponse - The RPC response payload type.
 * @typeParam Topic - The topic identifier type.
 * @typeParam Message - The published message type.
 * @typeParam K - The event type.
 */
export type ActorChannelEventListenerFor<
  RPCResponse,
  Topic,
  Message,
  K extends string,
> = K extends keyof ActorChannelEventMap<RPCResponse, Topic, Message>
  ? ActorChannelEventListener<RPCResponse, Topic, Message, K>
  : EventListenerOrEventListenerObject

//===================//
//  CHANNEL MANAGER  //
//=================//

/**
 * Maps each event dispatched by {@link ChannelBroker} to its payload.
 *
 * @typeParam Topic - The topic identifier type.
 * @typeParam RPCRequest - The RPC request payload type.
 * @typeParam RPCResponse - The RPC response payload type.
 */
export type ChannelBrokerEventMap<RPCRequest, RPCResponse, Topic = string> = {
  /** A channel whose attachment changed and the attachment's new state. */
  attachment: {
    owner: ActorChannelPair
    attachment: ChannelAttachment<Topic>
  }
  /** A protocol violation description. */
  violation: {
    violator: ActorChannelPair
    description: 'Wrong message encoding.' | 'Off protocol.' | 'Unauthorized.'
  }
  /** An RPC request and the callback used to send its response. */
  request: [RPCRequest, (response: RPCResponse) => void]
}

/**
 * A function or object that handles a {@link ChannelBroker} event.
 *
 * @typeParam Topic - The topic identifier type.
 * @typeParam RPCRequest - The RPC request payload type.
 * @typeParam RPCResponse - The RPC response payload type.
 * @typeParam K - The event type.
 */
export type ChannelBrokerEventListener<
  RPCRequest,
  RPCResponse,
  K extends keyof ChannelBrokerEventMap<RPCRequest, RPCResponse>,
  Topic = string,
> =
  | ((
      event: CustomEvent<
        ChannelBrokerEventMap<RPCRequest, RPCResponse, Topic>[K]
      >
    ) => void)
  | {
      handleEvent(
        event: CustomEvent<
          ChannelBrokerEventMap<RPCRequest, RPCResponse, Topic>[K]
        >
      ): void
    }

/**
 * Resolves a typed {@link ChannelBroker} listener for known event types and a
 * standard DOM event listener for other event types.
 *
 * @typeParam Topic - The topic identifier type.
 * @typeParam RPCRequest - The RPC request payload type.
 * @typeParam RPCResponse - The RPC response payload type.
 * @typeParam K - The event type.
 */
export type ChannelBrokerEventListenerFor<
  RPCRequest,
  RPCResponse,
  K extends string,
  Topic = string,
> = K extends keyof ChannelBrokerEventMap<RPCRequest, RPCResponse, Topic>
  ? ChannelBrokerEventListener<RPCRequest, RPCResponse, K, Topic>
  : EventListenerOrEventListenerObject

/**
 * The transport interface used by {@link ChannelBroker} to communicate with
 * an actor channel.
 */
export type ActorChannelPair = {
  /** Sends an encoded protocol message to the channel. */
  send: (data: ArrayBuffer) => void
  /** The current transport ready state. */
  readyState: number
}

/** Metadata and broker state associated with a channel. */
export type ChannelAttachment<Topic> = {
  /** The channel's IP address. */
  ipAddress?: string
  /** Whether the channel may issue RPC requests. */
  rpcEnabled?: boolean
  /** Topics currently subscribed by the channel. */
  topics?: Set<Topic>
}
