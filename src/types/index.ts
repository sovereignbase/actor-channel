/**
 * A message exchanged between OriginSocket clients and an upstream server.
 *
 * The `kind` discriminator determines which routing fields are present. The
 * generic detail type is intentionally not validated by `decodeContext`.
 *
 * @typeParam T - Detail type carried by the context.
 */
export type Context<T> =
  //////////////
  // REQ/RES //
  ////////////
  | {
      kind: 'request'
      id: string
      detail: T
    }
  | {
      kind: 'response'
      id: string
      detail?: T
    }
  //////////////
  // PUB/SUB //
  ////////////
  | {
      kind: 'publish'
      topic: string
      from: 'server' | 'client'
      detail: T
      peerOnly?: boolean
    }
  | {
      kind: 'subscribe'
      topic: string
      from: 'server' | 'client'
      peerOnly?: boolean
    }
  | {
      kind: 'unsubscribe'
      topic: string
      from: 'server' | 'client'
      peerOnly?: boolean
    }
/**
 * Maps OriginSocket event names to their event detail values.
 *
 * `connected` and `diconnected` describe the shared upstream connection. `gossip` and
 * `answer` carry their respective details in `CustomEvent.detail`.
 *
 * @typeParam Gossip - Detail emitted by `gossip` events.
 * @typeParam Answer - Detail emitted by `answer` events.
 */
export type OriginSocketEventMap<Gossip, Answer> = {
  /** A detail received for a locally subscribed topic. */
  gossip: Gossip
  /** A response to an offer created by this instance. */
  answer: Answer
  /** The shared upstream connection became available. */
  online: null
  /** The shared upstream connection was lost. */
  offline: null
}

/**
 * Represents a strongly typed OriginSocket event listener.
 *
 * @typeParam Gossip - Gossip event detail type.
 * @typeParam Answer - Answer event detail type.
 * @typeParam K - Event name.
 */
export type OriginSocketEventListener<
  Gossip,
  Answer,
  K extends keyof OriginSocketEventMap<Gossip, Answer>,
> =
  | ((event: CustomEvent<OriginSocketEventMap<Gossip, Answer>[K]>) => void)
  | {
      handleEvent(
        event: CustomEvent<OriginSocketEventMap<Gossip, Answer>[K]>
      ): void
    }

/**
 * Resolves an event name to its corresponding listener type.
 *
 * @typeParam Gossip - Gossip event detail type.
 * @typeParam Answer - Answer event detail type.
 * @typeParam K - Event name.
 */
export type OriginSocketEventListenerFor<
  Gossip,
  Answer,
  K extends string,
> = K extends keyof OriginSocketEventMap<Gossip, Answer>
  ? OriginSocketEventListener<Gossip, Answer, K>
  : EventListenerOrEventListenerObject

/**
 * Internal callbacks retained for a pending request.
 *
 * @typeParam T - Successful request response type.
 */
export type RequestPromise<T> = {
  /** Resolves the request with its response. */
  resolve: (value: T) => void
  /** Rejects the request. */
  reject: (reason?: unknown) => void
  /** Removes resources such as abort listeners. */
  cleanup: () => void
}

export type ChannelMessage<T> =
  | Context<T>
  | { kind: 'connected'; id: string }
  | { kind: 'disconnected'; id: string | null }
  | { kind: 'status' }

/**
 * Maps SocketManager event names to their event detail values.
 *
 * `connected` and `disconnected` describe the shared upstream connection. `gossip` and
 * `answer` carry their respective details in `CustomEvent.detail`.
 *
 * @typeParam Gossip - Detail emitted by `gossip` events.
 * @typeParam Answer - Detail emitted by `answer` events.
 */
export type SocketManagerEventMap<Request, Offer> = {
  /** A detail received for a locally subscribed topic. */
  violation: string
  /** A response to an offer created by this instance. */
  request: Request
  /** The shared upstream connection became available. */
  offer: Offer
}

/**
 * Represents a strongly typed SocketManager event listener.
 *
 * @typeParam Gossip - Gossip event detail type.
 * @typeParam Answer - Answer event detail type.
 * @typeParam K - Event name.
 */
export type SocketManagerEventListener<
  Gossip,
  Answer,
  K extends keyof SocketManagerEventMap<Gossip, Answer>,
> =
  | ((event: CustomEvent<SocketManagerEventMap<Gossip, Answer>[K]>) => void)
  | {
      handleEvent(
        event: CustomEvent<SocketManagerEventMap<Gossip, Answer>[K]>
      ): void
    }

/**
 * Resolves an event name to its corresponding listener type.
 *
 * @typeParam Gossip - Gossip event detail type.
 * @typeParam Answer - Answer event detail type.
 * @typeParam K - Event name.
 */
export type SocketManagerEventListenerFor<
  Gossip,
  Answer,
  K extends string,
> = K extends keyof SocketManagerEventMap<Gossip, Answer>
  ? SocketManagerEventListener<Gossip, Answer, K>
  : EventListenerOrEventListenerObject

export type SocketDetails<Topic extends string> = [WebSocket, Array<Topic>]
