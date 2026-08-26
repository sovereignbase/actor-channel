/**
 * A message exchanged between OriginSocket clients and an upstream server.
 *
 * The `kind` discriminator determines which routing fields are present. The
 * generic payload type is intentionally not validated by `decodeContext`.
 *
 * @typeParam T - Payload type carried by the context.
 */
export type Context<T> =
  | {
      kind: 'invoke'
      payload: T
    }
  | {
      kind: 'offer'
      id: string
      payload: T
    }
  | {
      kind: 'withdraw'
      id: string
    }
  | {
      kind: 'answer'
      id: string
      payload: T
    }
  | {
      kind: 'gossip'
      topic: string
      payload: T
      from: 'server' | 'client'
    }
  | {
      kind: 'transact'
      id: string
      payload: T
      phase: 'request' | 'response'
    }
  | {
      kind: 'subscribe'
      topic: string
      from: 'server' | 'client'
    }
  | {
      kind: 'unsubscribe'
      topic: string
      from: 'server' | 'client'
    }

/**
 * Maps OriginSocket event names to their event detail values.
 *
 * `online` and `offline` describe the shared upstream connection. `gossip` and
 * `answer` carry their respective payloads in `CustomEvent.detail`.
 *
 * @typeParam Gossip - Detail emitted by `gossip` events.
 * @typeParam Answer - Detail emitted by `answer` events.
 */
export type OriginSocketEventMap<Gossip, Answer> = {
  /** A payload received for a locally subscribed topic. */
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
 * Internal callbacks retained for a pending transaction.
 *
 * @typeParam T - Successful transaction response type.
 */
export type TransactionPromise<T> = {
  /** Resolves the transaction with its response. */
  resolve: (value: T) => void
  /** Rejects the transaction. */
  reject: (reason?: unknown) => void
  /** Removes resources such as abort listeners. */
  cleanup: () => void
}
