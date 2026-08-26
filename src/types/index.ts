export type Context<T> =
  | {
      kind: 'invoke'
      payload: T
    }
  | {
      kind: 'signal'
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
 * Maps OriginSocket event names to their event payload shapes.
 */
export type OriginSocketEventMap<Gossip, Signal> = {
  gossip: Gossip
  signal: Signal
}

/**
 * Represents a strongly typed OriginSocket event listener.
 */
export type OriginSocketEventListener<
  Gossip,
  Signal,
  K extends keyof OriginSocketEventMap<Gossip, Signal>,
> =
  | ((event: CustomEvent<OriginSocketEventMap<Gossip, Signal>[K]>) => void)
  | {
      handleEvent(
        event: CustomEvent<OriginSocketEventMap<Gossip, Signal>[K]>
      ): void
    }

/**
 * Resolves an event name to its corresponding listener type.
 */
export type OriginSocketEventListenerFor<
  Gossip,
  Signal,
  K extends string,
> = K extends keyof OriginSocketEventMap<Gossip, Signal>
  ? OriginSocketEventListener<Gossip, Signal, K>
  : EventListenerOrEventListenerObject

export type TransactionPromise<T> = {
  resolve: (value: T) => void
  reject: () => void
  cleanup: () => void
}
