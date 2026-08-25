export type Context<T> =
  | {
      kind: 'gossip'
      topic: string
      payload: T
      from: 'server' | 'client'
    }
  | {
      kind: 'invoke'
      payload: T
    }
  | {
      kind: 'transact'
      id: string
      payload: T
      phase: 'request' | 'response'
      from: 'server' | 'client'
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
export type OriginSocketEventMap<Gossip> = {
  gossip: Gossip
}

/**
 * Represents a strongly typed OriginSocket event listener.
 */
export type OriginSocketEventListener<
  T,
  K extends keyof OriginSocketEventMap<T>,
> =
  | ((event: CustomEvent<OriginSocketEventMap<T>[K]>) => void)
  | { handleEvent(event: CustomEvent<OriginSocketEventMap<T>[K]>): void }

/**
 * Resolves an event name to its corresponding listener type.
 */
export type OriginSocketEventListenerFor<
  T,
  K extends string,
> = K extends keyof OriginSocketEventMap<T>
  ? OriginSocketEventListener<T, K>
  : EventListenerOrEventListenerObject

export type TransactionPromise<T> = {
  resolve: (value: T) => void
  reject: () => void
  cleanup: () => void
}
