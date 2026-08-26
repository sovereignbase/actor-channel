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
 * Maps OriginSocket event names to their event payload shapes.
 */
export type OriginSocketEventMap<Gossip, Answer> = {
  gossip: Gossip
  answer: Answer
  online: null
  offline: null
}

/**
 * Represents a strongly typed OriginSocket event listener.
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
 */
export type OriginSocketEventListenerFor<
  Gossip,
  Answer,
  K extends string,
> = K extends keyof OriginSocketEventMap<Gossip, Answer>
  ? OriginSocketEventListener<Gossip, Answer, K>
  : EventListenerOrEventListenerObject

export type TransactionPromise<T> = {
  resolve: (value: T) => void
  reject: (reason?: unknown) => void
  cleanup: () => void
}
