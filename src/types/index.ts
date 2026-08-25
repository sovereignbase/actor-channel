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

export type TransactOptions = {
  signal: AbortSignal
  ttlMs: number
}
