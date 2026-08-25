export type RPC<T> =
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
    }
  | {
      kind: 'unsubscribe'
      topic: string
    }
