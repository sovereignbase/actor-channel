export type RPC<T> =
  | {
      kind: 'gossip'
      topic: string
      payload: T
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
    }
  | {
      kind: 'subscribe'
      topic: string
    }
  | {
      kind: 'unsubscribe'
      topic: string
    }
