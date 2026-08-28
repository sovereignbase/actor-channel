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

//=================//
//  ACTOR SOCKET  //
//===============//

export type ActorSocketEventMap<RPCResponse, Topic, Message> = {
  response: [id: string, response?: RPCResponse]
  message: [topic: Topic, Message]
}

export type ActorSocketEventListener<
  RPCResponse,
  Topic,
  Message,
  K extends keyof ActorSocketEventMap<RPCResponse, Topic, Message>,
> =
  | ((
      event: CustomEvent<ActorSocketEventMap<RPCResponse, Topic, Message>[K]>
    ) => void)
  | {
      handleEvent(
        event: CustomEvent<ActorSocketEventMap<RPCResponse, Topic, Message>[K]>
      ): void
    }

export type ActorSocketEventListenerFor<
  RPCResponse,
  Topic,
  Message,
  K extends string,
> = K extends keyof ActorSocketEventMap<RPCResponse, Topic, Message>
  ? ActorSocketEventListener<RPCResponse, Topic, Message, K>
  : EventListenerOrEventListenerObject

//===================//
//  SOCKET MANAGER  //
//=================//

export type SocketManagerEventMap<RPCRequest, RPCResponse> = {
  violation: string
  request: [RPCRequest, (response: RPCResponse) => void]
}

export type SocketManagerEventListener<
  RPCRequest,
  RPCResponse,
  K extends keyof SocketManagerEventMap<RPCRequest, RPCResponse>,
> =
  | ((
      event: CustomEvent<SocketManagerEventMap<RPCRequest, RPCResponse>[K]>
    ) => void)
  | {
      handleEvent(
        event: CustomEvent<SocketManagerEventMap<RPCRequest, RPCResponse>[K]>
      ): void
    }

export type SocketManagerEventListenerFor<
  RPCRequest,
  RPCResponse,
  K extends string,
> = K extends keyof SocketManagerEventMap<RPCRequest, RPCResponse>
  ? SocketManagerEventListener<RPCRequest, RPCResponse, K>
  : EventListenerOrEventListenerObject

export type SocketDummy = {
  send: (data: ArrayBuffer) => void
  readyState: number
}
