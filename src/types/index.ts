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
//  ACTOR CHANNEL //
//===============//

export type ActorChannelEventMap<RPCResponse, Topic, Message> = {
  response: [id: string, response?: RPCResponse]
  message: [topic: Topic, Message]
}

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

export type ChannelManagerEventMap<RPCRequest, RPCResponse> = {
  violation: string
  request: [RPCRequest, (response: RPCResponse) => void]
}

export type ChannelManagerEventListener<
  RPCRequest,
  RPCResponse,
  K extends keyof ChannelManagerEventMap<RPCRequest, RPCResponse>,
> =
  | ((
      event: CustomEvent<ChannelManagerEventMap<RPCRequest, RPCResponse>[K]>
    ) => void)
  | {
      handleEvent(
        event: CustomEvent<ChannelManagerEventMap<RPCRequest, RPCResponse>[K]>
      ): void
    }

export type ChannelManagerEventListenerFor<
  RPCRequest,
  RPCResponse,
  K extends string,
> = K extends keyof ChannelManagerEventMap<RPCRequest, RPCResponse>
  ? ChannelManagerEventListener<RPCRequest, RPCResponse, K>
  : EventListenerOrEventListenerObject

export type ActorChannelPair = {
  send: (data: ArrayBuffer) => void
  readyState: number
}
