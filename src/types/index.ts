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

export type ChannelBrokerEventMap<RPCRequest, RPCResponse> = {
  violation: string
  request: [RPCRequest, (response: RPCResponse) => void]
}

export type ChannelBrokerEventListener<
  RPCRequest,
  RPCResponse,
  K extends keyof ChannelBrokerEventMap<RPCRequest, RPCResponse>,
> =
  | ((
      event: CustomEvent<ChannelBrokerEventMap<RPCRequest, RPCResponse>[K]>
    ) => void)
  | {
      handleEvent(
        event: CustomEvent<ChannelBrokerEventMap<RPCRequest, RPCResponse>[K]>
      ): void
    }

export type ChannelBrokerEventListenerFor<
  RPCRequest,
  RPCResponse,
  K extends string,
> = K extends keyof ChannelBrokerEventMap<RPCRequest, RPCResponse>
  ? ChannelBrokerEventListener<RPCRequest, RPCResponse, K>
  : EventListenerOrEventListenerObject

export type ActorChannelPair = {
  send: (data: ArrayBuffer) => void
  readyState: number
}
