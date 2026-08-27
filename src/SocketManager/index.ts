import type { Context, SocketDummy } from '../types/index.js'
import { decode, encode } from '@msgpack/msgpack'

export class SocketManager<RPCRequest, RPCResponse> {
  private readonly eventTarget: EventTarget = new EventTarget()
  ///
  private readonly syncTopics: Map<string, Set<SocketDummy>> = new Map()
  private readonly peerTopics: Map<string, Set<SocketDummy>> = new Map()
  ///
  private readonly rpcEnabled: Set<SocketDummy> = new Set()
  ///

  addSocket(
    socket: SocketDummy,
    rpcEnabled: boolean = false,
    subscriptions?: {
      syncTopics?: Array<string>
      peerTopics?: Array<string>
    }
  ): void {
    if (rpcEnabled) void this.rpcEnabled.add(socket)

    if (!subscriptions) return
    if (subscriptions.syncTopics) {
      for (const topic of subscriptions.syncTopics) {
        void this.subscribeSyncTopic(socket, topic)
      }
    }
    if (subscriptions.peerTopics) {
      for (const topic of subscriptions.peerTopics) {
        void this.subscribePeerTopic(socket, topic)
      }
    }
    return
  }

  handleMessage(sender: SocketDummy, message: ArrayBuffer) {
    let ctx: Context<unknown>

    try {
      if (!(message instanceof ArrayBuffer)) throw null
      ctx = decode(message) as Context<unknown>
    } catch {
      return void this.eventTarget.dispatchEvent(
        new CustomEvent<string>('violation', {
          detail: 'Wrong message encoding.',
        })
      )
    }

    ////////////////
    //  REQUEST  //
    //////////////
    if (ctx?.kind === 'request') {
      if (!this.rpcEnabled.has(sender))
        return void this.eventTarget.dispatchEvent(
          new CustomEvent<string>('violation', { detail: 'Unauthorized.' })
        )
      return void this.eventTarget.dispatchEvent(
        new CustomEvent<
          [request: RPCRequest, resolve: (response?: RPCResponse) => void]
        >('request', {
          detail: [
            ctx.detail as RPCRequest,
            /////////////////
            //  RESPONSE  //
            ///////////////
            (detail?: RPCResponse) => {
              if (!sender || sender.readyState !== WebSocket.OPEN) return
              void sender.send(
                encode<Context<RPCResponse>>({
                  kind: 'response',
                  id: ctx.id,
                  detail,
                }).buffer
              )
            },
          ],
        })
      )
    }

    ////////////////
    //  PUBLISH  //
    //////////////

    if (ctx?.kind === 'publish') {
      if (ctx.peerOnly) {
        const topicSubscribers = this.peerTopics.get(ctx.topic)
        if (!topicSubscribers) return
        for (const socket of topicSubscribers.values()) {
          void socket.send(
            encode<Context<unknown>>({
              kind: 'publish',
              topic: ctx.topic,
              from: 'server',
              detail: ctx.detail,
              peerOnly: true,
            }).buffer
          )
        }
        return
      }

      const topicSubscribers = this.syncTopics.get(ctx.topic)
      if (!topicSubscribers) return
      for (const socket of topicSubscribers.values()) {
        void socket.send(
          encode<Context<unknown>>({
            kind: 'publish',
            topic: ctx.topic,
            from: 'server',
            detail: ctx.detail,
          }).buffer
        )
      }
      return
    }

    //////////////////
    //  SUBSCRIBE  //
    ////////////////

    if (ctx?.kind === 'subscribe') {
      if (ctx.peerOnly) {
        void this.subscribePeerTopic(sender, ctx.topic)
        const topicSubscribers = this.peerTopics.get(ctx.topic)!
        const buffer = encode<Context<unknown>>({
          kind: 'subscribe',
          topic: ctx.topic,
          from: 'server',
          peerOnly: true,
        }).buffer
        for (const socket of topicSubscribers.values()) {
          void socket.send(buffer)
        }
        return
      }
      void this.subscribeSyncTopic(sender, ctx.topic)
      const topicSubscribers = this.syncTopics.get(ctx.topic)!
      const buffer = encode<Context<unknown>>({
        kind: 'subscribe',
        topic: ctx.topic,
        from: 'server',
      }).buffer
      for (const socket of topicSubscribers.values()) {
        void socket.send(buffer)
      }
      return
    }

    ////////////////////
    //  UNSUBSCRIBE  //
    //////////////////

    if (ctx?.kind === 'unsubscribe') {
      if (ctx.peerOnly) {
        const topicSubscribers = this.peerTopics.get(ctx.topic)
        if (!topicSubscribers) return
        const buffer = encode<Context<unknown>>({
          kind: 'unsubscribe',
          topic: ctx.topic,
          from: 'server',
          peerOnly: true,
        }).buffer
        for (const socket of topicSubscribers.values()) {
          void socket.send(buffer)
        }
        return void this.unsubscribePeerTopic(sender, ctx.topic)
      }
      const topicSubscribers = this.syncTopics.get(ctx.topic)
      if (!topicSubscribers) return
      const buffer = encode<Context<unknown>>({
        kind: 'unsubscribe',
        topic: ctx.topic,
        from: 'server',
      }).buffer
      for (const socket of topicSubscribers.values()) {
        void socket.send(buffer)
      }
      return void this.unsubscribeSyncTopic(sender, ctx.topic)
    }

    return
  }

  //    //  //////  //      ////    //////  ////      /////
  //    //  //      //      //  //  //      //  //  ///
  ////////  //////  //      ////    //////  ////      ///
  //    //  //      //      //      //      //  //      ///
  //    //  //////  //////  //      //////  //  //  /////

  private subscribeSyncTopic(subscriber: SocketDummy, topic: string): void {
    const topicSubscribers = this.syncTopics.get(topic)
    if (!topicSubscribers)
      void this.syncTopics.set(topic, new Set([subscriber]))
    else void topicSubscribers.add(subscriber)
  }
  private unsubscribeSyncTopic(subscriber: SocketDummy, topic: string): void {
    const topicSubscribers = this.syncTopics.get(topic)
    if (!topicSubscribers) return
    else void topicSubscribers.delete(subscriber)
  }

  private subscribePeerTopic(subscriber: SocketDummy, topic: string): void {
    const topicSubscribers = this.peerTopics.get(topic)
    if (!topicSubscribers)
      void this.peerTopics.set(topic, new Set([subscriber]))
    else void topicSubscribers.add(subscriber)
  }
  private unsubscribePeerTopic(subscriber: SocketDummy, topic: string): void {
    const topicSubscribers = this.peerTopics.get(topic)
    if (!topicSubscribers) return
    else void topicSubscribers.delete(subscriber)
  }
}
