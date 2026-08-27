import type { Context, SocketDetails } from '../types/index.js'
import { decode, encode } from '@msgpack/msgpack'

export class SocketManager<
  Topic extends string,
  Gossip,
  Offer,
  Answer,
  RPCRequest,
  RPCResponse,
> {
  private readonly eventTarget: EventTarget = new EventTarget()
  private readonly subscriptionMap: Map<Topic, Set<WebSocket>> = new Map()
  private readonly rpcEnabled: Set<WebSocket> = new Set()
  private readonly relayOnly: Set<WebSocket> = new Set()

  addSocket(
    socket: WebSocket,
    rpcEnabled: boolean = false,
    subscriptions?: Topic[]
  ): void {
    if (rpcEnabled) void this.rpcEnabled.add(socket)
    else void this.relayOnly.add(socket)

    if (!subscriptions) return
    for (const topic of subscriptions) {
      let topicSubscribers = this.subscriptionMap.get(topic)
      if (!topicSubscribers)
        void this.subscriptionMap.set(topic, new Set([socket]))
      else void topicSubscribers.add(socket)
    }
    return
  }

  handleMessage(sender: WebSocket, message: ArrayBuffer) {
    if (!(message instanceof ArrayBuffer))
      return void this.eventTarget.dispatchEvent(
        new CustomEvent<string>('violation', {
          detail: 'Wrong message encoding.',
        })
      )
    let ctx: Context<unknown>

    try {
      ctx = decode(message) as Context<unknown>
    } catch {
      return void this.eventTarget.dispatchEvent(
        new CustomEvent<string>('violation', {
          detail: 'Wrong message encoding.',
        })
      )
    }

    //////////////
    // REQUEST //
    ////////////
    if (ctx.kind === 'request') {
      if (ctx.phase !== 'request')
        return void this.eventTarget.dispatchEvent(
          new CustomEvent<string>('violation', { detail: 'Off protocol.' })
        )
      if (!this.rpcEnabled.has(sender))
        return void this.eventTarget.dispatchEvent(
          new CustomEvent<string>('violation', { detail: 'Unauthorized.' })
        )
      return void this.eventTarget.dispatchEvent(
        new CustomEvent<
          [request: RPCRequest, resolve: (reponse: RPCResponse) => void]
        >('request', {
          detail: [
            ctx.detail as RPCRequest,
            (response: RPCResponse) => {
              void sender.send(
                encode<Context<RPCResponse>>({
                  kind: 'resolve',
                  id: ctx.id,
                  detail: response,
                }).buffer
              )
            },
          ],
        })
      )
    }

    /////////////
    // INVOKE //
    ///////////

    if (ctx.kind === 'request') {
      if (ctx.phase !== 'request')
        return void this.eventTarget.dispatchEvent(
          new CustomEvent<string>('violation', { detail: 'Off protocol.' })
        )
      if (!this.rpcEnabled.has(sender))
        return void this.eventTarget.dispatchEvent(
          new CustomEvent<string>('violation', { detail: 'Unauthorized.' })
        )
      return void this.eventTarget.dispatchEvent(
        new CustomEvent<
          [request: RPCRequest, resolve: (reponse: RPCResponse) => void]
        >('request', {
          detail: [
            ctx.detail as RPCRequest,
            (response: RPCResponse) => {
              void sender.send(
                encode<Context<RPCResponse>>({
                  kind: 'resolve',
                  id: ctx.id,
                  detail: response,
                }).buffer
              )
            },
          ],
        })
      )
    }
  }
}
