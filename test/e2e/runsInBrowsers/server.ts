import { readFile } from 'node:fs/promises'
import { createServer } from 'node:http'
import { resolve } from 'node:path'
import { WebSocketServer } from 'ws'
import { ChannelBroker } from '../../../dist/index.js'

const broker = new ChannelBroker<string, string, string, string>()
broker.addEventListener('request', (event) =>
  event.detail[1](`reply:${event.detail[0]}`)
)
const server = createServer(async (request, response) => {
  if (request.url === '/stats') {
    response.setHeader('content-type', 'application/json')
    response.end(
      JSON.stringify(
        [...broker.channelAttachments].map(([socket, attachment]) => ({
          url: urls.get(socket),
          topics: [...(attachment.topics ?? [])],
        }))
      )
    )
    return
  }
  const path =
    request.url === '/index.js'
      ? resolve('dist/index.js')
      : request.url?.startsWith('/msgpack/') &&
          /^[-\w./]+$/.test(request.url) &&
          !request.url.includes('..')
        ? resolve(
            'node_modules/@msgpack/msgpack/dist.esm',
            request.url.slice('/msgpack/'.length)
          )
        : undefined
  if (path) {
    try {
      response.setHeader('content-type', 'text/javascript')
      response.end(await readFile(path))
    } catch {
      response.writeHead(404).end()
    }
    return
  }
  response.setHeader('content-type', 'text/html')
  response.end(
    '<!doctype html><script type="importmap">{"imports":{"@msgpack/msgpack":"/msgpack/index.mjs"}}</script><title>ActorChannel tests</title>'
  )
})
const urls = new WeakMap<object, string>()
const sockets = new WebSocketServer({ server })
sockets.on('connection', (socket, request) => {
  urls.set(socket, request.url ?? '')
  broker.addChannel(socket, { rpcEnabled: true })
  socket.on('message', (data) => {
    if (Array.isArray(data)) throw new Error('Unexpected fragmented message')
    const bytes = data instanceof ArrayBuffer ? new Uint8Array(data) : data
    broker.handleMessage(socket, Uint8Array.from(bytes).buffer)
  })
  socket.on('close', () => broker.deleteChannel(socket))
})
server.listen(4173, '127.0.0.1')
