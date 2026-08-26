import { decode, encode } from '@msgpack/msgpack'
import { expect, test } from '@playwright/test'
import { readFile } from 'node:fs/promises'
import { createServer } from 'node:http'
import { resolve } from 'node:path'

let server: ReturnType<typeof createServer>
let baseUrl: string

test.beforeAll(async () => {
  server = createServer(async (request, response) => {
    if (request.url === '/index.js') {
      response.setHeader('content-type', 'text/javascript')
      response.end(await readFile(resolve('dist/index.js')))
      return
    }

    if (request.url?.startsWith('/msgpack/')) {
      response.setHeader('content-type', 'text/javascript')
      response.end(
        await readFile(
          resolve(
            'node_modules/@msgpack/msgpack/dist.esm',
            request.url.slice('/msgpack/'.length)
          )
        )
      )
      return
    }

    response.setHeader('content-type', 'text/html')
    response.end(`<!doctype html>
      <script type="importmap">
        {"imports":{"@msgpack/msgpack":"/msgpack/index.mjs"}}
      </script>
      <title>OriginSocket test</title>`)
  })

  await new Promise<void>((resolve) =>
    server.listen(0, '127.0.0.1', () => resolve())
  )
  const address = server.address()
  if (!address || typeof address === 'string') throw new Error('No test port')
  baseUrl = `http://127.0.0.1:${address.port}`
})

test.afterAll(async () => {
  await new Promise<void>((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve()))
  )
})

test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => {
    class FakeLockManager {
      held = false

      async request(
        _name: string,
        _options: LockOptions,
        callback: (lock: Lock | null) => Promise<void>
      ) {
        if (this.held) return callback(null)
        this.held = true
        try {
          await callback({} as Lock)
        } finally {
          this.held = false
        }
      }
    }

    class FakeWebSocket extends EventTarget {
      static OPEN = 1
      binaryType = ''
      readyState = 0
      sent: number[][] = []
      onopen: (() => void) | null = null
      onmessage: ((event: MessageEvent<ArrayBuffer>) => void) | null = null
      onclose: (() => void) | null = null

      constructor(_url: string) {
        super()
        ;((window as any).sockets ??= []).push(this)
        queueMicrotask(() => {
          this.readyState = FakeWebSocket.OPEN
          this.onopen?.()
        })
      }

      send(data: Uint8Array) {
        this.sent.push([...data])
      }

      receive(bytes: number[]) {
        const data = new Uint8Array(bytes)
        this.onmessage?.(
          new MessageEvent('message', {
            data: data.buffer,
          })
        )
      }

      close() {
        if (this.readyState !== FakeWebSocket.OPEN) return
        this.readyState = 3
        this.onclose?.()
        this.dispatchEvent(new Event('close'))
      }
    }

    Object.defineProperty(navigator, 'locks', {
      configurable: true,
      value: new FakeLockManager(),
    })
    Object.defineProperty(window, 'WebSocket', {
      configurable: true,
      value: FakeWebSocket,
    })
  })

  await page.goto(baseUrl)
})

test('routes offer answers and cleanup in a browser', async ({ page }) => {
  await page.evaluate(async (moduleUrl) => {
    const { OriginSocket } = await import(moduleUrl)
    const leader = new OriginSocket('ws://origin.test')
    const follower = new OriginSocket('ws://origin.test')
    const answers: unknown[] = []
    follower.addEventListener('answer', (event: CustomEvent) =>
      answers.push(event.detail)
    )
    const withdraw = follower.offer({ roomId: 'room-1' })

    Object.assign(window as any, { answers, leader, follower, withdraw })
  }, `${baseUrl}/index.js`)

  await expect
    .poll(() =>
      page.evaluate(() => (window as any).sockets[0]?.sent.length ?? 0)
    )
    .toBe(1)

  const offerBytes = await page.evaluate(
    () => (window as any).sockets[0].sent[0] as number[]
  )
  const offer = decode(new Uint8Array(offerBytes)) as { id: string }
  const answer = encode({
    kind: 'answer',
    id: offer.id,
    payload: { candidate: 'candidate-1' },
  })

  await page.evaluate(
    (bytes) => (window as any).sockets[0].receive(bytes),
    [...answer]
  )
  await expect
    .poll(() => page.evaluate(() => (window as any).answers))
    .toEqual([{ candidate: 'candidate-1' }])

  await page.evaluate(() => (window as any).withdraw())
  await expect
    .poll(() =>
      page.evaluate(() => (window as any).sockets[0].sent.length)
    )
    .toBe(2)

  const withdrawBytes = await page.evaluate(
    () => (window as any).sockets[0].sent[1] as number[]
  )
  expect(decode(new Uint8Array(withdrawBytes))).toEqual({
    kind: 'withdraw',
    id: offer.id,
  })

  await page.evaluate(() => {
    ;(window as any).follower.close()
    ;(window as any).leader.close()
  })
})
