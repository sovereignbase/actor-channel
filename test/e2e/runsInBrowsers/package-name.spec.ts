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
      <title>ActorChannel test</title>`)
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

test('fans publications between ActorChannel instances in a window', async ({
  page,
}) => {
  await page.goto(baseUrl)
  await page.evaluate(async (moduleUrl) => {
    const { ActorChannel, ChannelBroker } = await import(moduleUrl)
    const publisher = new ActorChannel()
    const subscriber = new ActorChannel()
    const messages: unknown[] = []
    subscriber.subscribe('topic')
    subscriber.addEventListener('message', (event: CustomEvent) =>
      messages.push(event.detail)
    )
    publisher.publish('topic', 'hello')
    Object.assign(window, { ActorChannel, ChannelBroker, messages })
  }, `${baseUrl}/index.js`)

  await expect
    .poll(() => page.evaluate(() => (window as any).messages))
    .toEqual([['topic', 'hello']])
  expect(
    await page.evaluate(
      () =>
        typeof (window as any).ActorChannel === 'function' &&
        typeof (window as any).ChannelBroker === 'function'
    )
  ).toBe(true)
})
