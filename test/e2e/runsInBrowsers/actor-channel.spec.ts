import { expect, test } from '@playwright/test'
import type * as Library from '../../../dist/index.js'

type Client = Library.ActorChannel<string, string, string, string>
declare global {
  interface Window {
    channel: Client
    messages: [string, string][]
    responses: [string, string?][]
    maintenance: Promise<void>
  }
}

test.beforeEach(async ({ page }) => {
  await page.goto('/')
})

test('publishes between channels and keeps local subscriptions after emptyBrokers', async ({
  page,
}) => {
  const messages = await page.evaluate(async () => {
    const moduleUrl = '/index.js'
    const { ActorChannel } = (await import(moduleUrl)) as typeof Library
    const publisher = new ActorChannel<string, string, string, string>()
    const subscriber = new ActorChannel<string, string, string, string>()
    subscriber.subscribe('topic')
    subscriber.emptyBrokers()
    const received = new Promise((resolve) =>
      subscriber.addEventListener('message', (event) => resolve(event.detail), {
        once: true,
      })
    )
    publisher.publish('topic', 'hello')
    return received
  })
  expect(messages).toEqual(['topic', 'hello'])
})

test('shares publications between same-origin tabs', async ({
  page,
  context,
}) => {
  const other = await context.newPage()
  await other.goto('/')
  await other.evaluate(async () => {
    const moduleUrl = '/index.js'
    const { ActorChannel } = (await import(moduleUrl)) as typeof Library
    window.channel = new ActorChannel()
    window.messages = []
    window.channel.subscribe('tabs')
    window.channel.addEventListener('message', (event) =>
      window.messages.push(event.detail)
    )
  })
  await page.evaluate(async () => {
    const moduleUrl = '/index.js'
    const { ActorChannel } = (await import(moduleUrl)) as typeof Library
    new ActorChannel<string, string, string, string>().publish(
      'tabs',
      'cross-tab'
    )
  })
  await expect
    .poll(() => other.evaluate(() => window.messages))
    .toEqual([['tabs', 'cross-tab']])
})

test('routes RPC and publications through a real broker, stops and reconnects', async ({
  page,
}, testInfo) => {
  const path = `/broker/${testInfo.project.name}/${testInfo.testId}/${testInfo.retry}`
  await page.evaluate(async (path) => {
    const moduleUrl = '/index.js'
    const { ActorChannel } = (await import(moduleUrl)) as typeof Library
    window.channel = new ActorChannel()
    window.messages = []
    window.responses = []
    window.channel.subscribe(path)
    window.channel.addEventListener('message', (event) =>
      window.messages.push(event.detail)
    )
    window.channel.addEventListener('response', (event) =>
      window.responses.push(event.detail)
    )
    window.maintenance = window.channel.addBroker(
      `ws://${location.host}${path}`,
      true
    )
  }, path)
  await expect
    .poll(() => page.evaluate(() => window.channel.rpcAvailable))
    .toBe(true)
  await expect
    .poll(async () => {
      const stats = await page.request.get('/stats')
      return ((await stats.json()) as { url: string; topics: string[] }[]).some(
        (socket) => socket.url === path && socket.topics.includes(path)
      )
    })
    .toBe(true)
  const id = await page.evaluate((path) => {
    window.channel.publish(path, 'via-broker')
    return window.channel.request('hello')
  }, path)
  await expect
    .poll(() => page.evaluate(() => window.responses))
    .toEqual([[id, 'reply:hello']])
  await expect
    .poll(() => page.evaluate(() => window.messages))
    .toEqual([[path, 'via-broker']])

  await page.evaluate(async () => {
    window.channel.emptyBrokers()
    await window.maintenance
  })
  expect(await page.evaluate(() => window.channel.rpcAvailable)).toBe(false)
  await expect
    .poll(async () => {
      const stats = await page.request.get('/stats')
      return ((await stats.json()) as { url: string }[]).filter(
        (socket) => socket.url === path
      ).length
    })
    .toBe(0)
  await page.evaluate((path) => {
    window.maintenance = window.channel.addBroker(
      `ws://${location.host}${path}`,
      true
    )
  }, path)
  await expect
    .poll(() => page.evaluate(() => window.channel.rpcAvailable))
    .toBe(true)
  await expect
    .poll(async () => {
      const stats = await page.request.get('/stats')
      return (
        (await stats.json()) as { url: string; topics: string[] }[]
      ).filter((socket) => socket.url === path && socket.topics.includes(path))
        .length
    })
    .toBe(1)
  await page.evaluate(async () => {
    window.channel.emptyBrokers()
    await window.maintenance
  })
})

test('elects one broker owner across tabs and releases the Web Lock when emptied', async ({
  page,
  context,
}, testInfo) => {
  const path = `/owner/${testInfo.project.name}/${testInfo.testId}/${testInfo.retry}`
  const other = await context.newPage()
  await other.goto('/')
  for (const tab of [page, other]) {
    await tab.evaluate(async (path) => {
      const moduleUrl = '/index.js'
      const { ActorChannel } = (await import(moduleUrl)) as typeof Library
      window.channel = new ActorChannel()
      window.maintenance = window.channel.addBroker(
        `ws://${location.host}${path}`,
        true
      )
    }, path)
  }
  await expect
    .poll(() => other.evaluate(() => window.channel.rpcAvailable))
    .toBe(true)
  const connections = async () => {
    const stats = await page.request.get('/stats')
    return ((await stats.json()) as { url: string }[]).filter(
      (socket) => socket.url === path
    ).length
  }
  expect(await connections()).toBe(1)
  await page.evaluate(async () => {
    window.channel.emptyBrokers()
    await window.maintenance
  })
  await other.evaluate(async () => {
    window.channel.emptyBrokers()
    await window.maintenance
  })
  await expect.poll(connections).toBe(0)
  await other.evaluate((path) => {
    window.maintenance = window.channel.addBroker(
      `ws://${location.host}${path}`,
      true
    )
  }, path)
  await expect.poll(connections).toBe(1)
  await other.evaluate(async () => {
    window.channel.emptyBrokers()
    await window.maintenance
  })
})
