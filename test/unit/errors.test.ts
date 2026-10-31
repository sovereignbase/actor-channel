import { expect, test } from 'vitest'
import { ActorChannelError } from '../../src/index.js'
import { unknownError } from '../../src/errors/unknown.js'

test('errors expose stable codes, package-prefixed messages and their cause', () => {
  const error = new ActorChannelError('UNKNOWN_ERROR')
  expect(error).toBeInstanceOf(Error)
  expect(error.name).toBe('ActorChannelError')
  expect(error.code).toBe('UNKNOWN_ERROR')
  expect(error.message).toBe('{@sovereignbase/actor-channel} UNKNOWN_ERROR')
  expect(error.cause).toBeUndefined()
  expect(new ActorChannelError('UNKNOWN_ERROR', '').message).toBe(
    '{@sovereignbase/actor-channel} '
  )
})

test.each([new Error('Transport failed'), 'failure', undefined, null])(
  'unexpected causes are preserved: %j',
  (cause) => {
    const error = unknownError(cause, 'operation')
    expect(error).toBeInstanceOf(ActorChannelError)
    expect(error.code).toBe('UNKNOWN_ERROR')
    expect(error.message).toBe(
      '{@sovereignbase/actor-channel} operation failed.'
    )
    expect(error.cause).toBe(cause)
    expect(unknownError(error, 'outer operation')).toBe(error)
  }
)
