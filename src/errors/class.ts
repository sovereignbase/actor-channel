/** Identifies an ActorChannel failure independently of its human-readable message. */
export type ActorChannelErrorCode =
  /** The same transport was registered with a broker more than once. */
  | 'CHANNEL_ALREADY_ADDED'
  /** An unexpected failure was wrapped with its original cause. */
  | 'UNKNOWN_ERROR'

/**
 * An ActorChannel error with a stable, machine-readable code and an optional cause.
 *
 * The message is prefixed with the library name. Use {@link code} rather than
 * parsing the message when handling a particular failure.
 */
export class ActorChannelError extends Error {
  /** The machine-readable category of the failure. */
  readonly code: ActorChannelErrorCode

  /**
   * Creates an error whose name is `ActorChannelError`.
   *
   * @param code The category of the failure.
   * @param message The detail to prefix with the library name; defaults to `code`.
   * @param options Error options, including the original `cause`.
   */
  constructor(
    code: ActorChannelErrorCode,
    message?: string,
    options?: ErrorOptions
  ) {
    super(`{@sovereignbase/actor-channel} ${message ?? code}`, options)
    this.code = code
    this.name = 'ActorChannelError'
  }
}
