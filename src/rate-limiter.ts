import { Duration, parseDuration } from './duration';

export type RateLimitConfig = {
  /** Maximum number of messages that may be consumed per `duration`. */
  maxMessages: number;
  /** Length of the window: ms, or a string like `'10s'`, `'1m'`, `'2h'`. */
  duration: Duration;
  /**
   * When a message counts against the limit. Default `'read'`.
   * - `'read'`: counted when taken from Redis; a failed message stays counted,
   *   and each retry counts again.
   * - `'ack'`: a spot is held when the message is taken from Redis (so the
   *   consumer can't over-read) and given back if the handler fails, so only
   *   successfully handled (acked) messages use up the limit.
   */
  countOn?: 'read' | 'ack';
};

/**
 * Token bucket used to throttle how fast a consumer pulls messages off a stream.
 *
 * The bucket holds up to `maxMessages` tokens and refills continuously at
 * `maxMessages / duration` tokens per millisecond, so short bursts up to
 * `maxMessages` are allowed while the long-run rate never exceeds the limit.
 */
export class RateLimiter {

  private tokens: number;
  private lastRefill: number;
  private readonly refillPerMs: number;
  private pendingWait?: { timer: NodeJS.Timeout; resolve: () => void };
  readonly countOn: 'read' | 'ack';

  constructor(
    private readonly config: RateLimitConfig,
    private readonly now: () => number = () => Date.now(),
  ) {
    if (!(config.maxMessages > 0) || !isFinite(config.maxMessages)) {
      throw new Error(`Invalid rateLimit.maxMessages '${config.maxMessages}': must be a positive number`);
    }
    const durationMs = parseDuration(config.duration, 'rateLimit.duration');
    if (config.countOn !== undefined && config.countOn !== 'read' && config.countOn !== 'ack') {
      throw new Error(`Invalid rateLimit.countOn '${config.countOn}': must be 'read' or 'ack'`);
    }
    this.countOn = config.countOn ?? 'read';
    this.tokens = config.maxMessages;
    this.lastRefill = this.now();
    this.refillPerMs = config.maxMessages / durationMs;
  }

  /** Whole tokens available right now. */
  get available(): number {
    this.refill();
    return Math.floor(this.tokens);
  }

  /**
   * Waits until a full batch is allowed, then returns how many messages may be
   * fetched (at most `batchSize`). A full batch is `batchSize` messages, or
   * `maxMessages` when the limit is smaller than the batch (a bigger batch could
   * never be allowed at once). Returns 0 if the wait was cancelled.
   */
  async waitForTokens(batchSize: number): Promise<number> {
    const needed = Math.max(1, Math.min(batchSize, Math.floor(this.config.maxMessages)));
    let available = this.available;
    while (available < needed) {
      const msUntilReady = Math.ceil((needed - this.tokens) / this.refillPerMs);
      const cancelled = await this.sleep(msUntilReady);
      if (cancelled) return 0;
      available = this.available;
    }
    return Math.min(available, batchSize);
  }

  /** Spends `count` tokens for messages that were actually consumed. */
  take(count: number): void {
    if (count <= 0) return;
    this.refill();
    this.tokens -= count;
  }

  /** Gives back `count` tokens, e.g. for failed messages when counting on ack. */
  refund(count: number): void {
    if (count <= 0) return;
    this.refill();
    this.tokens = Math.min(this.config.maxMessages, this.tokens + count);
  }

  /** Wakes up a pending `waitForTokens` call so shutdown isn't delayed. */
  cancel(): void {
    if (!this.pendingWait) return;
    clearTimeout(this.pendingWait.timer);
    const { resolve } = this.pendingWait;
    this.pendingWait = undefined;
    resolve();
  }

  private refill(): void {
    const now = this.now();
    const elapsed = now - this.lastRefill;
    if (elapsed <= 0) return;
    this.tokens = Math.min(this.config.maxMessages, this.tokens + elapsed * this.refillPerMs);
    this.lastRefill = now;
  }

  /** Resolves `true` if cancelled before `ms` elapsed. */
  private sleep(ms: number): Promise<boolean> {
    return new Promise<boolean>(resolve => {
      const timer = setTimeout(() => {
        this.pendingWait = undefined;
        resolve(false);
      }, Math.max(ms, 1));
      this.pendingWait = { timer, resolve: () => resolve(true) };
    });
  }
}
