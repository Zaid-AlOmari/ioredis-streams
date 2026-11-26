import loggerFactory from '@log4js-node/log4js-api';
import { IRedisClient } from './redis-client';
import { ConsumerBuffer } from './consumer-buffer';
import { RateLimiter } from './rate-limiter';
import { parseDuration } from './duration';
import { ConsumerConfigs, DeadLetterEvent, DeadLetterReason, IEvent, IWaitEvent } from './types';

export type EventProcessor = <T>(
  id: string,
  event: string,
  eventObj: IEvent<T> | IWaitEvent<T>
) => Promise<any>;

export class StreamConsumer {

  protected logger: loggerFactory.Logger;
  protected disposing = false;

  protected lastReadId = '0-0';
  protected checkBacklog = true;

  protected readonly buffer: ConsumerBuffer;
  protected readonly rateLimiter?: RateLimiter;
  protected readonly maxMessageAgeMs?: number;
  protected readingLoop: Promise<void> = Promise.resolve();

  constructor(
    protected readonly redis: IRedisClient,
    protected readonly processEvent: EventProcessor,
    protected readonly config: ConsumerConfigs,
  ) {
    this.logger = config.logger
      ?? loggerFactory.getLogger(config.streamName + ':Consumer');

    if (!this.config.batchSize) this.config.batchSize = 5;
    if (this.config.rateLimit) this.rateLimiter = new RateLimiter(this.config.rateLimit);
    if (this.config.maxMessageAge !== undefined) {
      if (!this.config.deadLetters) {
        throw new Error(
          `maxMessageAge on stream '${this.config.streamName}' requires deadLetters to be configured`,
        );
      }
      this.maxMessageAgeMs = parseDuration(this.config.maxMessageAge, 'maxMessageAge');
    }

    this.buffer = new ConsumerBuffer({
      ack: async (...ids: string[]) => {
        await this.redis.xack(this.config.streamName, this.config.groupName, ...ids);
      },
      error: async (id: string, message: string[], error: Error) => {
        // Counting on ack: a failed message isn't acked, so it gives its spot back.
        if (this.rateLimiter?.countOn === 'ack') this.rateLimiter.refund(1);
        this.logger.error(
          `Error during processMessage on stream '${this.config.streamName}' with id '${id}'`,
          error,
        );
      },
      process: (id: string, message: string[]) => this.processMessage(id, message),
      mode: this.config.mode ?? 'parallel',
      size: this.config.batchSize,
    });
  }

  get size(): number {
    return this.buffer.size;
  }

  async init(): Promise<void> {
    await this.redis.xgroup(
      'CREATE',
      this.config.streamName,
      this.config.groupName,
      '$',
      'MKSTREAM',
    ).catch((err: Error) => {
      if (!err.message.includes('BUSYGROUP')) {
        this.logger.error(err);
        throw err;
      }
    });
    this.logger.trace('Ready...');
  }

  async start(): Promise<void> {
    this.logger.trace('Started...');
    this.readingLoop = this.tryReading();
    await this.readingLoop;
  }

  protected async tryReading(): Promise<void> {
    while (!this.disposing) {
      try {
        // Without a rate limit, read straight away (no extra await) as before.
        const limit = this.rateLimiter
          ? await this.waitForReadCapacity(this.rateLimiter)
          : this.config.batchSize;
        if (this.disposing || limit < 1) continue;
        const claimedAnything = await this.doClaim(limit);
        if (claimedAnything && !this.disposing) continue;
        const dataAvailable = await this.doRead(limit);
        if (dataAvailable && !this.disposing) continue;
      } catch (err: any) {
        this.logger.error('Error while tryReading', err);
        if (err && typeof err.message === 'string' && err.message.includes('NOGROUP')) {
          this.checkBacklog = true;
          this.lastReadId = '0-0';
          await this.init();
        }
      }
    }
  }

  /**
   * Waits until the rate limit allows a full batch and returns how many messages
   * the next claim/read may fetch (at most the batch size). Waiting for a full
   * batch keeps Redis reads at one per batch, and fetching no more than allowed
   * keeps throttled messages in Redis instead of idling in memory.
   */
  protected waitForReadCapacity(limiter: RateLimiter): Promise<number> {
    return limiter.waitForTokens(this.config.batchSize);
  }

  /**
   * Hands entries to the buffer, spending one rate-limit token per entry.
   * Entries older than `maxMessageAge` go to dead letters instead and are not charged.
   */
  protected async consumeEntries(entries: [string, string[]][]): Promise<void> {
    let fresh = entries;
    if (this.maxMessageAgeMs !== undefined) {
      const now = Date.now();
      const late = entries.filter(([id]) => this.isTooOld(id, now));
      if (late.length) {
        fresh = entries.filter(entry => !late.includes(entry));
        this.logger.warn('Late messages sent to dead letters', JSON.stringify(late.map(([id]) => id)));
        await this.sendToDeadLetters('too-old', ...late);
        await this.redis.xack(this.config.streamName, this.config.groupName, ...late.map(([id]) => id));
      }
    }
    if (!fresh.length) return;
    this.rateLimiter?.take(fresh.length);
    await this.buffer.add(...fresh);
  }

  /** A stream id starts with the ms timestamp at which Redis added the entry. */
  protected isTooOld(id: string, now: number): boolean {
    const addedAt = Number(id.split('-')[0]);
    if (!isFinite(addedAt)) return false;
    return now - addedAt > this.maxMessageAgeMs!;
  }

  lastTimePendingCheck = 0;

  protected async doClaim(limit: number = this.config.batchSize): Promise<boolean> {
    if (this.lastTimePendingCheck + this.config.claimIdleTime >= Date.now()) return false;

    const allPending: [string, string, number, number][] = await this.redis.xpending(
      this.config.streamName,
      this.config.groupName,
      '-', '+', limit,
    );
    this.lastTimePendingCheck = Date.now();

    const result = allPending.filter(([, , idleTime]) => idleTime >= this.config.claimIdleTime);
    if (!result.length) return false;

    const toBeClaimedMessages = result.map(([id]) => id);
    const streamsEntries: [string, string[]][] = await this.redis.xclaim(
      this.config.streamName,
      this.config.groupName,
      this.config.peerName,
      this.config.claimIdleTime,
      ...toBeClaimedMessages,
    );

    const deadMessages = result.reduce((acc, [id, , , counter]) => {
      const threshold = this.config.deadLetters?.maxRetries ?? Number.MAX_VALUE;
      return counter >= threshold ? acc.add(id) : acc;
    }, new Set<string>());

    if (this.config.deadLetters && deadMessages.size) {
      const deadIds = Array.from(deadMessages);
      this.logger.trace('Dead Letters', JSON.stringify(deadIds));
      const deadEntries = streamsEntries.filter(
        entry => Array.isArray(entry) && entry.length && deadMessages.has(entry[0]),
      );
      await this.sendToDeadLetters(undefined, ...deadEntries);
      await this.redis.xack(this.config.streamName, this.config.groupName, ...deadIds);
    }

    const goodEntries = streamsEntries.filter(
      entry => Array.isArray(entry) && entry.length && !deadMessages.has(entry[0]),
    );
    if (goodEntries.length) {
      this.logger.info('Claimed', goodEntries.length);
      await this.consumeEntries(goodEntries);
    }

    return true;
  }

  protected async doRead(limit: number = this.config.batchSize): Promise<boolean> {
    const streamsEntries: [string, [string, string[]][]][] = await this.redis.xreadgroup(
      'GROUP', this.config.groupName,
      this.config.peerName,
      'COUNT', limit,
      'BLOCK', this.config.readBlockTime,
      'STREAMS', this.config.streamName,
      this.checkBacklog ? this.lastReadId : '>',
    );

    if (!streamsEntries || streamsEntries.length === 0) {
      this.checkBacklog = false;
      return false;
    }

    for (const [, streamEntries] of streamsEntries) {
      if (streamEntries.length === 0) {
        this.checkBacklog = false;
        return false;
      }
      this.lastReadId = streamEntries[streamEntries.length - 1][0];
      await this.consumeEntries(streamEntries);
    }

    return true;
  }

  protected async processMessage<T>(id: string, message: string[]): Promise<any> {
    let event: string;
    let eventObj: IEvent<T> | IWaitEvent<T>;

    try {
      let value: string;
      [event, value] = message;
      eventObj = JSON.parse(value);
    } catch (err) {
      return this.logger.error(
        'Error while parsing message. Corrupted or wrong-formatted stream message.',
        id, message, err,
      );
    }

    const result = await this.processEvent(id, event, eventObj);

    if ((eventObj as IWaitEvent<T>).wait) {
      const channel = `${eventObj.name}_${eventObj.time}_${(eventObj as IWaitEvent<T>).wait!.source}`;
      await this.redis.publish(channel, JSON.stringify({
        reply: result,
        wait: (eventObj as IWaitEvent<T>).wait,
      }));
    }

    return result;
  }

  async dispose(): Promise<void> {
    this.disposing = true;
    this.rateLimiter?.cancel();
    // Wait for the in-flight read/claim iteration (and everything it buffered) to finish
    // before deregistering, so we don't delete the consumer out from under active work.
    await this.readingLoop;
    await this.removeConsumer();
  }

  protected async removeConsumer(): Promise<void> {
    try {
      await this.redis.xgroup(
        'DELCONSUMER',
        this.config.streamName,
        this.config.groupName,
        this.config.peerName,
      );
      this.logger.trace('Consumer removed from group', this.config.peerName);
    } catch (err) {
      this.logger.error(
        `Error removing consumer '${this.config.peerName}' from group '${this.config.groupName}'`,
        err,
      );
    }
  }

  async publishDeadLetters(...messages: [string, string[]][]): Promise<void> {
    return this.sendToDeadLetters(undefined, ...messages);
  }

  protected async sendToDeadLetters(
    reason: DeadLetterReason | undefined,
    ...messages: [string, string[]][]
  ): Promise<void> {
    if (!this.config.deadLetters) return;

    const finalEvents: DeadLetterEvent[] = messages.map(([id, message]) => ({
      name: 'deadMessage',
      v: '1.0.0',
      time: Date.now(),
      data: {
        id,
        message,
        stream: this.config.streamName,
        group: this.config.groupName,
        // Retry dead letters keep their original shape; only new reasons add a field.
        ...(reason ? { reason } : {}),
      },
      by: { userId: this.config.peerName },
    } as any));

    await this.config.deadLetters.handler(...finalEvents);
  }
}
