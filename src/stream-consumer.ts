import loggerFactory from '@log4js-node/log4js-api';
import { IRedisClient } from './redis-client';
import { ConsumerBuffer } from './consumer-buffer';
import { ConsumerConfigs, DeadLetterEvent, IEvent, IWaitEvent } from './types';

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
  protected readingLoop: Promise<void> = Promise.resolve();

  constructor(
    protected readonly redis: IRedisClient,
    protected readonly processEvent: EventProcessor,
    protected readonly config: ConsumerConfigs,
  ) {
    this.logger = config.logger
      ?? loggerFactory.getLogger(config.streamName + ':Consumer');

    if (!this.config.batchSize) this.config.batchSize = 5;

    this.buffer = new ConsumerBuffer({
      ack: async (...ids: string[]) => {
        await this.redis.xack(this.config.streamName, this.config.groupName, ...ids);
      },
      error: async (id: string, message: string[], error: Error) => {
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
        const claimedAnything = await this.doClaim();
        if (claimedAnything && !this.disposing) continue;
        const dataAvailable = await this.doRead();
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

  lastTimePendingCheck = 0;

  protected async doClaim(): Promise<boolean> {
    if (this.lastTimePendingCheck + this.config.claimIdleTime >= Date.now()) return false;

    const allPending: [string, string, number, number][] = await this.redis.xpending(
      this.config.streamName,
      this.config.groupName,
      '-', '+', this.config.batchSize!,
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
      await this.publishDeadLetters(...deadEntries);
      await this.redis.xack(this.config.streamName, this.config.groupName, ...deadIds);
    }

    const goodEntries = streamsEntries.filter(
      entry => Array.isArray(entry) && entry.length && !deadMessages.has(entry[0]),
    );
    if (goodEntries.length) {
      this.logger.info('Claimed', goodEntries.length);
      await this.buffer.add(...goodEntries);
    }

    return true;
  }

  protected async doRead(): Promise<boolean> {
    const streamsEntries: [string, [string, string[]][]][] = await this.redis.xreadgroup(
      'GROUP', this.config.groupName,
      this.config.peerName,
      'COUNT', this.config.batchSize,
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
      await this.buffer.add(...streamEntries);
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
      },
      by: { userId: this.config.peerName },
    } as any));

    await this.config.deadLetters.handler(...finalEvents);
  }
}
