import loggerFactory from '@log4js-node/log4js-api';
import Redis from 'ioredis';

const redisHost = process.env.REDIS_HOST;
const redisPort = parseInt(process.env.REDIS_PORT || '0');
const getDefaultRedisConfigs = () => {
  if (!redisHost || redisHost === '') {
    throw new Error('No redis connection provided (REDIS_HOST) ...')
  }
  if (!redisPort || redisPort === 0) {
    throw new Error('No redis connection provided (REDIS_PORT) ...')
  }
  return {
    host: process.env.REDIS_HOST || '',
    port: parseInt(process.env.REDIS_PORT || '0')
  }
}

let _redisClient: Redis.Redis;
export const getExistingRedisClient = (config = getDefaultRedisConfigs()) => {
  if (!_redisClient) {
    _redisClient = new Redis(config);
  }
  return _redisClient;
};

export const getNewRedisClient = (config = getDefaultRedisConfigs()) => {
  return new Redis(config);
};

export class RedisStreams {

  protected groups: Map<string, Map<string, StreamGroupConsumer>> = new Map();
  protected config: RedisStreamsConfig;

  constructor(protected peerName: string, config?: Partial<RedisStreamsConfig>) {
    this.config = RedisStreams.getDefaultConfigs(peerName, config);
  }

  private getConsumerRedis() {
    return getNewRedisClient(this.config.redis);
  }

  private getProducerRedis() {
    return getExistingRedisClient(this.config.redis);
  }

  private static getDefaultConfigs(peerName: string, config?: Partial<RedisStreamsConfig>) {
    return <RedisStreamsConfig>Object.assign(<RedisStreamsConfig>{
      redis: getDefaultRedisConfigs(),
      logger: loggerFactory.getLogger(peerName),
    }, config || {});
  }

  private buildConsumerConfigs(streamName: string, groupName: string, config?: Partial<RegisterConsumerConfigs>) {
    return <ConsumerConfigs>Object.assign(<ConsumerConfigs>{
      claimIdleTime: 15000,
      groupName,
      batchSize: 5,
      mode: 'parallel',
      readBlockTime: 5000,
      peerName: this.peerName!,
      maxLen: 100000,
      streamName,
      deadLetters: this.config.deadLetters ? {
        maxRetries: this.config.deadLetters.maxRetries,
        handler: async (...events) => {
          if (!this.config.deadLetters) return;
          const redis = this.getProducerRedis().pipeline();
          this.doProduce(redis, this.config.deadLetters.stream, this.config.deadLetters.maxSize, ...events);
          await redis.exec();
        }
      } : undefined
    }, config || {});
  }

  private register(streamName: string, groupName: string, handlers: Map<string, EventHandler<{}>>, config: ConsumerConfigs) {
    const handler = async <T>(id: string, event: string | '*', eventObj: IEvent<T>) => {
      let handle = handlers.get(event);
      if (!handle) handle = handlers.get('*');
      if (!handle) {
        this.config.logger.warn('No handler for:', groupName, streamName, event, id);
        return;
      };
      return handle(id, eventObj);
    };
    const newStream = new StreamConsumer(this.getConsumerRedis(), handler, config);
    return newStream;
  }

  private getStreamsMap(groupName: string) {
    let streams = this.groups.get(groupName);
    if (!streams) {
      streams = new Map<string, any>();
      this.groups.set(groupName, streams);
    }
    return streams;
  }

  group(groupName: string) {
    const streams = this.getStreamsMap(groupName);
    return <ConsumerGroup>{
      stream: (streamName: string, config?: Partial<RegisterConsumerConfigs>) => {
        const stream = streams.get(streamName);
        if (stream) return stream;
        const newStream = this.stream(streamName, groupName, config);
        streams.set(streamName, newStream);
        return newStream;
      }
    }
  }

  private stream(streamName: string, groupName: string, config?: Partial<RegisterConsumerConfigs>) {
    const handlers = new Map<string, EventHandler<{}>>();
    const readyConfigs = this.buildConsumerConfigs(streamName, groupName, config);
    const stream = this.register(streamName, groupName, handlers, readyConfigs);
    const consume = async () => {
      await stream.init();
      stream.start();
      return {
        stop: () => stream.dispose(),
        continue: () => { stream.start(); return; }
      }
    };
    const handle = <T>(event: string | '*', handler: EventHandler<T>) => {
      handlers.set(event, <EventHandler<{}>>handler);
      return { handle, consume };
    };
    const produceMany = <T>(...events: IEvent<T>[]) => {
      const redis = this.doProduce(this.getRedisPipeline(), streamName, readyConfigs.maxLen, ...events);
      return {
        produceMany,
        flush: async () => {
          this._currentPipeline = undefined;
          await redis.exec();
        }
      }
    }
    const produce = async <T>(...events: IEvent<T>[]) => {
      await this.doProduce(this.getProducerRedis().pipeline(), streamName, readyConfigs.maxLen, ...events).exec();
    }

    const newStream = <StreamGroupConsumer>{
      handle,
      produceMany,
      produce,
      with: (events) => augmentEvents(events, newStream)
    }
    return newStream;
  }

  private _currentPipeline: Redis.Pipeline | undefined;
  private getRedisPipeline() {
    if (!this._currentPipeline) this._currentPipeline = this.getProducerRedis().multi();
    return this._currentPipeline;
  }

  private doProduce<T>(redis: Redis.Pipeline, stream: string, maxLen?: number, ...events: IEvent<T>[]) {
    if (events.length === 0) return redis;
    for (const one of events) {
      const eventString = JSON.stringify(one);
      if (typeof maxLen === 'number') {
        redis = redis.xadd(stream, 'MAXLEN', '~', String(maxLen), '*', one.name, eventString);
      } else {
        redis = redis.xadd(stream, '*', one.name, eventString);
      }
    }
    return redis;
  }
}

class StreamConsumer {

  protected logger!: loggerFactory.Logger;
  private disposing = false;

  private lastReadId = '0-0';
  private checkBacklog = true;

  private buffer: ConsumerBuffer;

  constructor(
    protected redis: Redis.Redis,
    protected processEvent: EventProccessor,
    protected config: ConsumerConfigs) {

    if (this.config.logger) this.logger = this.config.logger;
    else this.logger = loggerFactory.getLogger(this.config.streamName + ':Consumer');
    if (!this.config.batchSize) this.config.batchSize = 5;
    this.buffer = new ConsumerBuffer({
      ack: async (...ids: string[]) => {
        await this.redis.xack(
          this.config.streamName,
          this.config.groupName, ...ids
        );
      },
      error: async (id: string, message: string[], error: Error) => {
        this.logger.error(`Error during processMessage on stream '${this.config.streamName}' with id '${id}'`, error);
      },
      process: async (id: string, message: string[]) => {
        return this.processMessage(id, message);
      },
      mode: this.config.mode || 'parallel',
      size: this.config.batchSize
    });
  }

  async init() {
    await this.redis.xgroup(
      'CREATE',
      this.config.streamName,
      this.config.groupName,
      '$',
      'MKSTREAM'
    ).catch(err => {
      if (!err.message.includes('BUSYGROUP')) {
        this.logger.error(err);
        throw err;
      }
    });
    this.logger.trace('Ready...');
  }

  async start() {
    this.logger.trace('Started...');
    await this.tryReading()
  }

  protected async tryReading() {
    while (!this.disposing) {
      try {
        const claimedAnything = await this.doClaim()
        if (claimedAnything && !this.disposing) continue;
        const dataAvaliable = await this.doRead();
        if (dataAvaliable && !this.disposing) continue;
      } catch (err) {
        this.logger.error('Error while tryReading', err);
      }
    }
  }

  lastTimePendingCheck = 0;
  private async doClaim() {
    if (this.lastTimePendingCheck + this.config.claimIdleTime >= Date.now()) return false;
    const result: ([string, string, number, number])[] = await this.redis.xpending(
      this.config.streamName,
      this.config.groupName,
      'IDLE', this.config.claimIdleTime,
      '-', '+', this.config.batchSize!
    );
    this.lastTimePendingCheck = Date.now();
    if (!result.length) return false;

    const toBeClaimedMessages = result.map(([id]) => id);
    const streamsEntries = await this.redis.xclaim(
      this.config.streamName,
      this.config.groupName,
      this.config.peerName,
      this.config.claimIdleTime,
      ...toBeClaimedMessages,
    );

    const deadMessages = result.reduce((p, [id, , , counter]) => {
      return (counter >= (this.config.deadLetters?.maxRetries || Number.MAX_VALUE)) ? p.add(id) : p;
    }, new Set<string>());

    if (this.config.deadLetters && deadMessages.size) {
      this.logger.info('Dead Letters', JSON.stringify(Array.from(deadMessages)));
      const deadStreamsEntries = streamsEntries.filter(([id]) => deadMessages.has(id));
      await this.publishDeadLetters(...deadStreamsEntries)
    }
    const goodStreamsEntries = streamsEntries.filter(([id]) => !deadMessages.has(id));
    if (goodStreamsEntries.length) {
      this.logger.info('Claimed', goodStreamsEntries.length);
      await this.buffer.add(...goodStreamsEntries);
    }
    return true;
  }

  protected async doRead() {
    const streamsEntries = await this.redis.xreadgroup(
      'GROUP', this.config.groupName,
      this.config.peerName,
      'BLOCK', this.config.readBlockTime,
      'COUNT', 5,
      'STREAMS', this.config.streamName,
      this.checkBacklog ? this.lastReadId : '>'
    );
    if (!streamsEntries || streamsEntries.length === 0) {
      this.checkBacklog = false;
      return false;
    }
    for (const [steamName, streamEntries] of streamsEntries) {
      if (streamEntries.length === 0) {
        this.checkBacklog = false;
        return false;
      }
      await this.buffer.add(...streamEntries);
    }
    return true;
  }

  protected async processMessage<T>(id: string, message: string[]) {
    const [event, value] = message;
    const eventObj: IEvent<T> = JSON.parse(value);
    return this.processEvent(id, event, eventObj)
  }

  async dispose() {
    this.disposing = true;
  }

  async publishDeadLetters(...messages: [string, string[]][]) {
    if (!this.config.deadLetters) return;
    const finalEvents = messages.map(([id, message]) => {
      return <DeadLetterEvent>{
        name: 'deadMessage',
        v: '1.0.0',
        time: Date.now(),
        data: {
          id,
          message,
          stream: this.config.streamName,
          group: this.config.groupName,
        },
        by: {
          userId: this.config.peerName,
        }
      }
    });
    await this.config.deadLetters.handler(...finalEvents);
  }
}

class ConsumerBuffer {

  buffer: [string, string[]][] = [];
  constructor(protected configs: {
    ack: (...id: string[]) => Promise<void>,
    process: (id: string, message: string[]) => Promise<void>,
    error: (id: string, message: string[], error: Error) => Promise<void>,
    mode: 'parallel' | 'serial',
    size: number
  }) {

  }

  get size() {
    return this.configs.size;
  }

  async add(...rawMessages: [string, string[]][]) {
    this.buffer.push(...rawMessages);
    return this.do();
  }

  isDoing = false;
  async do() {
    if (this.isDoing) return;
    this.isDoing = true;
    const toBeAck: string[] = [];
    const operations: Promise<void>[] = [];
    while (this.buffer.length) {
      const [id, message] = this.buffer.shift()!;
      const operation = this.configs.process(id, message)
        .then(_ => {
          toBeAck.push(id);
        })
        .catch(err => this.configs.error(id, message, err).catch());
      if (this.configs.mode === 'serial') {
        await operation;
        await this.configs.ack(id);
      } else {
        operations.push(operation);
      }
    }
    this.isDoing = false;
    if (this.configs.mode === 'parallel') {
      await Promise.all(operations);
      if (toBeAck.length) {
        await this.configs.ack(...toBeAck);
      }
    }
  }
}

const augmentEvents = <T extends FunctionsMap<R>, R>(events: T, stream: StreamGroupConsumer) => {
  const initial = <WithTypedHandlers<T, R> & StreamGroupConsumer>{ ...events, ...stream };
  return Object.keys(events).reduce((p, c) => {
    p[c] = (...args: any[]) => events[c](...args).then((event: any) => {
      return stream.produce(event)
    });
    return p;
  }, initial)
}

export interface IEvent<T, EventName extends string = string> {
  time: number;
  name: EventName;
  v: string;
  data: T;
}

export type EventProccessor = <T>(id: string, event: string, eventObj: IEvent<T>) => Promise<void>;

export type DeadLetterEvent = IEvent<{
  id: string,
  message: string[],
  stream: string,
  group: string,
}>;

export type RedisStreamsConfig = {
  redis: {
    host: string,
    port: number
  },
  logger: loggerFactory.Logger;
  deadLetters?: {
    stream: string;
    maxRetries: number;
    maxSize: number;
  };
}

export type RegisterConsumerConfigs = {
  readBlockTime: number;
  claimIdleTime: number;
  batchSize: number;
  mode: 'parallel' | 'serial';
  maxLen: number;
};

export type ConsumerConfigs = RegisterConsumerConfigs & {
  peerName: string;
  streamName: string;
  groupName: string;
  logger?: loggerFactory.Logger;
  deadLetters?: {
    handler: (...event: DeadLetterEvent[]) => Promise<void>
    maxRetries: number
  }
}
export type EventHandler<T> = (id: string, event: IEvent<T>) => Promise<void>;

export type HandleFunc = <T = any, E = string>(event: E | '*', handler: EventHandler<T>) => {
  handle: HandleFunc;
  consume: () => Promise<{
    stop: () => void;
    continue: () => void;
  }>;
};

export type ProduceFunc = <T>(...events: IEvent<T>[]) => {
  produceMany: ProduceFunc;
  flush: () => Promise<void>;
}

export type StreamGroupConsumer = {
  handle: HandleFunc;
  produce: <T>(...events: IEvent<T>[]) => Promise<void>;
  produceMany: ProduceFunc;
  with: <O extends FunctionsMap<R>, R>(events: O) => WithTypedHandlers<O, R>;
}

export type ConsumerGroup = {
  stream: (streamName: string, config?: Partial<RegisterConsumerConfigs>) => StreamGroupConsumer;
}

export type HandleFunction<O extends FunctionsMap<R>, R> = <N extends (keyof O & string) >(event: N, handler: NamedEventHandler<ReturnType<O[N], N>>) => {
  handle: HandleFunction<O, R>;
  consume: () => Promise<{
    stop: () => void;
    continue: () => void;
  }>;
};

type WithTypedHandlers<T extends FunctionsMap<R>, R> = T & {
  handle: HandleFunction<T, R>
}
type UnrwapPromiseEvent<T, N extends string> = T extends Promise<IEvent<infer D>> ? IEvent<D, N> : unknown;
type ReturnType<T, N extends string> = T extends (...args: any[]) => infer R ? UnrwapPromiseEvent<R, N> : unknown;

type FunctionsMap<T> = {
  [func in keyof T]: T[func];
};

type NamedEventHandler<E> = (id: string, event: E) => Promise<void>;
