import loggerFactory from '@log4js-node/log4js-api';
import Redis, { Pipeline } from 'ioredis';

const redisHost = process.env.REDIS_HOST;
const redisPort = parseInt(process.env.REDIS_PORT || '0');
const redisCluster = process.env.REDIS_CLUSTER || undefined;

const parseClusterString = (redisCluster: string) => redisCluster
  .split(',')
  .map(x => x.split(':'))
  .map(([host, port]) => ({ host, port: parseInt(port) }));

const getDefaultRedisConfigs = () => {
  if (redisCluster) {
    return parseClusterString(redisCluster);
  }
  if (!redisHost || redisHost === '') {
    throw new Error('No redis connection provided (REDIS_HOST) ...')
  }
  if (!redisPort) {
    throw new Error('No redis connection provided (REDIS_PORT) ...')
  }
  return {
    host: process.env.REDIS_HOST || '',
    port: parseInt(process.env.REDIS_PORT || '0')
  }
}

let _redisClient: Redis.Redis | Redis.Cluster;
export const getExistingRedisClient = (config = getDefaultRedisConfigs()) => {
  if (!_redisClient) {
    if (config instanceof Array) {
      _redisClient = new Redis.Cluster(config);
    } else {
      _redisClient = new Redis(config);
    }
  }
  return _redisClient;
};

export const getNewRedisClient = (config = getDefaultRedisConfigs()) => {
  if (config instanceof Array) {
    return new Redis.Cluster(config);
  } else {
    return new Redis(config);
  }
};

export interface IEvent<T> {
  time: number;
  name: string;
  v: string;
  data: T;
}

export interface IWaitEvent<T> extends IEvent<T> {
  wait: { source: string };
}

export class RedisStreams {

  protected groups: Map<string, Map<string, StreamGroupConsumer>> = new Map();
  protected config: RedisStreamsConfig;

  constructor(protected peerName: string, config?: Partial<RedisStreamsInputConfig>) {
    this.config = RedisStreams.getDefaultConfigs(peerName, config);
  }

  private getConsumerRedis() {
    return getNewRedisClient(this.config.redis);
  }

  private getProducerRedis() {
    return getExistingRedisClient(this.config.redis);
  }

  private static getDefaultConfigs(peerName: string, config?: Partial<RedisStreamsInputConfig>) {
    let cluster: string | undefined;
    if (config?.redis && 'cluster' in config.redis) {
      cluster = (config!.redis! as { cluster: string }).cluster;
    }

    return <RedisStreamsConfig>Object.assign(<RedisStreamsConfig>{
      redis: getDefaultRedisConfigs(),
      logger: loggerFactory.getLogger(peerName),
    }, config || {},
      { ...(cluster ? { redis: parseClusterString(cluster) } : {}) });
  }

  private buildConsumerConfigs(streamName: string, groupName: string, config?: Partial<StreamConfigs>) {
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

  private register(streamName: string, groupName: string, handlers: Map<string, NamedEventHandler>, config: ConsumerConfigs) {
    const handler = async <T>(id: string, event: string | '*', eventObj: IEvent<T>) => {
      let handle = handlers.get(event);
      if (!handle) handle = handlers.get('*');
      if (!handle) {
        return;
      }
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
      stream: (streamName: string, config?: Partial<StreamConfigs>) => {
        const stream = streams.get(streamName);
        if (stream) return stream;
        const newStream = this.stream(streamName, groupName, config);
        streams.set(streamName, newStream);
        return newStream;
      }
    }
  }

  private stream(streamName: string, groupName: string, config?: Partial<StreamConfigs>) {
    const handlers = new Map<string, NamedEventHandler>();
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
    const handle = <T extends IEvent<D>, D>(event: string | '*', handler: NamedEventHandler<T>) => {
      handlers.set(event, <NamedEventHandler>handler);
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
    const produce = <T, R = any>(...events: (IEvent<T> | IWaitEvent<T>)[]) => {
      if (events.length === 1 && events[0]['wait']) {
        const pipeline = this.doProduce(this.getProducerRedis().pipeline(), streamName, readyConfigs.maxLen, ...events);
        return {
          wait: async (timeout: number) => {
            const redis = this.getConsumerRedis();
            const channel = events[0].name + '_' + events[0].time + '_' + events[0]['wait']?.['source'];
            let id: NodeJS.Timeout | undefined;
            let handler: ((...args: any[]) => void) | undefined;
            const cleanup = () => {
              if (handler) redis.off('message', handler);
              redis.unsubscribe(channel);
              redis.disconnect();
              if (id) clearTimeout(id);
            }
            try {
              const reply = await new Promise<R>(async (resolve, reject) => {
                id = setTimeout(() => reject(new Error('Timeout')), timeout);
                await redis.subscribe(channel);
                handler = (commingChannel, data) => resolve(JSON.parse(data)['reply']);
                redis.on('message', handler);
                await pipeline.exec();
              });
              return reply;
            } finally {
              cleanup();
            }
          }
        }
      } else {
        const r1 = this.doProduce(this.getProducerRedis().pipeline(), streamName, readyConfigs.maxLen, ...events);
        return r1.exec().then(r => { return; });
      }
    }

    const newStream = <StreamGroupConsumer>{
      consume,
      handle,
      produceMany,
      produce,
      with: (events) => augmentEvents(events, newStream)
    }
    return newStream;
  }

  private _currentPipeline: Pipeline | undefined;
  private getRedisPipeline() {
    if (!this._currentPipeline) this._currentPipeline = this.getProducerRedis().multi();
    return this._currentPipeline;
  }

  private doProduce<T>(redis: Pipeline, stream: string, maxLen?: number, ...events: IEvent<T>[]) {
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

type EventProccessor = <T>(id: string, event: string, eventObj: IEvent<T> | IWaitEvent<T>) => Promise<any>;
class StreamConsumer {

  protected logger!: loggerFactory.Logger;
  private disposing = false;

  private lastReadId = '0-0';
  private checkBacklog = true;

  private buffer: ConsumerBuffer;

  constructor(
    protected redis: Redis.Redis | Redis.Cluster,
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
      } catch (err: any) {
        this.logger.error('Error while tryReading', err);
        if (err && typeof err.message === 'string' && err.message.includes('NOGROUP')) {
          await this.init()
        }
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
      const deadIds = Array.from(deadMessages);
      this.logger.trace('Dead Letters', JSON.stringify(deadIds));
      const deadStreamsEntries = streamsEntries.filter(entry => (entry instanceof Array) && entry.length && deadMessages.has(entry[0]));
      await this.publishDeadLetters(...deadStreamsEntries);
      await this.redis.xack(this.config.streamName,
        this.config.groupName, ...deadIds);
    }
    const goodStreamsEntries = streamsEntries.filter((entry => (entry instanceof Array) && entry.length && !deadMessages.has(entry[0])));
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
      'COUNT', 5,
      'BLOCK', this.config.readBlockTime,
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
      this.lastReadId = streamEntries[streamEntries.length - 1][0]
      await this.buffer.add(...streamEntries);
    }
    return true;
  }

  protected async processMessage<T>(id: string, message: string[]) {
    let event: string;
    let eventObj: IEvent<T> | IWaitEvent<T>
    try {
      let value: string;
      [event, value] = message;
      eventObj = JSON.parse(value);
    }
    catch (err) {
      //log corrupted messages
      return this.logger.error('Error while parsing message. Corrupted or wrong-formatted stream message.', id, message, err);
    }
    const result = await this.processEvent(id, event, eventObj);
    if (eventObj['wait']) {
      const channel = eventObj.name + '_' + eventObj.time + '_' + eventObj['wait']?.['source'];
      await this.redis.publish(channel, JSON.stringify({
        reply: result,
        wait: eventObj['wait']
      }));
    }
    return result;
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
    process: (id: string, message: string[]) => Promise<any>,
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

const augmentEvents = <T extends AllowedFactories<R>, R>(events: T, stream: StreamGroupConsumer) => {
  return Object.keys(events).reduce((p, c) => {
    const factory = events[c as keyof T];
    p[c] = (...args: any[]) => stream.produce(factory(...args))
    return p;
  }, { ...events, ...stream })
}

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

export type RedisStreamsInputConfig = Omit<RedisStreamsConfig, 'redis'> & {
  redis: {
    host: string;
    port: number;
  }
  | {
    cluster: string;
  };
};

export type StreamConfigs = {
  readBlockTime: number;
  claimIdleTime: number;
  batchSize: number;
  mode: 'parallel' | 'serial';
  maxLen: number;
};

type ConsumerConfigs = StreamConfigs & {
  peerName: string;
  streamName: string;
  groupName: string;
  logger?: loggerFactory.Logger;
  deadLetters?: {
    handler: (...event: DeadLetterEvent[]) => Promise<void>
    maxRetries: number
  }
}

export type ProduceFunc = (...events: IEvent<any>[]) => {
  produceMany: ProduceFunc;
  flush: () => Promise<void>;
}

export type StreamGroupConsumer = ConsumeFunctions & {
  handle: HandleFunction<any>;
  produce: (...events: (IEvent<any> | IWaitEvent<any>)[]) => Promise<void> | {
    wait: (timeout: number) => Promise<any>;
  }
  produceMany: ProduceFunc;
  with: <O extends AllowedFactories<O>>(events: O) => WithTypedHandlers<O>;
}

export type ConsumerGroup = {
  stream: (streamName: string, config?: Partial<StreamConfigs>) => StreamGroupConsumer;
}

type NamedEvent<T, N extends string> = IEvent<T> & { name: N };
type NamedWaitEvent<T, N extends string, ReplyType = any> = IWaitEvent<T> & { name: N };
type ReturnType<T> = T extends (...args: any[]) => infer R ? R : any;
type ReplyTypeOf<T> = T extends NamedWaitEvent<any, string, infer Reply> ? Reply : any
type DataOfHandler<T> = T extends (...args: any[]) => IEvent<infer R> ? R : any;
type ArgsOf<T> = T extends (...args: infer Args) => any ? Args : never;

type AllowedFactories<T> = { [name in (keyof T)]: (...args: any[]) => IEvent<DataOfHandler<T[name]>> | IWaitEvent<DataOfHandler<T[name]>> };
export type NamedEventHandler<E = IEvent<any>> = (id: string, event: E) => Promise<any>;

export type ConsumeFunctions = {
  consume: () => Promise<{
    stop: () => void;
    continue: () => void;
  }>;
}
export type HandleFunction<T> = <N extends (keyof T | '*') >(event: N, handler: NamedEventHandler<N extends keyof T ? ReturnType<T[N]> : IEvent<any>>) => {
  handle: HandleFunction<T>;
} & ConsumeFunctions;


type WithTypedHandlers<T> = PromisifiedFunctionsMap<T> & {
  handle: HandleFunction<T>
} & Omit<StreamGroupConsumer, 'handle'>;


type PromisifiedFunctionsMap<T> = {
  [func in keyof T]: (...args: ArgsOf<T[func]>) => ReturnType<T[func]> extends { wait: { source: string } } ? { wait: (timeout: number) => Promise<ReplyTypeOf<ReturnType<T[func]>>> } : Promise<void>
};

export const event = <N extends string>(name: N, v = '1.0.0') => {
  type ReturnType<T, N extends string> = {
    [K in N]: (data: T, time?: number) => NamedEvent<T, N>;
  };
  return {
    of: <T>() => {
      return {
        [name]: (data: T, time = Date.now()) => ({ name, v, data, time })
      } as ReturnType<T, N>
    }
  }
}

export const eventWithReply = <N extends string>(name: N, source: string, v = '1.0.0') => {
  type ReturnType<T, N extends string, ReplyType> = {
    [K in N]: (data: T, time?: number) => NamedWaitEvent<T, N, ReplyType>;
  };
  return {
    of: <T, ReplyType = any>() => {
      return {
        [name]: (data: T, time = Date.now()) => (<IWaitEvent<T>>{
          name, v, data, time, wait: { source }
        })
      } as ReturnType<T, N, ReplyType>
    }
  }
}