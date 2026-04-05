import loggerFactory from '@log4js-node/log4js-api';
import { IRedisClient, RedisConfig, getDefaultRedisConfigs, getNewRedisClient, parseClusterString } from './redis-client';
import { StreamConsumer, EventProcessor } from './stream-consumer';
import { augmentEvents } from './events';
import {
  AllowedFactories,
  ConsumerConfigs,
  ConsumerGroup,
  DeadLetterEvent,
  HandleFunction,
  IEvent,
  IWaitEvent,
  NamedEventHandler,
  ProduceFunc,
  RedisStreamsConfig,
  RedisStreamsInputConfig,
  StreamConfigs,
  StreamGroupConsumer,
} from './types';

export class RedisStreams {

  protected readonly groups: Map<string, Map<string, StreamGroupConsumer>> = new Map();
  protected readonly config: RedisStreamsConfig;

  constructor(protected readonly peerName: string, config?: Partial<RedisStreamsInputConfig>) {
    this.config = RedisStreams.buildConfig(peerName, config);
  }

  private _producerRedis: IRedisClient | undefined;
  protected getProducerRedis(): IRedisClient {
    if (!this._producerRedis) {
      this._producerRedis = getNewRedisClient(this.config.redis as RedisConfig, this.config.clusterOptions);
    }
    return this._producerRedis;
  }

  protected getConsumerRedis(): IRedisClient {
    return getNewRedisClient(this.config.redis as RedisConfig, this.config.clusterOptions);
  }

  private static buildConfig(
    peerName: string,
    config?: Partial<RedisStreamsInputConfig>,
  ): RedisStreamsConfig {
    let clusterNodes: { host: string; port: number }[] | undefined;
    if (config?.redis && 'cluster' in config.redis) {
      clusterNodes = parseClusterString((config.redis as { cluster: string }).cluster);
    }

    const redis = clusterNodes
      ? clusterNodes
      : ((config?.redis && !('cluster' in config.redis)) ? config.redis : getDefaultRedisConfigs());

    return {
      redis: redis as any,
      logger: config?.logger ?? loggerFactory.getLogger(peerName),
      ...(config?.deadLetters ? { deadLetters: config.deadLetters } : {}),
      ...(config?.clusterOptions ? { clusterOptions: config.clusterOptions } : {}),
    } as RedisStreamsConfig;
  }

  private buildConsumerConfigs(
    streamName: string,
    groupName: string,
    config?: Partial<StreamConfigs>,
  ): ConsumerConfigs {
    const base: ConsumerConfigs = {
      claimIdleTime: 15000,
      groupName,
      batchSize: 5,
      mode: 'parallel',
      readBlockTime: 5000,
      peerName: this.peerName,
      maxLen: 100000,
      streamName,
      deadLetters: this.config.deadLetters
        ? {
            maxRetries: this.config.deadLetters.maxRetries,
            handler: async (...events: DeadLetterEvent[]) => {
              /* istanbul ignore next */
              if (!this.config.deadLetters) return;
              const pipeline = this.getProducerRedis().pipeline();
              this.doProduce(pipeline, this.config.deadLetters.stream, this.config.deadLetters.maxSize, ...events);
              await pipeline.exec();
            },
          }
        : undefined,
    };
    return Object.assign(base, config ?? {});
  }

  protected registerConsumer(
    streamName: string,
    groupName: string,
    handlers: Map<string, NamedEventHandler>,
    config: ConsumerConfigs,
  ): StreamConsumer {
    const handler: EventProcessor = async <T>(id: string, event: string, eventObj: IEvent<T>) => {
      let handle = handlers.get(event);
      if (!handle) handle = handlers.get('*');
      if (!handle) return;
      return handle(id, eventObj);
    };
    return new StreamConsumer(this.getConsumerRedis(), handler, config);
  }

  private getStreamsMap(groupName: string): Map<string, StreamGroupConsumer> {
    let streams = this.groups.get(groupName);
    if (!streams) {
      streams = new Map();
      this.groups.set(groupName, streams);
    }
    return streams;
  }

  group(groupName: string): ConsumerGroup {
    const streams = this.getStreamsMap(groupName);
    return {
      stream: (streamName: string, config?: Partial<StreamConfigs>) => {
        const existing = streams.get(streamName);
        if (existing) return existing;
        const newStream = this.buildStream(streamName, groupName, config);
        streams.set(streamName, newStream);
        return newStream;
      },
    };
  }

  private buildStream(
    streamName: string,
    groupName: string,
    config?: Partial<StreamConfigs>,
  ): StreamGroupConsumer {
    const handlers = new Map<string, NamedEventHandler>();
    const readyConfigs = this.buildConsumerConfigs(streamName, groupName, config);
    const consumer = this.registerConsumer(streamName, groupName, handlers, readyConfigs);

    const consume = async () => {
      await consumer.init();
      consumer.start();
      return {
        stop: () => consumer.dispose(),
        continue: () => { consumer.start(); },
      };
    };

    const handle = ((event: string | '*', handler: NamedEventHandler<any>) => {
      handlers.set(event as string, handler);
      return { handle, consume };
    }) as HandleFunction<any>;

    // Per-stream pipeline — isolated, not shared across streams
    let _currentPipeline: IRedisClient | undefined;
    const getRedisPipeline = (): IRedisClient => {
      if (!_currentPipeline) _currentPipeline = this.getProducerRedis().multi();
      return _currentPipeline;
    };

    const produceMany: ProduceFunc = <T>(...events: IEvent<T>[]) => {
      const pipeline = this.doProduce(getRedisPipeline(), streamName, readyConfigs.maxLen, ...events);
      return {
        produceMany,
        flush: async () => {
          _currentPipeline = undefined;
          await pipeline.exec();
        },
      };
    };

    const produce = <T, R = any>(...events: (IEvent<T> | IWaitEvent<T>)[]) => {
      if (events.length === 1 && (events[0] as IWaitEvent<T>).wait) {
        const pipeline = this.doProduce(
          this.getProducerRedis().pipeline(),
          streamName,
          readyConfigs.maxLen,
          ...events,
        );
        return {
          wait: async (timeout: number): Promise<R> => {
            const redis = this.getConsumerRedis();
            const waitEvent = events[0] as IWaitEvent<T>;
            const channel = `${waitEvent.name}_${waitEvent.time}_${waitEvent.wait!.source}`;

            let timerId: NodeJS.Timeout | undefined;
            let msgHandler: ((...args: any[]) => void) | undefined;

            const cleanup = () => {
              if (msgHandler) redis.off('message', msgHandler);
              redis.unsubscribe(channel);
              redis.disconnect();
              /* istanbul ignore next */
              if (timerId) clearTimeout(timerId);
            };

            try {
              const reply = await new Promise<R>(async (resolve, reject) => {
                timerId = setTimeout(() => reject(new Error('Timeout')), timeout);
                await redis.subscribe(channel);
                msgHandler = (_channel: string, data: string) => resolve(JSON.parse(data)['reply']);
                redis.on('message', msgHandler);
                await pipeline.exec();
              });
              return reply;
            } finally {
              cleanup();
            }
          },
        };
      }

      const pipeline = this.doProduce(
        this.getProducerRedis().pipeline(),
        streamName,
        readyConfigs.maxLen,
        ...events,
      );
      return pipeline.exec().then(() => {});
    };

    const streamObj: StreamGroupConsumer = {
      consume,
      handle,
      produceMany,
      produce,
      with: (events) => augmentEvents(events, streamObj),
    };

    return streamObj;
  }

  private doProduce<T>(
    pipeline: IRedisClient,
    stream: string,
    maxLen?: number,
    ...events: IEvent<T>[]
  ): IRedisClient {
    if (events.length === 0) return pipeline;
    for (const one of events) {
      const eventString = JSON.stringify(one);
      if (typeof maxLen === 'number') {
        pipeline = pipeline.xadd(stream, 'MAXLEN', '~', String(maxLen), '*', one.name, eventString) as any;
      } else {
        pipeline = pipeline.xadd(stream, '*', one.name, eventString) as any;
      }
    }
    return pipeline;
  }
}
